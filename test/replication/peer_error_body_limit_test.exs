defmodule ElixirDB.Replication.PeerErrorBodyLimitTest do
  @moduledoc """
  Regression tests for bounding peer error-body buffering in RemoteTransport.

  A configured peer must not be able to stream an arbitrarily large non-2xx
  response body into memory. The error-object drain caps at the same encoded
  size ceiling used for success envelopes and, once the limit is crossed,
  stops consuming the remote stream and returns `payload_too_large` instead of
  buffering bytes without bound. The one-shot path bounds both success and
  non-2xx bodies before wire decoding or HTTP-status classification.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.Config
  alias ElixirDB.Error
  alias ElixirDB.Replication.RemoteTransport
  alias ElixirDB.TestServer

  defp with_small_limits(fun) do
    previous = Application.get_env(:elixir_db, :host_limits, [])

    Application.put_env(
      :elixir_db,
      :host_limits,
      Map.merge(Map.new(previous), %{max_replication_batch_bytes: 1_024})
    )

    on_exit(fn -> Application.put_env(:elixir_db, :host_limits, previous) end)
    fun.()
  end

  test "enumerable error body under the limit keeps status classification" do
    server =
      TestServer.start_supervised!(
        request_hook: fn conn ->
          {:halt, Plug.Conn.send_resp(conn, 500, "plain text, not wire zstd")}
        end
      )

    assert {:error, %Error{code: :internal_error, retryable: true}} =
             RemoteTransport.open_stream(
               server.base_url,
               "/v1/identity",
               "digest"
             )
  end

  test "oversized enumerable error body returns payload_too_large and stops early" do
    with_small_limits(fn ->
      {base_url, counter, server_pid} = streaming_server!(64 * 1024 * 1024)

      assert {:error, %Error{code: :payload_too_large}} =
               RemoteTransport.open_stream(
                 base_url,
                 "/v1/identity",
                 "digest"
               )

      await_server(server_pid)

      %{total: total, written: written} = Agent.get(counter, & &1)

      assert written > 0,
             "the server must have delivered some body before the drain capped it"

      assert written < total,
             "the tail of an oversized error body must not be consumed (wrote #{written} of #{total} bytes)"
    end)
  end

  defp await_server(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      5_000 -> flunk("streaming server did not terminate")
    end
  end

  test "oversized one-shot success body returns payload_too_large" do
    body = String.duplicate("x", 65_000)

    with_small_limits(fn ->
      server =
        TestServer.start_supervised!(
          request_hook: fn conn ->
            {:halt, Plug.Conn.send_resp(conn, 200, body)}
          end
        )

      assert {:error, %Error{code: :payload_too_large}} =
               RemoteTransport.request(
                 server.base_url,
                 :get,
                 "/v1/identity",
                 nil,
                 nil,
                 5_000
               )
    end)
  end

  test "oversized one-shot non-2xx body returns payload_too_large before classification" do
    body = String.duplicate("y", 65_000)

    with_small_limits(fn ->
      server =
        TestServer.start_supervised!(
          request_hook: fn conn ->
            {:halt, Plug.Conn.send_resp(conn, 502, body)}
          end
        )

      assert {:error, %Error{code: :payload_too_large}} =
               RemoteTransport.request(
                 server.base_url,
                 :get,
                 "/v1/identity",
                 nil,
                 nil,
                 5_000
               )
    end)
  end

  test "small one-shot non-2xx body is still status-classified under the default limit" do
    server =
      TestServer.start_supervised!(
        request_hook: fn conn ->
          {:halt, Plug.Conn.send_resp(conn, 503, "service unavailable page")}
        end
      )

    assert {:error, %Error{code: :internal_error, retryable: true}} =
             RemoteTransport.request(
               server.base_url,
               :get,
               "/v1/identity",
               nil,
               nil,
               Config.request_timeout_ms()
             )
  end

  defp streaming_server!(total) do
    {:ok, counter} = Agent.start_link(fn -> %{total: total, written: 0} end)
    {:ok, lsock} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(lsock)

    server_pid =
      spawn_link(fn ->
        accept_and_stream(lsock, counter, total)
        :gen_tcp.close(lsock)
      end)

    on_exit(fn ->
      if Process.alive?(counter), do: Agent.stop(counter)
    end)

    base_url = "http://127.0.0.1:#{port}"
    {base_url, counter, server_pid}
  end

  defp accept_and_stream(lsock, counter, total) do
    case :gen_tcp.accept(lsock) do
      {:ok, sock} ->
        {:ok, _request} = :gen_tcp.recv(sock, 0, 5_000)

        :ok =
          :gen_tcp.send(
            sock,
            "HTTP/1.1 500 Error\r\ncontent-length: #{total}\r\nconnection: keep-alive\r\n\r\n"
          )

        stream_body(sock, counter, total, 0)

      {:error, _reason} ->
        Agent.update(counter, fn m -> %{m | written: -1} end)
        :ok
    end
  end

  # Write the body in chunks, recording how many bytes were actually accepted
  # by the peer socket. When the client cancels the stream early (the drain
  # capped at the encoded size limit), the socket write fails and fewer bytes
  # are counted than the full advertised body.
  defp stream_body(_sock, counter, total, written) when written >= total do
    Agent.update(counter, fn m -> %{m | written: written} end)
  end

  defp stream_body(sock, counter, total, written) do
    chunk_size = 4_096
    remaining = total - written
    n = min(chunk_size, remaining)

    case :gen_tcp.send(sock, :binary.copy("z", n)) do
      :ok ->
        stream_body(sock, counter, total, written + n)

      {:error, _reason} ->
        Agent.update(counter, fn m -> %{m | written: written} end)
    end
  end
end
