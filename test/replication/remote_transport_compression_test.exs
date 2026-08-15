defmodule VialKeeper.Replication.RemoteTransportCompressionTest do
  @moduledoc """
  RemoteTransport sends and requires Zstandard JSON and representation blob headers.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Attachments
  alias VialKeeper.Attachments.FilesystemStore
  alias VialKeeper.Replication.RemoteTransport
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.TestServer

  setup do
    path = "wire-transport-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), path)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    {:ok, captured} = Agent.start_link(fn -> [] end)

    server =
      TestServer.start_supervised!(
        request_hook: fn conn ->
          Agent.update(captured, fn acc ->
            [{conn.method, conn.request_path, conn.req_headers} | acc]
          end)

          conn
        end
      )

    on_exit(fn ->
      if Process.alive?(captured), do: Agent.stop(captured)
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, base_url: server.base_url, captured: captured}
  end

  test "JSON identity request is zstd-framed and has no uncompressed fallback", %{
    uuid: uuid,
    base_url: base_url,
    captured: captured
  } do
    _ =
      RemoteTransport.request(
        base_url,
        :get,
        "/v1/databases/#{uuid}/replication/identity",
        nil,
        nil
      )

    headers = last_headers(captured, "GET")
    assert header(headers, "accept-encoding") == "zstd"
    assert header(headers, "content-encoding") == nil
    assert header(headers, "content-length") in [nil, "0"]
  end

  test "JSON POST bodies are zstd-compressed with canonical uncompressed-length", %{
    uuid: uuid,
    base_url: base_url,
    captured: captured
  } do
    _ =
      RemoteTransport.request(
        base_url,
        :post,
        "/v1/databases/#{uuid}/replication/blobs/diff",
        %{"digests" => []},
        nil
      )

    headers = last_headers(captured, "POST")
    assert header(headers, "content-encoding") == "zstd"
    assert header(headers, "content-type") |> String.starts_with?("application/json")

    length = header(headers, "x-vialkeeper-uncompressed-length")
    assert match?({int, ""} when int > 0, Integer.parse(length || ""))
  end

  test "successful blob GET uses representation media type without Content-Encoding", %{
    uuid: uuid,
    base_url: base_url
  } do
    payload = String.duplicate("representation-contract-", 4_096)
    assert {:ok, %{blob: digest, length: logical}} = Attachments.upload_stream(uuid, [payload])
    {:ok, bundle_root} = DatabaseCatalog.bundle_root(uuid)
    assert {:ok, stat} = FilesystemStore.stat(bundle_root, digest)
    assert stat.encoding == :zstd
    assert encoded_payload_length(bundle_root, digest) < logical

    assert {:ok, response} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/blobs/#{digest}",
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    assert response.status == 200
    type = header_from_response(response, "content-type") || ""
    assert String.starts_with?(type, "application/vnd.vialkeeper.blob-representation")
    encoding = header_from_response(response, "content-encoding")
    assert encoding in [nil, "", "identity"]
    assert header_from_response(response, "x-vialkeeper-blob-encoding") == "zstd"
  end

  defp last_headers(captured, method) do
    captured
    |> Agent.get(& &1)
    |> Enum.find_value([], fn
      {^method, _path, headers} -> headers
      _ -> nil
    end)
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(to_string(key)) == name, do: value
    end)
  end

  defp header_from_response(%{headers: headers}, name) do
    Enum.find_value(headers, fn
      {key, value} ->
        if String.downcase(to_string(key)) == name, do: header_value(value)

      _ ->
        nil
    end)
  end

  defp header_value(value) when is_binary(value), do: value
  defp header_value([value | _]) when is_binary(value), do: value
  defp header_value(_), do: nil

  defp encoded_payload_length(bundle, digest) do
    prefix = String.slice(digest, 0, 2)
    blob = Path.join([bundle, "blobs", prefix, digest <> ".blob"])
    File.stat!(blob).size - 92
  end
end
