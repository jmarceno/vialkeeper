defmodule VialKeeper.Bench.TestHTTP do
  @moduledoc """
  Tiny local HTTP fixture server for dataset downloader tests.

  Never used by production commands. Routes are injected per test.
  """

  alias Plug.Conn

  def init(opts), do: opts

  def call(conn, opts) do
    routes = Keyword.get(opts, :routes, %{})
    key = conn.request_path

    case Map.get(routes, key) do
      nil ->
        Conn.send_resp(conn, 404, "missing")

      route ->
        serve(conn, route)
    end
  end

  defp serve(conn, route) do
    body = Map.get(route, :body, "")
    status = Map.get(route, :status, 200)
    content_type = Map.get(route, :content_type, "application/octet-stream")
    etag = Map.get(route, :etag)
    support_range = Map.get(route, :support_range, false)
    drop_after = Map.get(route, :drop_after)

    conn = Conn.put_resp_header(conn, "content-type", content_type)
    conn = if is_binary(etag), do: Conn.put_resp_header(conn, "etag", etag), else: conn

    case Conn.get_req_header(conn, "range") do
      ["bytes=" <> range] when support_range ->
        serve_range(conn, body, range)

      _ when is_integer(drop_after) ->
        partial = binary_part(body, 0, min(drop_after, byte_size(body)))
        Conn.send_resp(conn, status, partial)

      _ ->
        Conn.send_resp(conn, status, body)
    end
  end

  defp serve_range(conn, body, range) do
    case parse_range(range, byte_size(body)) do
      {:ok, start_at, stop_at} ->
        slice = binary_part(body, start_at, stop_at - start_at + 1)

        conn
        |> Conn.put_resp_header(
          "content-range",
          "bytes #{start_at}-#{stop_at}/#{byte_size(body)}"
        )
        |> Conn.send_resp(206, slice)

      :error ->
        Conn.send_resp(conn, 416, "")
    end
  end

  defp parse_range(range, size) do
    case String.split(range, "-", parts: 2) do
      [start_s, ""] ->
        case Integer.parse(start_s) do
          {start_at, ""} -> {:ok, start_at, size - 1}
          _ -> :error
        end

      [start_s, stop_s] ->
        case {Integer.parse(start_s), Integer.parse(stop_s)} do
          {{start_at, ""}, {stop_at, ""}} -> {:ok, start_at, min(stop_at, size - 1)}
          _ -> :error
        end

      _ ->
        :error
    end
  end
end
