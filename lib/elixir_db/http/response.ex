defmodule ElixirDB.HTTP.Response do
  @moduledoc false
  import Plug.Conn

  def request_id(conn) do
    case get_req_header(conn, "x-request-id") do
      [value | _] when byte_size(value) <= 128 and value != "" ->
        if Regex.match?(~r/^[A-Za-z0-9._-]+$/, value), do: value, else: ElixirDB.UUID.v4()

      _ ->
        ElixirDB.UUID.v4()
    end
  end

  def ok(conn, data, status \\ 200) do
    send_json(conn, status, %{"request_id" => request_id(conn), "data" => data})
  end

  def error(conn, %ElixirDB.Error{} = error),
    do:
      send_json(conn, error.http_status, %{
        "request_id" => request_id(conn),
        "error" => ElixirDB.Error.public(error)
      })

  def result(conn, result, status \\ 200)
  def result(conn, {:ok, data}, status), do: ok(conn, data, status)
  def result(conn, {:error, error}, _status), do: error(conn, error)
  def result(conn, :ok, status), do: ok(conn, %{}, status)

  def send_json(conn, status, body) do
    request_id = body["request_id"] || body[:request_id] || request_id(conn)

    conn
    |> put_resp_header("x-request-id", request_id)
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode_to_iodata!(body))
  end
end
