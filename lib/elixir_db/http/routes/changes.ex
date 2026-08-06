defmodule ElixirDB.HTTP.Routes.Changes do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:changes, "changes request contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
      end
    )
  end

  post "/stream" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:changes_stream, "changes stream contains an unknown field"),
      fn body, conn ->
        with :ok <- validate_stream_request(body),
             {:ok, changes} <- stream_read(Request.uuid(conn), body),
             {:ok, conn} <- start_stream(conn),
             {:ok, conn} <- stream_events(conn, changes) do
          stream_follow(Request.uuid(conn), conn, changes.last_sequence, body)
        else
          {:error, %ElixirDB.Error{} = error} -> Response.error(conn, error)
          {:error, _reason} -> conn
        end
      end
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp validate_stream_request(body) when is_map(body) do
    since = body["since"] || 0
    limit = body["limit"] || 100
    heartbeat = body["heartbeat_ms"] || 0
    max_batch = ElixirDB.Config.host_limits()[:max_changes_batch] || 500
    max_wait = ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000

    cond do
      not is_integer(since) or since < 0 ->
        {:error, ElixirDB.Error.invalid_request("since must be a non-negative integer")}

      not is_integer(limit) or limit <= 0 ->
        {:error, ElixirDB.Error.invalid_request("limit must be a positive integer")}

      limit > max_batch ->
        {:error, ElixirDB.Error.resource_limit("changes stream limit exceeds the host limit")}

      not is_integer(heartbeat) or heartbeat < 0 ->
        {:error, ElixirDB.Error.invalid_request("heartbeat_ms must be a non-negative integer")}

      heartbeat > max_wait ->
        {:error, ElixirDB.Error.resource_limit("heartbeat_ms exceeds the host limit")}

      true ->
        :ok
    end
  end

  defp validate_stream_request(_),
    do: {:error, ElixirDB.Error.invalid_request("changes stream request must be an object")}

  defp start_stream(conn) do
    {:ok,
     conn
     |> Plug.Conn.put_resp_content_type("application/x-ndjson")
     |> Plug.Conn.send_chunked(200)}
  end

  defp stream_read(uuid, body) do
    request = %{since: body["since"] || 0, limit: body["limit"] || 100, wait_ms: 0}

    case ElixirDB.Changes.read(uuid, request) do
      {:ok, changes} -> {:ok, changes}
      {:error, error} -> {:error, error}
    end
  end

  defp stream_events(conn, changes) do
    events =
      Enum.map(changes.results, &%{"type" => "change", "change" => &1}) ++
        [%{"type" => "caught_up", "sequence" => changes.last_sequence}]

    Enum.reduce_while(events, {:ok, conn}, fn event, {:ok, conn} ->
      case Plug.Conn.chunk(conn, [JSON.encode_to_iodata!(event), "\n"]) do
        {:ok, conn} -> {:cont, {:ok, conn}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stream_follow(uuid, conn, since, body) do
    heartbeat_ms = body["heartbeat_ms"] || 0

    if heartbeat_ms == 0 do
      conn
    else
      with {:ok, ref, current} <- ElixirDB.Runtime.ChangeNotifier.subscribe(uuid, since) do
        if current > since do
          ElixirDB.Runtime.ChangeNotifier.unsubscribe(uuid, ref)
          stream_next_batch(uuid, conn, since, body)
        else
          receive do
            {:database_changed, ^uuid, _sequence} ->
              ElixirDB.Runtime.ChangeNotifier.unsubscribe(uuid, ref)
              stream_next_batch(uuid, conn, since, body)

            {:database_closed, ^uuid} ->
              ElixirDB.Runtime.ChangeNotifier.unsubscribe(uuid, ref)
              _ = Plug.Conn.chunk(conn, [JSON.encode_to_iodata!(%{"type" => "closed"}), "\n"])
              conn
          after
            heartbeat_ms ->
              ElixirDB.Runtime.ChangeNotifier.unsubscribe(uuid, ref)

              case Plug.Conn.chunk(conn, [
                     JSON.encode_to_iodata!(%{"type" => "heartbeat"}),
                     "\n"
                   ]) do
                {:ok, conn} -> stream_follow(uuid, conn, since, body)
                {:error, _} -> conn
              end
          end
        end
      else
        {:error, error} -> stream_error(conn, error)
      end
    end
  end

  defp stream_next_batch(uuid, conn, since, body) do
    case stream_read(uuid, %{"since" => since, "limit" => body["limit"] || 100}) do
      {:ok, changes} ->
        case stream_events(conn, changes) do
          {:ok, conn} -> stream_follow(uuid, conn, changes.last_sequence, body)
          {:error, _} -> conn
        end

      {:error, error} ->
        stream_error(conn, error)
    end
  end

  defp stream_error(conn, %ElixirDB.Error{} = error) do
    _ =
      Plug.Conn.chunk(conn, [
        JSON.encode_to_iodata!(%{"type" => "error", "error" => ElixirDB.Error.public(error)}),
        "\n"
      ])

    conn
  end
end
