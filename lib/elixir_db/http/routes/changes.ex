defmodule ElixirDB.HTTP.Routes.Changes do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.Changes.Request, as: ChangesRequest
  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.HTTP.Schemas
  alias ElixirDB.Runtime.ChangeNotifier
  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:changes, "changes request contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
      end
    )
  end

  post "/stream" do
    Request.call(
      conn,
      Schemas.opts(:changes_stream, "changes stream contains an unknown field"),
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
    values = %{
      since: body["since"] || 0,
      limit: body["limit"] || 100,
      heartbeat: body["heartbeat_ms"] || 0
    }

    validate_stream_values(values)
  end

  defp validate_stream_request(_),
    do: {:error, ElixirDB.Error.invalid_request("changes stream request must be an object")}

  defp validate_stream_values(%{since: since, limit: limit, heartbeat: heartbeat}) do
    max_batch = ElixirDB.Config.host_limits()[:max_changes_batch] || 500
    max_wait = ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000

    validators = [
      fn -> valid_stream_since(since) end,
      fn -> valid_stream_limit(limit) end,
      fn -> valid_stream_limit_max(limit, max_batch) end,
      fn -> valid_heartbeat(heartbeat) end,
      fn -> valid_heartbeat_max(heartbeat, max_wait) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> {:error, error}
    end
  end

  defp valid_stream_since(value) when is_integer(value) and value >= 0, do: nil

  defp valid_stream_since(_),
    do: ElixirDB.Error.invalid_request("since must be a non-negative integer")

  defp valid_stream_limit(value) when is_integer(value) and value > 0, do: nil
  defp valid_stream_limit(_), do: ElixirDB.Error.invalid_request("limit must be a positive integer")

  defp valid_stream_limit_max(value, max) when value <= max, do: nil

  defp valid_stream_limit_max(_value, _max),
    do: ElixirDB.Error.resource_limit("changes stream limit exceeds the host limit")

  defp valid_heartbeat(value) when is_integer(value) and value >= 0, do: nil

  defp valid_heartbeat(_),
    do: ElixirDB.Error.invalid_request("heartbeat_ms must be a non-negative integer")

  defp valid_heartbeat_max(value, max) when value <= max, do: nil

  defp valid_heartbeat_max(_value, _max),
    do: ElixirDB.Error.resource_limit("heartbeat_ms exceeds the host limit")

  defp start_stream(conn) do
    {:ok,
     conn
     |> Plug.Conn.put_resp_content_type("application/x-ndjson")
     |> Plug.Conn.send_chunked(200)}
  end

  defp stream_read(uuid, body) do
    request = %ChangesRequest{since: body["since"] || 0, limit: body["limit"] || 100, wait_ms: 0}

    ElixirDB.Changes.read(uuid, request)
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
      subscribe_and_follow(uuid, conn, since, body, heartbeat_ms)
    end
  end

  defp subscribe_and_follow(uuid, conn, since, body, heartbeat_ms) do
    case ChangeNotifier.subscribe(uuid, since) do
      {:ok, ref, current} ->
        follow_subscription(uuid, conn, since, body, heartbeat_ms, ref, current)

      {:error, error} ->
        stream_error(conn, error)
    end
  end

  defp follow_subscription(uuid, conn, since, body, heartbeat_ms, ref, current) do
    if current > since do
      ChangeNotifier.unsubscribe(uuid, ref)
      stream_next_batch(uuid, conn, since, body)
    else
      receive_stream_event(uuid, conn, since, body, heartbeat_ms, ref)
    end
  end

  defp receive_stream_event(uuid, conn, since, body, heartbeat_ms, ref) do
    receive do
      {:database_changed, ^uuid, _sequence} ->
        ChangeNotifier.unsubscribe(uuid, ref)
        stream_next_batch(uuid, conn, since, body)

      {:database_closed, ^uuid} ->
        ChangeNotifier.unsubscribe(uuid, ref)
        _ = Plug.Conn.chunk(conn, [JSON.encode_to_iodata!(%{"type" => "closed"}), "\n"])
        conn
    after
      heartbeat_ms ->
        ChangeNotifier.unsubscribe(uuid, ref)
        stream_heartbeat(uuid, conn, since, body)
    end
  end

  defp stream_heartbeat(uuid, conn, since, body) do
    case Plug.Conn.chunk(conn, [JSON.encode_to_iodata!(%{"type" => "heartbeat"}), "\n"]) do
      {:ok, conn} -> stream_follow(uuid, conn, since, body)
      {:error, _} -> conn
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
