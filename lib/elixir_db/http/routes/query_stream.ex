defmodule ElixirDB.HTTP.Routes.QueryStream do
  @moduledoc false

  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.HTTP.Schemas
  alias ElixirDB.Observability.Instrumentation.Subscription, as: SubscriptionTelemetry
  alias ElixirDB.Query.Subscription.Events
  alias ElixirDB.Query.Subscriptions

  @doc "Handles `POST /v1/databases/:uuid/query/stream`."
  def stream(conn) do
    Request.call(
      conn,
      Schemas.opts(:query_stream, "query stream contains an unknown field"),
      fn body, conn ->
        uuid = Request.uuid(conn)

        case Subscriptions.open(uuid, body, self()) do
          {:ok, subscription} ->
            SubscriptionTelemetry.open(uuid)
            {:ok, conn} = start_stream(conn)
            stream_loop(conn, uuid, subscription, next_timeout(body))

          {:error, %ElixirDB.Error{} = error} ->
            Response.error(conn, error)
        end
      end
    )
  end

  defp start_stream(conn) do
    {:ok,
     conn
     |> Plug.Conn.put_resp_content_type("application/x-ndjson")
     |> Plug.Conn.send_chunked(200)}
  end

  defp stream_loop(conn, uuid, subscription, next_timeout) do
    case Subscriptions.next(subscription, next_timeout) do
      {:ok, event} ->
        maybe_record_update(uuid, event)

        case chunk_event(conn, event) do
          {:ok, conn} -> stream_loop(conn, uuid, subscription, next_timeout)
          {:error, _} -> close_and_halt(subscription, conn)
        end

      {:closed, event} ->
        _ = chunk_event(conn, event)
        Subscriptions.close(subscription)
        conn

      {:error, event} ->
        maybe_record_overload(uuid, event)
        _ = chunk_event(conn, event)
        Subscriptions.close(subscription)
        conn
    end
  end

  defp next_timeout(body) when is_map(body) do
    heartbeat_ms =
      case body do
        %{"heartbeat_ms" => value} when is_integer(value) and value > 0 -> value
        %{heartbeat_ms: value} when is_integer(value) and value > 0 -> value
        _ -> 15_000
      end

    # GenServer.call must outlive the subscription heartbeat timer.
    heartbeat_ms + 5_000
  end

  defp chunk_event(conn, event) do
    Plug.Conn.chunk(conn, [JSON.encode_to_iodata!(Events.public(event)), "\n"])
  end

  defp maybe_record_update(uuid, %{type: type})
       when type in [:upsert, :remove, :snapshot, :reset, :caught_up] do
    SubscriptionTelemetry.update(uuid, type)
  end

  defp maybe_record_update(_uuid, _event), do: :ok

  defp maybe_record_overload(uuid, %{
         type: :error,
         error: %ElixirDB.Error{code: :subscription_overloaded}
       }) do
    SubscriptionTelemetry.overload(uuid)
  end

  defp maybe_record_overload(_uuid, _event), do: :ok

  defp close_and_halt(subscription, conn) do
    Subscriptions.close(subscription)
    conn
  end
end
