defmodule ElixirDB.HTTP.Router do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}

  plug(:match)
  plug(:dispatch)

  post "/v1/databases" do
    Request.call(conn, fn body, conn ->
      if unknown_fields?(body, ["path", "config"]) do
        Response.error(
          conn,
          ElixirDB.Error.invalid_request("database creation contains an unknown field")
        )
      else
        path = body["path"] || "#{ElixirDB.UUID.v4()}.db"

        if Map.has_key?(body, "path") and not is_binary(body["path"]) do
          Response.error(conn, ElixirDB.Error.invalid_request("database path must be a string"))
        else
          case body["config"] do
            config when is_map(config) or is_nil(config) ->
              case ElixirDB.Runtime.DatabaseCatalog.create(
                     path,
                     if(config, do: %{config: config}, else: %{})
                   ) do
                {:ok, info} -> Response.ok(conn, info, 201)
                {:error, error} -> Response.error(conn, error)
              end

            _ ->
              Response.error(
                conn,
                ElixirDB.Error.invalid_request("database config must be an object")
              )
          end
        end
      end
    end)
  end

  post "/v1/registrations" do
    Request.call(conn, fn body, conn ->
      if unknown_fields?(body, ["path"]) do
        Response.error(
          conn,
          ElixirDB.Error.invalid_request("registration contains an unknown field")
        )
      else
        case ElixirDB.Runtime.DatabaseCatalog.register(body["path"]) do
          {:ok, info} -> Response.ok(conn, info, 201)
          {:error, error} -> Response.error(conn, error)
        end
      end
    end)
  end

  get "/v1/databases" do
    case ElixirDB.Runtime.DatabaseCatalog.list() do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  delete "/v1/registrations/:uuid" do
    case ElixirDB.Runtime.DatabaseCatalog.unregister(Request.uuid(conn)) do
      :ok -> Response.ok(conn, %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/v1/databases/:uuid" do
    case ElixirDB.Runtime.DatabaseCatalog.info(Request.uuid(conn)) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/v1/databases/:uuid/config" do
    case ElixirDB.Runtime.DatabaseCatalog.info(Request.uuid(conn)) do
      {:ok, data} -> Response.ok(conn, data.config || %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  put "/v1/databases/:uuid/config" do
    Request.call(conn, fn body, conn ->
      if is_map(body),
        do:
          result(
            conn,
            ElixirDB.Runtime.DatabaseCatalog.command(
              Request.uuid(conn),
              {:command, :update_config, body}
            )
          ),
        else:
          Response.error(conn, ElixirDB.Error.invalid_request("configuration must be an object"))
    end)
  end

  post "/v1/databases/:uuid/close" do
    case ElixirDB.Runtime.DatabaseCatalog.close(Request.uuid(conn)) do
      :ok -> Response.ok(conn, %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  post "/v1/databases/:uuid/integrity-check" do
    case ElixirDB.Runtime.DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :integrity_check, %{}}
         ) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  post "/v1/databases/:uuid/documents/get" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.get(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/documents/put" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.put(Request.uuid(conn), body), 201)
    end)
  end

  post "/v1/databases/:uuid/documents/delete" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.delete(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/documents/resolve" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.resolve(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/documents/bulk-get" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.bulk_get(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/documents/bulk-write" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Documents.bulk_write(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/changes" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/changes/stream" do
    Request.call(conn, fn body, conn ->
      with :ok <- validate_stream_request(body),
           {:ok, changes} <- stream_read(Request.uuid(conn), body),
           {:ok, conn} <- start_stream(conn),
           {:ok, conn} <- stream_events(conn, changes) do
        stream_follow(Request.uuid(conn), conn, changes.last_sequence, body)
      else
        {:error, %ElixirDB.Error{} = error} -> Response.error(conn, error)
        {:error, _reason} -> conn
      end
    end)
  end

  post "/v1/databases/:uuid/indexes" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Query.create_index(Request.uuid(conn), body), 201)
    end)
  end

  get "/v1/databases/:uuid/indexes" do
    result(conn, ElixirDB.Query.list_indexes(Request.uuid(conn)))
  end

  delete "/v1/databases/:uuid/indexes/:index_id" do
    result(conn, ElixirDB.Query.delete_index(Request.uuid(conn), conn.path_params["index_id"]))
  end

  post "/v1/databases/:uuid/indexes/:index_id/rebuild" do
    result(conn, ElixirDB.Query.rebuild_index(Request.uuid(conn), conn.path_params["index_id"]))
  end

  post "/v1/databases/:uuid/query" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Query.execute(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/query/explain" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Query.explain(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/replications" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Replication.JobManager.put(Request.uuid(conn), body), 201)
    end)
  end

  get "/v1/databases/:uuid/replications" do
    result(conn, ElixirDB.Replication.JobManager.list(Request.uuid(conn)))
  end

  get "/v1/databases/:uuid/replications/:job_id" do
    result(
      conn,
      ElixirDB.Replication.JobManager.get(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/v1/databases/:uuid/replications/:job_id/start" do
    result(
      conn,
      ElixirDB.Replication.JobManager.start(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/v1/databases/:uuid/replications/:job_id/cancel" do
    result(
      conn,
      ElixirDB.Replication.JobManager.cancel(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/v1/databases/:uuid/replications/:job_id/enable" do
    result(
      conn,
      ElixirDB.Replication.JobManager.enable(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/v1/databases/:uuid/replications/:job_id/disable" do
    result(
      conn,
      ElixirDB.Replication.JobManager.disable(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  delete "/v1/databases/:uuid/replications/:job_id" do
    result(
      conn,
      ElixirDB.Replication.JobManager.delete(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  get "/v1/databases/:uuid/replication/identity" do
    case ElixirDB.Runtime.DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :identity, %{}}
         ) do
      {:ok, identity} ->
        result(
          conn,
          {:ok,
           %{
             "database_uuid" => identity.database_uuid,
             "current_sequence" => identity.current_sequence,
             "replication_protocol_major" => identity.replication_protocol_major,
             "revision_algorithm_version" => identity.revision_algorithm_version,
             "canonicalization_version" => identity.canonicalization_version
           }}
        )

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  post "/v1/databases/:uuid/replication/changes" do
    Request.call(conn, fn body, conn ->
      result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
    end)
  end

  post "/v1/databases/:uuid/replication/revisions/diff" do
    Request.call(conn, fn body, conn ->
      result(
        conn,
        ElixirDB.Runtime.DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :diff_revisions, body}
        )
      )
    end)
  end

  post "/v1/databases/:uuid/replication/revisions/get" do
    Request.call(conn, fn body, conn ->
      result(
        conn,
        ElixirDB.Runtime.DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_revision_chains, body}
        )
      )
    end)
  end

  post "/v1/databases/:uuid/replication/revisions/put" do
    Request.call(conn, fn body, conn ->
      result(
        conn,
        ElixirDB.Runtime.DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :import_revision_chains, body}
        )
      )
    end)
  end

  get "/v1/databases/:uuid/replication/checkpoints/:replication_id" do
    result(
      conn,
      ElixirDB.Runtime.DatabaseCatalog.command(
        Request.uuid(conn),
        {:command, :get_local_record, "checkpoints", conn.path_params["replication_id"]}
      )
    )
  end

  put "/v1/databases/:uuid/replication/checkpoints/:replication_id" do
    Request.call(conn, fn body, conn ->
      if valid_checkpoint_put?(body),
        do:
          result(
            conn,
            ElixirDB.Runtime.DatabaseCatalog.command(
              Request.uuid(conn),
              {:command, :put_local_record,
               %{
                 namespace: "checkpoints",
                 key: conn.path_params["replication_id"],
                 expected_version: body["expected_checkpoint_version"],
                 value: Map.delete(body, "expected_checkpoint_version")
               }}
            )
          ),
        else:
          Response.error(
            conn,
            ElixirDB.Error.invalid_request("checkpoint PUT has an invalid replacement")
          )
    end)
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp result(conn, result, status \\ 200)
  defp result(conn, {:ok, data}, status), do: Response.ok(conn, data, status)
  defp result(conn, {:error, error}, _status), do: Response.error(conn, error)
  defp result(conn, :ok, status), do: Response.ok(conn, %{}, status)

  defp unknown_fields?(map, allowed) when is_map(map),
    do: Enum.any?(Map.keys(map), &(&1 not in allowed))

  defp unknown_fields?(_, _), do: true

  defp valid_checkpoint_put?(body) when is_map(body) do
    allowed = [
      "expected_checkpoint_version",
      "version",
      "checkpoint_version",
      "replication_id",
      "session_id",
      "source_sequence",
      "history"
    ]

    Enum.all?(Map.keys(body), &(&1 in allowed)) and
      is_integer(body["expected_checkpoint_version"]) and
      body["expected_checkpoint_version"] >= 0 and
      body["version"] == 1 and
      is_integer(body["checkpoint_version"]) and body["checkpoint_version"] > 0 and
      is_binary(body["replication_id"]) and body["replication_id"] != "" and
      is_binary(body["session_id"]) and body["session_id"] != "" and
      is_integer(body["source_sequence"]) and body["source_sequence"] >= 0 and
      is_list(body["history"])
  end

  defp valid_checkpoint_put?(_), do: false

  defp validate_stream_request(body) when is_map(body) do
    allowed = ["since", "limit", "heartbeat_ms"]

    if Enum.all?(Map.keys(body), &(&1 in allowed)) do
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
    else
      {:error, ElixirDB.Error.invalid_request("changes stream contains an unknown field")}
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
