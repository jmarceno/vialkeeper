defmodule ElixirDB.HTTP.Routes.ReplicationWire do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}

  plug(:match)
  plug(:dispatch)

  get "/identity" do
    case ElixirDB.Runtime.DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :identity, %{}}
         ) do
      {:ok, identity} ->
        Response.result(
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

  post "/changes" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:wire_changes, "replication changes contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
      end
    )
  end

  post "/revisions/diff" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:wire_diff, "revision diff contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          ElixirDB.Runtime.DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :diff_revisions, body}
          )
        )
      end
    )
  end

  post "/revisions/get" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:wire_get_chains, "revision get contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          ElixirDB.Runtime.DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :get_revision_chains, body}
          )
        )
      end
    )
  end

  post "/revisions/put" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:wire_put_chains, "revision put contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          ElixirDB.Runtime.DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :import_revision_chains, body}
          )
        )
      end
    )
  end

  get "/checkpoints/:replication_id" do
    with_path_id(conn, fn conn, replication_id ->
      Response.result(
        conn,
        ElixirDB.Runtime.DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_local_record, "checkpoints", replication_id}
        )
      )
    end)
  end

  put "/checkpoints/:replication_id" do
    Request.call(
      conn,
      ElixirDB.HTTP.Schemas.opts(:wire_checkpoint, "checkpoint PUT has an invalid replacement"),
      fn body, conn ->
        if valid_checkpoint_put?(body),
          do:
            with_path_id(conn, fn conn, replication_id ->
              Response.result(
                conn,
                ElixirDB.Runtime.DatabaseCatalog.command(
                  Request.uuid(conn),
                  {:command, :put_local_record,
                   %{
                     namespace: "checkpoints",
                     key: replication_id,
                     expected_version: body["expected_checkpoint_version"],
                     value: Map.delete(body, "expected_checkpoint_version")
                   }}
                )
              )
            end),
          else:
            Response.error(
              conn,
              ElixirDB.Error.invalid_request("checkpoint PUT has an invalid replacement")
            )
      end
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp valid_checkpoint_put?(body) when is_map(body) do
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

  # SAFETY: bounds-check the :replication_id path parameter before it is used as a storage
  # key. See Request.validate_path_id/1.
  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["replication_id"]) do
      :ok -> fun.(conn, conn.path_params["replication_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end
end
