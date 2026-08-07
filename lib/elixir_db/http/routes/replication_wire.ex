defmodule ElixirDB.HTTP.Routes.ReplicationWire do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.HTTP.Schemas
  alias ElixirDB.Replication.Wire
  alias ElixirDB.Runtime.DatabaseCatalog
  plug(:match)
  plug(:dispatch)

  get "/identity" do
    case DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :identity, %{}}
         ) do
      {:ok, identity} ->
        Response.ok(conn, Wire.identity(identity))

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  post "/boundaries" do
    Request.call(
      conn,
      Schemas.opts(:wire_boundaries, "boundary page request contains an unknown field"),
      fn body, conn ->
        request =
          body
          |> Map.new(fn {k, v} -> {to_string(k), v} end)
          |> boundary_request()

        Response.result(
          conn,
          boundary_page_result(Request.uuid(conn), request)
        )
      end
    )
  end

  post "/boundaries/install" do
    Request.call(
      conn,
      Schemas.opts(:wire_boundaries, "boundary install request contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :install_boundary_pages, body}
          )
        )
      end
    )
  end

  get "/peers" do
    Response.result(
      conn,
      DatabaseCatalog.command(Request.uuid(conn), {:command, :list_peer_positions, %{}})
    )
  end

  get "/local-origin" do
    case DatabaseCatalog.command(Request.uuid(conn), {:command, :has_local_origin_changes}) do
      {:ok, has_local?} when is_boolean(has_local?) ->
        Response.ok(conn, %{"has_local_origin_changes" => has_local?})

      {:error, error} ->
        Response.error(conn, error)
    end
  end

  get "/peers/:peer_database_uuid" do
    with_path_id(conn, fn conn, peer_id ->
      Response.result(
        conn,
        DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_local_record, "peer_ledger", peer_id}
        )
      )
    end)
  end

  put "/peers/:peer_database_uuid" do
    Request.call(conn, fn body, conn ->
      with_path_id(conn, fn conn, _ ->
        peer_id = conn.path_params["peer_database_uuid"]

        request =
          body
          |> Map.new(fn {k, v} -> {to_string(k), v} end)
          |> Map.put("peer_database_uuid", peer_id)

        Response.result(
          conn,
          DatabaseCatalog.command(
            Request.uuid(conn),
            {:command, :put_peer_position_cas, request}
          )
        )
      end)
    end)
  end

  post "/changes" do
    Request.call(
      conn,
      Schemas.opts(:wire_changes, "replication changes contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Changes.wait(Request.uuid(conn), body))
      end
    )
  end

  post "/revisions/diff" do
    Request.call(
      conn,
      Schemas.opts(:wire_diff, "revision diff contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
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
      Schemas.opts(:wire_get_chains, "revision get contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
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
      Schemas.opts(:wire_put_chains, "revision put contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          DatabaseCatalog.command(
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
        DatabaseCatalog.command(
          Request.uuid(conn),
          {:command, :get_local_record, "checkpoints", replication_id}
        )
      )
    end)
  end

  put "/checkpoints/:replication_id" do
    Request.call(
      conn,
      Schemas.opts(:wire_checkpoint, "checkpoint PUT has an invalid replacement"),
      fn body, conn ->
        if valid_checkpoint_put?(body),
          do:
            with_path_id(conn, fn conn, replication_id ->
              Response.result(
                conn,
                DatabaseCatalog.command(
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
    validators = [
      &valid_expected_checkpoint_version?/1,
      &valid_checkpoint_version?/1,
      &valid_replication_id?/1,
      &valid_session_id?/1,
      &valid_source_sequence?/1,
      &valid_history?/1
    ]

    Enum.all?(validators, & &1.(body))
  end

  defp valid_checkpoint_put?(_), do: false

  defp valid_expected_checkpoint_version?(body),
    do: is_integer(body["expected_checkpoint_version"]) and body["expected_checkpoint_version"] >= 0

  defp valid_checkpoint_version?(body),
    do:
      body["version"] == 1 and is_integer(body["checkpoint_version"]) and
        body["checkpoint_version"] > 0

  defp valid_replication_id?(body),
    do: is_binary(body["replication_id"]) and body["replication_id"] != ""

  defp valid_session_id?(body),
    do: is_binary(body["session_id"]) and body["session_id"] != ""

  defp valid_source_sequence?(body),
    do: is_integer(body["source_sequence"]) and body["source_sequence"] >= 0

  defp valid_history?(body), do: is_list(body["history"])

  defp boundary_request(body) when is_map(body) do
    cursor = body["page_cursor"] || body["cursor"]

    %{
      "source_history_epoch" => body["source_history_epoch"],
      "compaction_epoch" => body["compaction_epoch"],
      "cursor" => cursor,
      "limit" => body["limit"]
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp boundary_page_result(uuid, request) do
    case DatabaseCatalog.command(uuid, {:command, :read_boundary_pages, request}) do
      {:ok, page} -> {:ok, Wire.boundary_page(page)}
      error -> error
    end
  end

  # SAFETY: bounds-check the :replication_id path parameter before it is used as a storage
  # key. See Request.validate_path_id/1.
  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["replication_id"]) do
      :ok -> fun.(conn, conn.path_params["replication_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end
end
