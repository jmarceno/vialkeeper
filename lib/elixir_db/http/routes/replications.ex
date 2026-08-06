defmodule ElixirDB.HTTP.Routes.Replications do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response, Schemas}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:replication_job, "replication job contains an unknown field"),
      fn body, conn ->
        Response.result(conn, ElixirDB.Replication.JobManager.put(Request.uuid(conn), body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, ElixirDB.Replication.JobManager.list(Request.uuid(conn)))
  end

  get "/:job_id" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.get(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/:job_id/start" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.start(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/:job_id/cancel" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.cancel(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/:job_id/enable" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.enable(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  post "/:job_id/disable" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.disable(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  delete "/:job_id" do
    Response.result(
      conn,
      ElixirDB.Replication.JobManager.delete(Request.uuid(conn), conn.path_params["job_id"])
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
