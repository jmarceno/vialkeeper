defmodule VialKeeper.HTTP.Routes.Replications do
  @moduledoc "HTTP routes for durable replication-job management."
  use Plug.Router
  use VialKeeper.HTTP.RouterSpecs
  alias VialKeeper.HTTP.{Request, Response, Schemas}
  alias VialKeeper.Replication.JobManager
  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:replication_job, "replication job contains an unknown field"),
      fn body, conn ->
        Response.result(conn, JobManager.put(Request.uuid(conn), body), 201)
      end
    )
  end

  get "/" do
    Response.result(conn, JobManager.list(Request.uuid(conn)))
  end

  get "/:job_id" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.get(Request.uuid(conn), job_id))
    end)
  end

  post "/:job_id/start" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.start(Request.uuid(conn), job_id))
    end)
  end

  post "/:job_id/cancel" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.cancel(Request.uuid(conn), job_id))
    end)
  end

  post "/:job_id/enable" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.enable(Request.uuid(conn), job_id))
    end)
  end

  post "/:job_id/disable" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.disable(Request.uuid(conn), job_id))
    end)
  end

  delete "/:job_id" do
    with_path_id(conn, fn conn, job_id ->
      Response.result(conn, JobManager.delete(Request.uuid(conn), job_id))
    end)
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  # SAFETY: bounds-check the :job_id path parameter (length/UTF-8/control chars) before it
  # is used as a job identifier. See Request.validate_path_id/1.
  defp with_path_id(conn, fun) do
    case Request.validate_path_id(conn.path_params["job_id"]) do
      :ok -> fun.(conn, conn.path_params["job_id"])
      {:error, error} -> Response.error(conn, error)
    end
  end
end
