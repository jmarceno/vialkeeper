defmodule ElixirDB.HTTP.Router do
  @moduledoc false
  use Plug.Router

  alias ElixirDB.HTTP.Response

  alias ElixirDB.HTTP.Routes.{
    Attachments,
    Changes,
    Databases,
    Documents,
    Indexes,
    Replications,
    ReplicationWire
  }

  alias ElixirDB.Observability.Dashboard
  alias ElixirDB.Observability.Instrumentation.HTTP
  # Authentication runs before route matching on every request, so it is the
  # single chokepoint for all `/v1` traffic (AUTH-001) and unauthenticated
  # probes cannot learn which routes exist.
  plug(ElixirDB.HTTP.AuthPlug)
  plug(:match)
  plug(:dispatch)

  # Plan §5.7: wrap the routing pipeline in the elixir_db.http.request server
  # span. Overriding call/2 (rather than a plug in the pipeline) guarantees the
  # span is ended — and the prior trace context restored — even when a
  # downstream plug raises before any response is sent.
  def call(conn, opts) do
    HTTP.wrap(conn, fn conn ->
      super(conn, opts)
    end)
  end

  # More specific database-scoped resources first so they are not swallowed by
  # the `/v1/databases` forward below.
  forward("/v1/databases/:uuid/attachments", to: Attachments)
  forward("/v1/databases/:uuid/documents", to: Documents)
  forward("/v1/databases/:uuid/changes", to: Changes)
  forward("/v1/databases/:uuid/indexes", to: Indexes)
  forward("/v1/databases/:uuid/replications", to: Replications)
  forward("/v1/databases/:uuid/replication", to: ReplicationWire)

  # Query shares the indexes route module (match+delegate).
  post "/v1/databases/:uuid/query" do
    Indexes.query(conn)
  end

  post "/v1/databases/:uuid/query/explain" do
    Indexes.explain(conn)
  end

  # Registrations are sibling URIs; keep them on the databases module.
  post "/v1/registrations" do
    Databases.register(conn)
  end

  delete "/v1/registrations/:uuid" do
    Databases.unregister(conn)
  end

  get "/v1/observability/snapshot" do
    if Application.get_env(:elixir_db, :observability_dashboard, false) do
      Response.ok(conn, Dashboard.snapshot())
    else
      Response.error(
        conn,
        ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
      )
    end
  end

  forward("/v1/databases", to: Databases)

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
