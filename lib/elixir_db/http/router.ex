defmodule ElixirDB.HTTP.Router do
  @moduledoc "Top-level Plug router for the ElixirDB HTTP API."
  use Plug.Router

  alias ElixirDB.HTTP.Response

  alias ElixirDB.HTTP.Routes.{
    Attachments,
    Changes,
    Databases,
    Documents,
    Federation,
    Indexes,
    MaterializedViews,
    QueryStream,
    Replications,
    ReplicationWire,
    ShadowControl,
    Shadows,
    Views
  }

  alias ElixirDB.Observability.Dashboard
  alias ElixirDB.Observability.Instrumentation.HTTP
  alias ElixirDB.WebUI.Router, as: WebUIRouter

  # Replication JSON compression is path-scoped and does not authorize. Auth
  # remains the chokepoint for `/v1` traffic (AUTH-001); unauthenticated probes
  # cannot learn which routes exist. The Web UI shell/assets may pass
  # anonymously when enabled; fragment and action routes remain protected.
  plug(ElixirDB.HTTP.ReplicationWirePlug)
  plug(ElixirDB.HTTP.AuthPlug)
  plug(:match)
  plug(:dispatch)

  # Wrap the routing pipeline in the request span. Overriding call/2 rather
  # than using a plug guarantees the span is ended and the prior trace context
  # is restored even when a downstream plug raises before any response is sent.
  def call(conn, opts) do
    HTTP.wrap(conn, fn conn ->
      super(conn, opts)
    end)
  end

  # More specific database-scoped resources first so they are not swallowed by
  # the `/v1/databases` forward below.
  forward("/v1/control-plane", to: ShadowControl)
  forward("/v1/databases/:uuid/shadow", to: Shadows)
  forward("/v1/databases/:uuid/attachments", to: Attachments)
  forward("/v1/databases/:uuid/documents", to: Documents)
  forward("/v1/databases/:uuid/changes", to: Changes)
  forward("/v1/databases/:uuid/indexes", to: Indexes)
  forward("/v1/databases/:uuid/replications", to: Replications)
  forward("/v1/databases/:uuid/replication", to: ReplicationWire)
  forward("/v1/databases/:uuid/views", to: Views)
  forward("/v1/federation", to: Federation)
  forward("/v1/materialized-views", to: MaterializedViews)

  # Query shares the indexes route module (match+delegate).
  # Stream must be registered before the ordinary query POST.
  post "/v1/databases/:uuid/query/stream" do
    QueryStream.stream(conn)
  end

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

  forward("/ui", to: WebUIRouter)

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
