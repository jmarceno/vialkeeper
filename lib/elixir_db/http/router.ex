defmodule ElixirDB.HTTP.Router do
  @moduledoc false
  use Plug.Router

  alias ElixirDB.HTTP.Response

  alias ElixirDB.HTTP.Routes.{
    Changes,
    Databases,
    Documents,
    Indexes,
    ReplicationWire,
    Replications
  }

  plug(:match)
  plug(:dispatch)

  # More specific database-scoped resources first so they are not swallowed by
  # the `/v1/databases` forward below.
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

  forward("/v1/databases", to: Databases)

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end
end
