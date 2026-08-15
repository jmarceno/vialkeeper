defmodule VialKeeper.HTTP.Routes.Federation do
  @moduledoc "HTTP routes for explicit and named federation queries."

  use Plug.Router

  alias VialKeeper.Federation
  alias VialKeeper.Federation.SavedQueries
  alias VialKeeper.HTTP.{Request, Response, Schemas}
  alias VialKeeper.MapAccess

  plug(:match)
  plug(:dispatch)

  post "/query" do
    Request.call(
      conn,
      Schemas.opts(:federation_query, "federation query contains an unknown field"),
      fn body, conn ->
        Response.result(conn, Federation.query(body))
      end
    )
  end

  get "/saved-queries" do
    Response.ok(conn, Enum.map(SavedQueries.list(), &public_saved_query/1))
  end

  post "/saved-queries/execute" do
    Request.call(
      conn,
      Schemas.opts(
        :federation_saved_query_execute,
        "saved federation query execution contains an unknown field"
      ),
      fn body, conn -> execute_saved(conn, body) end
    )
  end

  match _ do
    Response.error(
      conn,
      VialKeeper.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp execute_saved(conn, body) do
    case Map.get(body, "name") do
      name when is_binary(name) and name != "" ->
        case SavedQueries.get(name) do
          nil -> Response.error(conn, VialKeeper.Error.invalid_request("saved query not found"))
          saved -> Response.result(conn, Federation.query(saved_request(saved, body)))
        end

      _ ->
        Response.error(conn, VialKeeper.Error.invalid_request("saved query name is required"))
    end
  end

  defp saved_request(saved, body) do
    query = MapAccess.get(saved, :query, %{})

    %{
      databases: MapAccess.get(saved, :databases, []),
      query: %{
        selector: MapAccess.get(query, :selector, %{}),
        fields: MapAccess.get(query, :fields),
        sort: MapAccess.get(query, :sort, []),
        limit: request_limit(body, query),
        bookmark: Map.get(body, "bookmark")
      }
    }
  end

  defp request_limit(body, query) do
    case Map.get(body, "limit") do
      nil -> MapAccess.get(query, :limit)
      limit -> limit
    end
  end

  defp public_saved_query(saved) do
    query = MapAccess.get(saved, :query, %{})

    %{
      "name" => MapAccess.get(saved, :name),
      "sources" => MapAccess.get(saved, :databases, []),
      "query" => %{
        "selector" => MapAccess.get(query, :selector, %{}),
        "fields" => MapAccess.get(query, :fields),
        "sort" => public_sort(MapAccess.get(query, :sort, [])),
        "limit" => MapAccess.get(query, :limit)
      }
    }
  end

  defp public_sort(sort) when is_list(sort) do
    Enum.map(sort, fn field ->
      %{
        "path" => MapAccess.get(field, :path),
        "direction" => MapAccess.get(field, :direction, "asc")
      }
    end)
  end

  defp public_sort(_sort), do: []
end
