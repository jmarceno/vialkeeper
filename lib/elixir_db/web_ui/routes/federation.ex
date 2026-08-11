defmodule ElixirDB.WebUI.Routes.Federation do
  @moduledoc """
  Ad-hoc and saved federation query fragments for the administration console.

  Ad-hoc forms call `Federation.query/1` with an explicit source UUID list and
  strict query JSON. Saved definitions are listed and executed through
  `Federation.SavedQueries`; create/edit/delete remains host.toml-owned.
  """

  alias ElixirDB.Error
  alias ElixirDB.Federation
  alias ElixirDB.Federation.SavedQueries
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @example_query ~s({\n  "selector": {},\n  "limit": 25\n})

  @doc "Renders the federation console with ad-hoc and saved-query panels."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    Response.fragment(conn, render_console(SavedQueries.list(), nil, nil, "", @example_query))
  end

  @doc "Executes an ad-hoc federated query and renders the result page."
  @spec query(Plug.Conn.t()) :: Plug.Conn.t()
  def query(conn) do
    with {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, databases} <- decode_databases(params),
         {:ok, query} <- decode_query(params),
         request <- federation_request(databases, query, params),
         result <- Federation.query(request) do
      case result do
        {:ok, page} ->
          Response.fragment(
            conn,
            render_console(
              SavedQueries.list(),
              page,
              nil,
              databases_text(databases),
              Request.param(params, "query") || @example_query
            )
          )

        {:error, %Error{} = error} ->
          Response.fragment(
            conn,
            render_console(
              SavedQueries.list(),
              nil,
              error,
              databases_text(databases),
              Request.param(params, "query") || @example_query
            )
          )
      end
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Executes a host-configured saved federation query."
  @spec execute_saved(Plug.Conn.t()) :: Plug.Conn.t()
  def execute_saved(conn) do
    with {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, name} <- require_saved_name(params),
         {:ok, saved} <- require_saved(name),
         request <- saved_request(saved, params),
         result <- Federation.query(request) do
      case result do
        {:ok, page} ->
          Response.fragment(
            conn,
            render_console(SavedQueries.list(), page, nil, "", @example_query, name)
          )

        {:error, %Error{} = error} ->
          Response.fragment(
            conn,
            render_console(SavedQueries.list(), nil, error, "", @example_query, name)
          )
      end
    else
      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  defp require_saved_name(params) do
    case Request.param(params, "name") do
      name when is_binary(name) and name != "" -> {:ok, name}
      _ -> {:error, Error.invalid_request("saved query name is required")}
    end
  end

  defp require_saved(name) do
    case SavedQueries.get(name) do
      nil -> {:error, Error.invalid_request("saved query not found")}
      saved -> {:ok, saved}
    end
  end

  defp render_console(
         saved,
         page,
         error,
         databases_text,
         query_text,
         executed_saved \\ nil
       ) do
    [
      "<section class=\"stack\">\n",
      Components.page_header(
        "Federation",
        "Cross-database queries over explicitly named sources."
      ),
      ad_hoc_form(databases_text, query_text),
      if(error, do: Components.error_block(error), else: []),
      results_panel(page, databases_text, query_text, executed_saved),
      saved_queries_panel(saved),
      "</section>\n"
    ]
  end

  defp ad_hoc_form(databases_text, query_text) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Ad-hoc query</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("List source database UUIDs in order, one per line."),
      "</p>\n",
      "    <form class=\"stack\" hx-post=\"/ui/actions/federation/query\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Source databases",
        [
          " <textarea name=\"databases\" rows=\"4\" required spellcheck=\"false\">",
          HTML.textarea(databases_text),
          "</textarea>"
        ]
      ),
      Components.field(
        "Query JSON",
        [
          " <textarea name=\"query\" rows=\"12\" required spellcheck=\"false\">",
          HTML.textarea(query_text),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Execute</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp results_panel(nil, _databases_text, _query_text, _executed_saved), do: []

  defp results_panel(page, databases_text, query_text, executed_saved) when is_map(page) do
    sources = MapAccess.get(page, :sources) || []
    documents = MapAccess.get(page, :documents) || MapAccess.get(page, :results) || []
    bookmark = MapAccess.get(page, :bookmark)

    source_rows =
      Enum.map(sources, fn source ->
        [
          HTML.escape(MapAccess.get(source, :database_uuid) |> to_string()),
          HTML.escape(MapAccess.get(source, :sequence) |> to_string())
        ]
      end)

    doc_rows =
      Enum.map(documents, fn doc ->
        [
          HTML.escape(MapAccess.get(doc, :id) |> to_string()),
          HTML.escape(MapAccess.get(doc, :source_database_uuid) |> to_string()),
          ["<pre class=\"mono\">", HTML.textarea(public_document(doc)), "</pre>"]
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Source sequence vector</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("Independent source snapshots used for this page, in query order."),
      "</p>\n",
      Components.table("Federation sources", ["Database UUID", "Sequence"], source_rows),
      "  </div>\n",
      "  <div class=\"panel\">\n",
      "    <h2>Results</h2>\n",
      Components.table(nil, ["ID", "Source", "Projection"], doc_rows),
      pagination(page, bookmark, databases_text, query_text, executed_saved),
      "  </div>\n"
    ]
  end

  defp pagination(_page, bookmark, _databases_text, _query_text, _executed_saved)
       when not is_binary(bookmark) or bookmark == "",
       do: []

  defp pagination(_page, bookmark, _databases_text, _query_text, name)
       when is_binary(name) and name != "" do
    [
      "<form class=\"row\" hx-post=\"/ui/actions/federation/saved-queries/execute\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "  <input type=\"hidden\" name=\"name\" value=\"",
      HTML.attr(name),
      "\">\n",
      "  <input type=\"hidden\" name=\"bookmark\" value=\"",
      HTML.attr(bookmark),
      "\">\n",
      "  <button type=\"submit\">Next page</button>\n",
      "</form>\n"
    ]
  end

  defp pagination(_page, bookmark, databases_text, query_text, _executed_saved) do
    [
      "<form class=\"row\" hx-post=\"/ui/actions/federation/query\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "  <input type=\"hidden\" name=\"bookmark\" value=\"",
      HTML.attr(bookmark),
      "\">\n",
      "  <textarea name=\"databases\" hidden>",
      HTML.textarea(databases_text),
      "</textarea>\n",
      "  <textarea name=\"query\" hidden>",
      HTML.textarea(query_text),
      "</textarea>\n",
      "  <button type=\"submit\">Next page</button>\n",
      "</form>\n"
    ]
  end

  defp saved_queries_panel(saved) do
    rows =
      Enum.map(saved, fn entry ->
        name = MapAccess.get(entry, :name) |> to_string()
        sources = MapAccess.get(entry, :databases, [])

        [
          HTML.escape(name),
          HTML.escape(Enum.map_join(sources, ", ", &to_string/1)),
          saved_execute_form(name)
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Saved queries</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape(
        "Definitions are operator-owned in host.toml. Editing requires host.toml changes and a restart; this console cannot create, edit, or delete saved queries."
      ),
      "</p>\n",
      Components.table(nil, ["Name", "Sources", "Actions"], rows),
      "  </div>\n"
    ]
  end

  defp saved_execute_form(name) do
    [
      "<form class=\"row\" hx-post=\"/ui/actions/federation/saved-queries/execute\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "  <input type=\"hidden\" name=\"name\" value=\"",
      HTML.attr(name),
      "\">\n",
      "  <button type=\"submit\">Execute</button>\n",
      "</form>\n"
    ]
  end

  defp decode_databases(params) do
    case Request.param(params, "databases") do
      value when is_binary(value) -> decode_databases_text(String.trim(value))
      _ -> {:error, Error.invalid_request("databases must list at least one UUID")}
    end
  end

  defp decode_databases_text(""),
    do: {:error, Error.invalid_request("databases must list at least one UUID")}

  defp decode_databases_text("[" <> _ = trimmed) do
    case StrictDecoder.decode(trimmed) do
      {:ok, list} when is_list(list) -> validate_uuid_list(list)
      {:ok, _} -> {:error, Error.invalid_request("databases must be a JSON array of UUIDs")}
      {:error, _} = error -> error
    end
  end

  defp decode_databases_text(trimmed) do
    trimmed
    |> String.split(~r/[\s,]+/, trim: true)
    |> validate_uuid_list()
  end

  defp validate_uuid_list(list) when is_list(list) do
    Enum.reduce_while(list, {:ok, []}, fn item, {:ok, acc} ->
      case Request.require_uuid(to_string(item)) do
        {:ok, uuid} -> {:cont, {:ok, acc ++ [uuid]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, []} -> {:error, Error.invalid_request("databases must list at least one UUID")}
      other -> other
    end
  end

  defp decode_query(params) do
    with {:ok, query} <- Request.decode_json_field(params, "query"),
         true <- is_map(query) do
      {:ok, query}
    else
      false -> {:error, Error.invalid_request("query must be a JSON object")}
      {:error, _} = error -> error
    end
  end

  defp federation_request(databases, query, params) do
    query =
      case Request.param(params, "bookmark") do
        bookmark when is_binary(bookmark) and bookmark != "" ->
          Map.put(query, "bookmark", bookmark)

        _ ->
          query
      end

    %{"databases" => databases, "query" => query}
  end

  defp saved_request(saved, params) do
    query = MapAccess.get(saved, :query, %{})

    request_query = %{
      "selector" => MapAccess.get(query, :selector, %{}),
      "fields" => MapAccess.get(query, :fields),
      "sort" => public_sort(MapAccess.get(query, :sort, [])),
      "limit" => saved_limit(params, query)
    }

    request_query =
      case Request.param(params, "bookmark") do
        bookmark when is_binary(bookmark) and bookmark != "" ->
          Map.put(request_query, "bookmark", bookmark)

        _ ->
          request_query
      end

    %{
      "databases" => MapAccess.get(saved, :databases, []),
      "query" => request_query
    }
  end

  defp saved_limit(params, query) do
    case Request.param(params, "limit") do
      limit when is_binary(limit) and limit != "" ->
        case Integer.parse(limit) do
          {value, ""} -> value
          _ -> MapAccess.get(query, :limit)
        end

      _ ->
        MapAccess.get(query, :limit)
    end
  end

  defp public_sort(sort) when is_list(sort) do
    Enum.map(sort, fn field ->
      %{
        "path" => MapAccess.get(field, :path),
        "direction" => MapAccess.get(field, :direction, "asc")
      }
    end)
  end

  defp public_sort(_), do: []

  defp public_document(doc) when is_map(doc) do
    Map.new(doc, fn
      {key, _value} when key in ["body", :body] -> {stringify_key(key), "[omitted]"}
      {key, value} -> {stringify_key(key), public_value(value)}
    end)
  end

  defp public_value(map) when is_map(map) and not is_struct(map), do: public_document(map)
  defp public_value(list) when is_list(list), do: Enum.map(list, &public_value/1)
  defp public_value(value) when is_boolean(value), do: value
  defp public_value(nil), do: nil
  defp public_value(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp public_value(other), do: other

  defp stringify_key(key) when is_atom(key), do: Atom.to_string(key)
  defp stringify_key(key) when is_binary(key), do: key
  defp stringify_key(key), do: to_string(key)

  defp databases_text(databases) when is_list(databases), do: Enum.join(databases, "\n")
  defp databases_text(_), do: ""
end
