defmodule ElixirDB.WebUI.Routes.Queries do
  @moduledoc """
  Query, explain, and index management fragments for the console.

  JSON textareas are decoded with `StrictDecoder`. Explain and index listings
  render storage-neutral fields only.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Query
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @example_query ~s({\n  "selector": {},\n  "limit": 25\n})

  @example_index ~s({\n  "name": "by-type",\n  "type": "structured",\n  "fields": [{"path": "/type", "type": "string", "direction": "asc"}]\n})

  @explain_keys [
    "plan_kind",
    "plan_digest",
    "selected_indexes",
    "selected_index",
    "union_branches",
    "candidate_indexes",
    "rejected_index_reasons",
    "pushdown_predicates",
    "post_filter_predicates",
    "full_scan",
    "candidate_count",
    "scan_allowed",
    "selector",
    "sort",
    "pagination",
    "sort_compatible",
    "backend_detail"
  ]

  @index_keys [
    "index_id",
    "name",
    "type",
    "fields",
    "tokenization",
    "definition_digest",
    "lifecycle_state"
  ]

  @doc "Renders the query and index console for a database."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, indexes} <- Query.list_indexes(uuid) do
      Response.fragment(conn, render_console(uuid, indexes, nil, nil, @example_query))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Executes a query JSON textarea against the Query facade."
  @spec execute(Plug.Conn.t()) :: Plug.Conn.t()
  def execute(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, request} <- decode_query_request(params),
         {:ok, indexes} <- Query.list_indexes(uuid),
         result <- Query.execute(uuid, request) do
      case result do
        {:ok, page} ->
          Response.fragment(
            conn,
            render_console(
              uuid,
              indexes,
              page,
              nil,
              Request.param(params, "query") || @example_query
            )
          )

        {:error, %Error{} = error} ->
          Response.fragment(
            conn,
            render_console(
              uuid,
              indexes,
              nil,
              error,
              Request.param(params, "query") || @example_query
            )
          )
      end
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Explains a query without executing it."
  @spec explain(Plug.Conn.t()) :: Plug.Conn.t()
  def explain(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, request} <- decode_query_request(params),
         {:ok, indexes} <- Query.list_indexes(uuid),
         {:ok, explanation} <- Query.explain(uuid, request) do
      Response.fragment(
        conn,
        render_console(
          uuid,
          indexes,
          %{explain: public_explain(explanation)},
          nil,
          Request.param(params, "query") || @example_query
        )
      )
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Creates an index from a JSON textarea."
  @spec create_index(Plug.Conn.t()) :: Plug.Conn.t()
  def create_index(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, definition} <- Request.decode_json_field(params, "definition"),
         true <- is_map(definition),
         {:ok, _created} <- Query.create_index(uuid, definition) do
      show(%{conn | path_params: %{"uuid" => uuid}})
    else
      false ->
        Response.error_fragment(
          conn,
          Error.invalid_request("index definition must be a JSON object")
        )

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Deletes an index by id."
  @spec delete_index(Plug.Conn.t()) :: Plug.Conn.t()
  def delete_index(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         index_id when is_binary(index_id) and index_id != "" <-
           conn.path_params["index_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(index_id),
         {:ok, _deleted} <- Query.delete_index(uuid, index_id) do
      show(%{conn | path_params: %{"uuid" => uuid}})
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("index id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Rebuilds an index by id."
  @spec rebuild_index(Plug.Conn.t()) :: Plug.Conn.t()
  def rebuild_index(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         index_id when is_binary(index_id) and index_id != "" <-
           conn.path_params["index_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(index_id),
         {:ok, _rebuilt} <- Query.rebuild_index(uuid, index_id) do
      show(%{conn | path_params: %{"uuid" => uuid}})
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("index id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  defp decode_query_request(params) do
    with {:ok, request} <- Request.decode_json_field(params, "query"),
         true <- is_map(request) do
      case Request.param(params, "bookmark") do
        bookmark when is_binary(bookmark) and bookmark != "" ->
          {:ok, Map.put(request, "bookmark", bookmark)}

        _ ->
          {:ok, request}
      end
    else
      false -> {:error, Error.invalid_request("query must be a JSON object")}
      {:error, _} = error -> error
    end
  end

  defp render_console(uuid, indexes, page, error, query_text) do
    [
      "<section class=\"stack\">\n",
      Components.page_header("Queries", uuid),
      "  <div class=\"panel row\">\n",
      fragment_link("Documents", "/ui/fragments/databases/#{uuid}/documents"),
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      "  </div>\n",
      query_form(uuid, query_text),
      if(error, do: Components.error_block(error), else: []),
      results_panel(uuid, page, query_text),
      indexes_panel(uuid, indexes),
      "</section>\n"
    ]
  end

  defp query_form(uuid, query_text) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Query</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("Prefill is an example only; nothing executes until you submit."),
      "</p>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/queries/execute"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Query JSON",
        [
          " <textarea name=\"query\" rows=\"12\" required spellcheck=\"false\">",
          HTML.textarea(query_text),
          "</textarea>"
        ]
      ),
      "      <div class=\"row\">\n",
      "        <button type=\"submit\">Execute</button>\n",
      "        <button type=\"submit\" class=\"secondary\" formaction=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/queries/explain"),
      "\" formmethod=\"post\">Explain</button>\n",
      "      </div>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp results_panel(_uuid, nil, _query_text), do: []

  defp results_panel(_uuid, %{explain: explanation}, _query_text) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Explain</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(explanation),
      "</pre>\n",
      "  </div>\n"
    ]
  end

  defp results_panel(uuid, page, query_text) when is_map(page) do
    results = MapAccess.get(page, :results) || MapAccess.get(page, :documents) || []
    bookmark = MapAccess.get(page, :bookmark)

    rows =
      Enum.map(results, fn doc ->
        id = MapAccess.get(doc, :id) |> to_string()
        revision = MapAccess.get(doc, :revision) |> to_string()
        body = MapAccess.get(doc, :body)

        [
          HTML.escape(id),
          HTML.escape(revision),
          ["<pre class=\"mono\">", HTML.textarea(body || %{}), "</pre>"]
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Results</h2>\n",
      Components.table(nil, ["ID", "Revision", "Body"], rows),
      pagination_with_query(uuid, bookmark, query_text),
      "  </div>\n"
    ]
  end

  defp pagination_with_query(_uuid, bookmark, _query_text) when not is_binary(bookmark), do: []
  defp pagination_with_query(_uuid, "", _query_text), do: []

  defp pagination_with_query(uuid, bookmark, query_text) do
    [
      "<form class=\"row\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/queries/execute"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "  <input type=\"hidden\" name=\"bookmark\" value=\"",
      HTML.attr(bookmark),
      "\">\n",
      "  <textarea name=\"query\" hidden>",
      HTML.textarea(query_text),
      "</textarea>\n",
      "  <button type=\"submit\">Next page</button>\n",
      "</form>\n"
    ]
  end

  defp indexes_panel(uuid, indexes) do
    public = Enum.map(indexes, &public_index/1)

    rows =
      Enum.map(public, fn index ->
        index_id = MapAccess.get(index, :index_id) |> to_string()
        name = MapAccess.get(index, :name) |> to_string()
        type = MapAccess.get(index, :type) |> to_string()
        state = MapAccess.get(index, :lifecycle_state) |> to_string()

        [
          HTML.escape(name),
          HTML.escape(index_id),
          HTML.escape(type),
          HTML.escape(state),
          index_actions(uuid, index_id)
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Indexes</h2>\n",
      Components.table(nil, ["Name", "ID", "Type", "State", "Actions"], rows),
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/indexes"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Index definition JSON",
        [
          " <textarea name=\"definition\" rows=\"10\" required spellcheck=\"false\">",
          HTML.textarea(@example_index),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Create index</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp index_actions(uuid, index_id) do
    encoded = URI.encode_www_form(index_id)

    [
      "<div class=\"row\">\n",
      "  <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/indexes/#{encoded}/rebuild"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "    <button type=\"submit\" class=\"secondary\">Rebuild</button>\n",
      "  </form>\n",
      "  <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/indexes/#{encoded}/delete"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Delete this index?\">\n",
      "    <button type=\"submit\" class=\"secondary\">Delete</button>\n",
      "  </form>\n",
      "</div>\n"
    ]
  end

  defp public_explain(explanation) when is_map(explanation) do
    explanation
    |> HTML.stringify_keys()
    |> Map.take(@explain_keys)
  end

  defp public_index(index) when is_map(index) do
    index
    |> HTML.stringify_keys()
    |> Map.take(@index_keys)
  end

  defp fragment_link(label, path) do
    [
      "<a href=\"#\" hx-get=\"",
      HTML.attr(path),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">",
      HTML.escape(label),
      "</a>"
    ]
  end
end
