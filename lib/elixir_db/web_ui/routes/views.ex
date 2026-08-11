defmodule ElixirDB.WebUI.Routes.Views do
  @moduledoc """
  Local declarative view console fragments.

  Status panels may poll while a view is building. Stable statuses render without
  a polling trigger. Consistent queries rely on `Views.query/3` wait semantics
  rather than a second UI wait loop.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Views
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @example_view ~s({\n  "name": "scores",\n  "key": [{"path": "/kind"}],\n  "value": {"path": "/score"},\n  "reducer": "_sum"\n})

  @example_query ~s({\n  "consistency": "stale_ok",\n  "limit": 50\n})

  @building_statuses MapSet.new(["building", "rebuilding", :building, :rebuilding])

  @doc "Renders the local views list and create form."
  @spec list(Plug.Conn.t()) :: Plug.Conn.t()
  def list(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, views} <- Views.list(uuid) do
      Response.fragment(conn, render_list(uuid, views))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders a view status panel, optionally with HTMX polling while building."
  @spec status(Plug.Conn.t()) :: Plug.Conn.t()
  def status(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         view_id when is_binary(view_id) and view_id != "" <- conn.path_params["view_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(view_id),
         {:ok, state} <- Views.state(uuid, view_id) do
      Response.fragment(conn, render_status(uuid, view_id, state))
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("view id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Creates a local view from a JSON textarea."
  @spec create(Plug.Conn.t()) :: Plug.Conn.t()
  def create(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, definition} <- Request.decode_json_field(params, "definition"),
         true <- is_map(definition),
         {:ok, _created} <- Views.create(uuid, definition) do
      list(%{conn | path_params: %{"uuid" => uuid}})
    else
      false ->
        Response.error_fragment(
          conn,
          Error.invalid_request("view definition must be a JSON object")
        )

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Queries a view using the Views facade consistency modes."
  @spec query(Plug.Conn.t()) :: Plug.Conn.t()
  def query(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         view_id when is_binary(view_id) and view_id != "" <- conn.path_params["view_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(view_id),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, request} <- Request.decode_json_field(params, "query"),
         true <- is_map(request),
         {:ok, result} <- Views.query(uuid, view_id, request),
         {:ok, views} <- Views.list(uuid) do
      Response.fragment(conn, render_list(uuid, views, view_id, result))
    else
      false ->
        Response.error_fragment(conn, Error.invalid_request("view query must be a JSON object"))

      nil ->
        Response.error_fragment(conn, Error.invalid_request("view id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Requests a view rebuild."
  @spec rebuild(Plug.Conn.t()) :: Plug.Conn.t()
  def rebuild(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         view_id when is_binary(view_id) and view_id != "" <- conn.path_params["view_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(view_id),
         :ok <- Views.rebuild(uuid, view_id) do
      status(%{conn | path_params: %{"uuid" => uuid, "view_id" => view_id}})
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("view id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Deletes a local view."
  @spec delete(Plug.Conn.t()) :: Plug.Conn.t()
  def delete(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         view_id when is_binary(view_id) and view_id != "" <- conn.path_params["view_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(view_id),
         {:ok, _deleted} <- Views.delete(uuid, view_id) do
      list(%{conn | path_params: %{"uuid" => uuid}})
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("view id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  defp render_list(uuid, views, queried_view_id \\ nil, query_result \\ nil) do
    rows =
      Enum.map(views, fn view ->
        view_id = MapAccess.get(view, :view_id) |> to_string()
        name = MapAccess.get(view, :name) |> to_string()
        status = MapAccess.get(view, :status) |> to_string()
        indexed = MapAccess.get(view, :indexed_through) |> to_string()

        [
          HTML.escape(name),
          HTML.escape(view_id),
          HTML.escape(status),
          HTML.escape(indexed),
          view_actions(uuid, view_id)
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header("Local views", uuid),
      "  <div class=\"panel row\">\n",
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      fragment_link("Queries", "/ui/fragments/databases/#{uuid}/queries"),
      "  </div>\n",
      "  <div class=\"panel\">\n",
      Components.table(nil, ["Name", "View ID", "Status", "Indexed through", "Actions"], rows),
      "  </div>\n",
      Enum.map(views, fn view ->
        view_id = MapAccess.get(view, :view_id) |> to_string()
        status_embed(uuid, view_id, view)
      end),
      create_form(uuid),
      Enum.map(views, fn view ->
        view_id = MapAccess.get(view, :view_id) |> to_string()
        query_form(uuid, view_id)
      end),
      if(queried_view_id && query_result,
        do: query_result_panel(queried_view_id, query_result),
        else: []
      ),
      "</section>\n"
    ]
  end

  defp status_embed(uuid, view_id, view) do
    state = %{
      view_id: view_id,
      status: MapAccess.get(view, :status),
      indexed_through: MapAccess.get(view, :indexed_through),
      active_generation: MapAccess.get(view, :active_generation),
      building_generation: MapAccess.get(view, :building_generation),
      last_error_code: MapAccess.get(view, :last_error_code)
    }

    [
      "  <div id=\"",
      HTML.attr("view-status-" <> view_id),
      "\">\n",
      render_status(uuid, view_id, state),
      "  </div>\n"
    ]
  end

  defp render_status(uuid, view_id, state) do
    status = MapAccess.get(state, :status)
    polling? = MapSet.member?(@building_statuses, status)

    attrs =
      if polling? do
        [
          " hx-get=\"",
          HTML.attr("/ui/fragments/databases/#{uuid}/views/#{URI.encode_www_form(view_id)}/status"),
          "\" hx-trigger=\"every 2s\" hx-target=\"this\" hx-swap=\"outerHTML\""
        ]
      else
        []
      end

    [
      "<div class=\"panel\"",
      attrs,
      ">\n",
      "  <h2>",
      HTML.escape("View status"),
      "</h2>\n",
      "  <p><strong>",
      HTML.escape(view_id),
      "</strong></p>\n",
      "  <p>",
      Components.status_badge(to_string(status), status_tone(status)),
      "</p>\n",
      "  <p class=\"muted\">",
      HTML.escape("indexed_through=#{MapAccess.get(state, :indexed_through)}"),
      "</p>\n",
      "  <p class=\"muted\">",
      HTML.escape(
        "active=#{MapAccess.get(state, :active_generation)} building=#{MapAccess.get(state, :building_generation)}"
      ),
      "</p>\n",
      case MapAccess.get(state, :last_error_code) do
        nil -> []
        code -> ["  <p class=\"muted\">", HTML.escape("last_error_code=#{code}"), "</p>\n"]
      end,
      "</div>\n"
    ]
  end

  defp create_form(uuid) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Create view</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/views"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Definition JSON",
        [
          " <textarea name=\"definition\" rows=\"12\" required spellcheck=\"false\">",
          HTML.textarea(@example_view),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Create</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp query_form(uuid, view_id) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>",
      HTML.escape("Query #{view_id}"),
      "</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/views/#{URI.encode_www_form(view_id)}/query"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Query JSON",
        [
          " <textarea name=\"query\" rows=\"8\" required spellcheck=\"false\">",
          HTML.textarea(@example_query),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Query view</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp query_result_panel(view_id, result) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>",
      HTML.escape("Results for #{view_id}"),
      "</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(public_result(result)),
      "</pre>\n",
      "  </div>\n"
    ]
  end

  defp view_actions(uuid, view_id) do
    [
      "<div class=\"row\">\n",
      "  <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/views/#{URI.encode_www_form(view_id)}/rebuild"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "    <button type=\"submit\" class=\"secondary\">Rebuild</button>\n",
      "  </form>\n",
      "  <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/views/#{URI.encode_www_form(view_id)}/delete"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Delete this view?\">\n",
      "    <button type=\"submit\" class=\"secondary\">Delete</button>\n",
      "  </form>\n",
      "</div>\n"
    ]
  end

  defp public_result(result) when is_map(result) do
    Map.new(result, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), public_result(value)}
      {key, value} -> {key, public_result(value)}
    end)
  end

  defp public_result(list) when is_list(list), do: Enum.map(list, &public_result/1)
  defp public_result(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp public_result(other), do: other

  defp status_tone(status) when status in ["ready", :ready, "current", :current], do: :ok

  defp status_tone(status) when status in ["building", :building, "rebuilding", :rebuilding],
    do: :warn

  defp status_tone(_), do: :danger

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
