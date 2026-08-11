defmodule ElixirDB.WebUI.Routes.MaterializedViews do
  @moduledoc """
  Materialized federated view console fragments.

  Handlers call `MaterializedViews` for list/get and lifecycle actions. Status
  panels poll while a materialization is transitioning and drop the polling
  trigger once the runtime status is stable.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.MaterializedViews
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @example_definition ~s({\n  "name": "sales",\n  "sources": ["00000000-0000-0000-0000-000000000000"],\n  "map": {\n    "key": [{"path": "/kind"}],\n    "value": {"path": "/amount"}\n  }\n})

  @transitioning MapSet.new([
                   "rebuilding",
                   :rebuilding,
                   "starting",
                   :starting,
                   "catching_up",
                   :catching_up
                 ])

  @doc "Renders the materialized views list and create form."
  @spec list(Plug.Conn.t()) :: Plug.Conn.t()
  def list(conn) do
    case MaterializedViews.list() do
      {:ok, views} -> Response.fragment(conn, render_list(views))
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders one materialized view detail, including source checkpoints."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, view} <- MaterializedViews.get(uuid) do
      Response.fragment(conn, render_detail(view))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders a pollable status panel for a materialized view."
  @spec status(Plug.Conn.t()) :: Plug.Conn.t()
  def status(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, view} <- MaterializedViews.get(uuid) do
      Response.fragment(conn, render_status(view))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Creates a materialized federated view from JSON."
  @spec create(Plug.Conn.t()) :: Plug.Conn.t()
  def create(conn) do
    with {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, definition} <- Request.decode_json_field(params, "definition"),
         true <- is_map(definition),
         {:ok, created} <- MaterializedViews.create(definition) do
      uuid = MapAccess.get(created, :database_uuid) || MapAccess.get(created, :materialization_id)
      show(%{conn | path_params: %{"uuid" => to_string(uuid)}})
    else
      false ->
        Response.error_fragment(
          conn,
          Error.invalid_request("materialized view definition must be a JSON object")
        )

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Enables a materialized view."
  @spec enable(Plug.Conn.t()) :: Plug.Conn.t()
  def enable(conn), do: action(conn, &MaterializedViews.enable/1)

  @doc "Disables a materialized view."
  @spec disable(Plug.Conn.t()) :: Plug.Conn.t()
  def disable(conn), do: action(conn, &MaterializedViews.disable/1)

  @doc "Requests a refresh of a materialized view."
  @spec refresh(Plug.Conn.t()) :: Plug.Conn.t()
  def refresh(conn), do: action(conn, &MaterializedViews.refresh/1)

  @doc "Requests a rebuild of a materialized view."
  @spec rebuild(Plug.Conn.t()) :: Plug.Conn.t()
  def rebuild(conn), do: action(conn, &MaterializedViews.rebuild/1)

  defp action(conn, fun) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, _view} <- fun.(uuid) do
      show(%{conn | path_params: %{"uuid" => uuid}})
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  defp render_list(views) do
    rows =
      Enum.map(views, fn view ->
        uuid = MapAccess.get(view, :database_uuid) |> to_string()
        name = MapAccess.get(view, :name) |> to_string()
        status = MapAccess.get(view, :status) |> to_string()
        enabled = MapAccess.get(view, :enabled) |> to_string()

        [
          fragment_link(name, "/ui/fragments/materialized-views/" <> uuid),
          HTML.escape(uuid),
          Components.status_badge(status, status_tone(status)),
          HTML.escape(enabled)
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header(
        "Materialized views",
        "Derived databases maintained from ordered ordinary sources."
      ),
      "  <div class=\"panel\">\n",
      Components.table(nil, ["Name", "Database UUID", "Status", "Enabled"], rows),
      "  </div>\n",
      create_form(),
      "</section>\n"
    ]
  end

  defp render_detail(view) do
    uuid = MapAccess.get(view, :database_uuid) |> to_string()
    name = MapAccess.get(view, :name) |> to_string()
    kind = MapAccess.get(view, :database_kind) |> to_string()
    enabled = MapAccess.get(view, :enabled)
    runtime = MapAccess.get(view, :runtime_status) || MapAccess.get(view, :status)
    definition = MapAccess.get(view, :definition) || %{}
    sources = MapAccess.get(view, :sources) || []
    reducer = MapAccess.get(definition, :reduce) || MapAccess.get(definition, :reducer)
    group_level = MapAccess.get(definition, :group_level)

    source_rows =
      Enum.map(sources, fn source ->
        state = MapAccess.get(source, :state) |> to_string()

        [
          HTML.escape(MapAccess.get(source, :source_ordinal) |> to_string()),
          HTML.escape(MapAccess.get(source, :source_database_uuid) |> to_string()),
          Components.status_badge(source_state_label(state), source_tone(state)),
          HTML.escape(MapAccess.get(source, :checkpoint_sequence) |> to_string()),
          HTML.escape(MapAccess.get(source, :source_history_epoch) |> to_string())
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header(name, uuid),
      "  <div class=\"panel row\">\n",
      Components.status_badge(kind, :warn),
      Components.status_badge(to_string(runtime), status_tone(runtime)),
      "    <span class=\"muted\">",
      HTML.escape("enabled=#{enabled}"),
      "</span>\n",
      "  </div>\n",
      "  <div id=\"",
      HTML.attr("mv-status-" <> uuid),
      "\">\n",
      render_status(view),
      "  </div>\n",
      "  <div class=\"panel\">\n",
      "    <h2>Definition</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("reducer=#{inspect(reducer)} group_level=#{inspect(group_level)}"),
      "</p>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(definition),
      "</pre>\n",
      "  </div>\n",
      "  <div class=\"panel\">\n",
      "    <h2>Ordered sources</h2>\n",
      Components.table(
        "Source checkpoint vector",
        ["Ordinal", "Source UUID", "State", "Checkpoint", "History epoch"],
        source_rows
      ),
      "  </div>\n",
      actions(uuid, enabled),
      derived_links(uuid),
      "  <p class=\"row\">\n",
      fragment_link("All materialized views", "/ui/fragments/materialized-views"),
      "  </p>\n",
      "</section>\n"
    ]
  end

  defp render_status(view) do
    uuid = MapAccess.get(view, :database_uuid) |> to_string()
    status = MapAccess.get(view, :runtime_status) || MapAccess.get(view, :status)
    status_text = to_string(status)
    polling? = MapSet.member?(@transitioning, status)
    rebuilding? = status_text in ["rebuilding", "starting", "catching_up"]

    attrs =
      if polling? do
        [
          " hx-get=\"",
          HTML.attr("/ui/fragments/materialized-views/#{uuid}/status"),
          "\" hx-trigger=\"every 2s\" hx-swap=\"outerHTML\""
        ]
      else
        []
      end

    [
      "<div class=\"panel\"",
      attrs,
      ">\n",
      "  <h2>",
      HTML.escape("Runtime status"),
      "</h2>\n",
      "  <p>",
      Components.status_badge(status_text, status_tone(status)),
      "</p>\n",
      if rebuilding? do
        [
          "  <p role=\"status\" aria-live=\"polite\"><strong>",
          HTML.escape("Rebuilding — results are transitioning until status becomes current."),
          "</strong></p>\n"
        ]
      else
        []
      end,
      "  <p class=\"muted\">",
      HTML.escape("persistent_state=#{MapAccess.get(view, :persistent_state)}"),
      "</p>\n",
      "</div>\n"
    ]
  end

  defp create_form do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Create materialized view</h2>\n",
      "    <form class=\"stack\" hx-post=\"/ui/actions/materialized-views\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Definition JSON",
        [
          " <textarea name=\"definition\" rows=\"14\" required spellcheck=\"false\">",
          HTML.textarea(@example_definition),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Create</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp actions(uuid, enabled) do
    [
      "  <div class=\"panel row\">\n",
      action_button(uuid, "enable", "Enable", enabled != true),
      action_button(uuid, "disable", "Disable", enabled == true),
      action_button(uuid, "refresh", "Refresh", enabled == true),
      action_button(uuid, "rebuild", "Rebuild", enabled == true, confirm?: true),
      "  </div>\n"
    ]
  end

  defp action_button(uuid, action, label, enabled?, opts \\ []) do
    confirm =
      if Keyword.get(opts, :confirm?, false) do
        " hx-confirm=\"Rebuild this materialized view?\""
      else
        []
      end

    if enabled? do
      [
        "    <form hx-post=\"",
        HTML.attr("/ui/actions/materialized-views/#{uuid}/#{action}"),
        "\" hx-target=\"#app\" hx-swap=\"innerHTML\"",
        confirm,
        ">\n",
        "      <button type=\"submit\"",
        if(action == "rebuild", do: " class=\"secondary\"", else: []),
        ">",
        HTML.escape(label),
        "</button>\n",
        "    </form>\n"
      ]
    else
      []
    end
  end

  defp derived_links(uuid) do
    [
      "  <div class=\"panel row\">\n",
      "    <p class=\"muted\">",
      HTML.escape("Derived database pages are read-only for mutations."),
      "</p>\n",
      fragment_link("Documents", "/ui/fragments/databases/#{uuid}/documents"),
      fragment_link("Queries", "/ui/fragments/databases/#{uuid}/queries"),
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      "  </div>\n"
    ]
  end

  defp source_state_label(state) do
    case to_string(state) do
      "active" -> "current"
      "pending" -> "catching_up"
      other -> other
    end
  end

  defp source_tone(state) do
    case to_string(state) do
      label when label in ["active", "current"] -> :ok
      label when label in ["pending", "catching_up", "rebuilding"] -> :warn
      _ -> :danger
    end
  end

  defp status_tone(status) when status in ["current", :current], do: :ok

  defp status_tone(status)
       when status in [
              "rebuilding",
              :rebuilding,
              "starting",
              :starting,
              "catching_up",
              :catching_up
            ],
       do: :warn

  defp status_tone(status) when status in ["stopped", :stopped, "disabled", :disabled], do: :warn
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
