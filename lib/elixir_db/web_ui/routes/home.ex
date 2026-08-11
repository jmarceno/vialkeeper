defmodule ElixirDB.WebUI.Routes.Home do
  @moduledoc """
  Authenticated home fragment for the embedded administration console.

  The landing surface calls only bounded application facades for registration
  metadata and optional observability. It does not open every database or query
  per-database job managers.
  """

  alias ElixirDB.MapAccess
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Observability.Dashboard
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.WebUI.{Components, HTML, Response}

  @doc """
  Renders the authenticated home fragment.
  """
  @spec call(Plug.Conn.t()) :: Plug.Conn.t()
  def call(conn) do
    databases = list_databases()
    materialized = list_materialized()
    observability = maybe_observability()

    Response.fragment(conn, [
      "<section class=\"stack\" aria-labelledby=\"home-heading\">\n",
      Components.page_header("Console", "Administration surface for this ElixirDB node."),
      "  <div class=\"panel\">\n",
      "    <h2 id=\"home-heading\">Overview</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("Registered databases and bounded runtime summaries."),
      "</p>\n",
      "  </div>\n",
      databases_panel(databases),
      materialized_panel(materialized),
      observability_panel(observability),
      "</section>\n"
    ])
  end

  defp list_databases do
    case DatabaseCatalog.list() do
      {:ok, entries} when is_list(entries) ->
        {:ok, entries}

      {:error, error} ->
        {:error, error}

      other ->
        {:error, ElixirDB.Error.internal_error("unexpected catalog list", %{got: inspect(other)})}
    end
  end

  defp list_materialized do
    case MaterializedViews.list() do
      {:ok, views} when is_list(views) -> {:ok, views}
      {:error, error} -> {:error, error}
    end
  end

  defp maybe_observability do
    if Application.get_env(:elixir_db, :observability_dashboard, false) do
      {:ok, Dashboard.snapshot()}
    else
      :disabled
    end
  end

  defp databases_panel({:ok, entries}) do
    rows =
      Enum.map(entries, fn entry ->
        uuid = MapAccess.get(entry, :database_uuid) |> to_string()
        path = MapAccess.get(entry, :path) |> to_string()
        kind = MapAccess.get(entry, :database_kind, :ordinary) |> to_string()
        state = MapAccess.get(entry, :state, :registered) |> to_string()

        [
          [
            "<a href=\"#\" hx-get=\"",
            HTML.attr("/ui/fragments/databases/" <> uuid),
            "\" hx-target=\"#app\" hx-swap=\"innerHTML\">",
            HTML.escape(uuid),
            "</a>"
          ],
          HTML.escape(path),
          Components.status_badge(kind, kind_tone(kind)),
          HTML.escape(state)
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Databases</h2>\n",
      Components.table("Registered databases", ["UUID", "Path", "Kind", "State"], rows),
      "  </div>\n"
    ]
  end

  defp databases_panel({:error, error}), do: Components.error_block(error)

  defp materialized_panel({:ok, views}) do
    count = length(views)

    rows =
      Enum.map(views, fn view ->
        uuid = MapAccess.get(view, :database_uuid) |> to_string()
        name = MapAccess.get(view, :name) |> to_string()
        status = MapAccess.get(view, :status) |> to_string()

        [
          HTML.escape(name),
          [
            "<a href=\"#\" hx-get=\"",
            HTML.attr("/ui/fragments/databases/" <> uuid),
            "\" hx-target=\"#app\" hx-swap=\"innerHTML\">",
            HTML.escape(uuid),
            "</a>"
          ],
          HTML.escape(status)
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Materialized views</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("#{count} derived database(s)"),
      "</p>\n",
      Components.table(nil, ["Name", "Database", "Status"], rows),
      "  </div>\n"
    ]
  end

  defp materialized_panel({:error, error}), do: Components.error_block(error)

  defp observability_panel(:disabled), do: []

  defp observability_panel({:ok, snapshot}) when is_map(snapshot) do
    status = MapAccess.get(snapshot, :status) |> to_string()
    sampled = MapAccess.get(snapshot, :sampled_at) |> to_string()

    [
      "  <div class=\"panel\">\n",
      "    <h2>Observability</h2>\n",
      "    <p>",
      HTML.escape("Status: #{status}"),
      "</p>\n",
      "    <p class=\"muted\">",
      HTML.escape("Sampled at: #{sampled}"),
      "</p>\n",
      "  </div>\n"
    ]
  end

  defp kind_tone("derived"), do: :warn
  defp kind_tone(_), do: :ok
end
