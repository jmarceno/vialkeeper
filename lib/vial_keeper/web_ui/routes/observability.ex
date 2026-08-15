defmodule VialKeeper.WebUI.Routes.Observability do
  @moduledoc """
  Bounded observability dashboard fragment for the administration console.

  When `:observability_dashboard` is disabled, a safe message is rendered.
  When enabled, only fields from `Observability.Dashboard.snapshot/0` are shown.
  """

  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Dashboard
  alias VialKeeper.WebUI.{Components, HTML, Response}

  @runtime_keys [
    "status",
    "memory_bytes",
    "run_queue",
    "schedulers_online",
    "dirty_cpu_schedulers_online",
    "dirty_io_schedulers",
    "process_count",
    "replication_workers",
    "registered_databases",
    "open_databases"
  ]

  @otel_keys [
    "available",
    "http",
    "commands",
    "changes",
    "replication",
    "checkpoints",
    "database_opens",
    "admission",
    "errors"
  ]

  @doc "Renders the observability fragment."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    body =
      if Application.get_env(:vial_keeper, :observability_dashboard, false) do
        render_snapshot(Dashboard.snapshot())
      else
        render_disabled()
      end

    Response.fragment(conn, body)
  end

  defp render_disabled do
    [
      "<section class=\"stack\">\n",
      Components.page_header("Observability", "Local runtime dashboard."),
      "  <div class=\"panel\">\n",
      "    <p>",
      HTML.escape(
        "The observability dashboard is disabled on this node. Set application env :observability_dashboard to true to view bounded runtime metrics."
      ),
      "</p>\n",
      "  </div>\n",
      "</section>\n"
    ]
  end

  defp render_snapshot(snapshot) when is_map(snapshot) do
    status = MapAccess.get(snapshot, :status) |> to_string()
    sampled = MapAccess.get(snapshot, :sampled_at) |> to_string()
    runtime = take_map(MapAccess.get(snapshot, :runtime), @runtime_keys)
    otel = take_map(MapAccess.get(snapshot, :otel), @otel_keys)
    admission = MapAccess.get(MapAccess.get(snapshot, :runtime), :admission_queues) || []

    [
      "<section class=\"stack\">\n",
      Components.page_header("Observability", "Bounded local dashboard snapshot."),
      "  <div class=\"panel\">\n",
      "    <h2>Runtime</h2>\n",
      "    <p>",
      Components.status_badge(status, if(status == "ok", do: :ok, else: :warn)),
      "</p>\n",
      "    <p class=\"muted\">",
      HTML.escape("Sampled at: #{sampled}"),
      "</p>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(runtime),
      "</pre>\n",
      "  </div>\n",
      "  <div class=\"panel\">\n",
      "    <h2>OpenTelemetry summaries</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(otel),
      "</pre>\n",
      "  </div>\n",
      admission_panel(admission),
      "</section>\n"
    ]
  end

  defp admission_panel(queues) when is_list(queues) do
    rows =
      Enum.map(queues, fn entry ->
        [
          HTML.escape(MapAccess.get(entry, :database_uuid) |> to_string()),
          HTML.escape(MapAccess.get(entry, :active_class) |> to_string()),
          HTML.escape(
            inspect(Map.drop(HTML.stringify_keys(entry), ["database_uuid", "active_class"]))
          )
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Admission queues</h2>\n",
      Components.table(nil, ["Database", "Active class", "Stats"], rows),
      "  </div>\n"
    ]
  end

  defp admission_panel(_), do: []

  defp take_map(map, keys) when is_map(map) do
    map
    |> HTML.stringify_keys()
    |> Map.take(keys)
  end

  defp take_map(_, _), do: %{}
end
