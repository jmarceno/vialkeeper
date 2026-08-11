defmodule ElixirDB.WebUI.Routes.Databases do
  @moduledoc """
  Database list, detail, configuration, and lifecycle fragments for the console.

  Handlers call `DatabaseCatalog`, `Views`, and `JobManager` only. Adapter-private
  fields stay hidden; registration paths are shown for lifecycle operations.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Replication.Wire
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Views
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @doc "Renders the registered database list fragment."
  @spec list(Plug.Conn.t()) :: Plug.Conn.t()
  def list(conn) do
    case DatabaseCatalog.list() do
      {:ok, entries} -> Response.fragment(conn, render_list(entries))
      {:error, error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders a single database detail fragment."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, info} <- DatabaseCatalog.info(uuid),
         {:ok, views} <- Views.list(uuid),
         {:ok, jobs} <- JobManager.list(uuid),
         {:ok, entries} <- DatabaseCatalog.list() do
      path = registration_path(entries, uuid)
      Response.fragment(conn, render_detail(uuid, info, views, jobs, path))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Creates an ordinary database from a form submission."
  @spec create(Plug.Conn.t()) :: Plug.Conn.t()
  def create(conn) do
    with {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, relative} <- create_path(Request.param(params, "path")),
         {:ok, info} <- DatabaseCatalog.create(relative) do
      uuid = MapAccess.get(info, :database_uuid)

      conn
      |> Response.put_hx_trigger(%{"elixirdb:databases-changed" => %{}})
      |> Response.fragment(render_created(uuid, relative))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Replaces database configuration from a JSON textarea."
  @spec update_config(Plug.Conn.t()) :: Plug.Conn.t()
  def update_config(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, config} <- Request.decode_json_field(params, "config"),
         true <- is_map(config),
         {:ok, _updated} <-
           DatabaseCatalog.command(uuid, {:command, :update_config, config}) do
      show(%{conn | path_params: Map.put(conn.path_params, "uuid", uuid)})
    else
      false ->
        Response.error_fragment(
          conn,
          Error.invalid_request("configuration must be a JSON object")
        )

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Closes an open database runtime."
  @spec close(Plug.Conn.t()) :: Plug.Conn.t()
  def close(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         :ok <- DatabaseCatalog.close(uuid) do
      show(%{conn | path_params: Map.put(conn.path_params, "uuid", uuid)})
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Registers an existing bundle path."
  @spec register(Plug.Conn.t()) :: Plug.Conn.t()
  def register(conn) do
    with {:ok, params, conn} <- Request.fetch_params(conn),
         path when is_binary(path) and path != "" <- Request.param(params, "path"),
         {:ok, info} <- DatabaseCatalog.register(path) do
      uuid = MapAccess.get(info, :database_uuid)

      conn
      |> Response.put_hx_trigger(%{"elixirdb:databases-changed" => %{}})
      |> Response.fragment(render_created(uuid, path))
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("path is required"))

      path when is_binary(path) ->
        Response.error_fragment(conn, Error.invalid_request("path is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Unregisters a database UUID from the catalog."
  @spec unregister(Plug.Conn.t()) :: Plug.Conn.t()
  def unregister(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         :ok <- DatabaseCatalog.unregister(uuid) do
      list(conn)
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  defp render_list(entries) do
    rows =
      Enum.map(entries, fn entry ->
        uuid = MapAccess.get(entry, :database_uuid) |> to_string()
        path = MapAccess.get(entry, :path) |> to_string()
        kind = MapAccess.get(entry, :database_kind, :ordinary) |> to_string()
        state = MapAccess.get(entry, :state, :registered) |> to_string()

        [
          fragment_link(uuid, "/ui/fragments/databases/" <> uuid),
          HTML.escape(path),
          Components.status_badge(kind, kind_tone(kind)),
          HTML.escape(state)
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header("Databases", "Registered ordinary and derived databases."),
      "  <div class=\"panel\">\n",
      Components.table("Databases", ["UUID", "Path", "Kind", "State"], rows),
      "  </div>\n",
      create_form(),
      register_form(),
      "</section>\n"
    ]
  end

  defp render_detail(uuid, info, views, jobs, path) do
    kind = MapAccess.get(info, :database_kind, :ordinary) |> to_string()
    public_info = Wire.identity(info)
    config = MapAccess.get(info, :config, %{})
    safe_jobs = Enum.map(jobs, &redact_job/1)

    [
      "<section class=\"stack\">\n",
      Components.page_header("Database", uuid),
      "  <div class=\"panel row\">\n",
      Components.status_badge(kind, kind_tone(kind)),
      "    <span class=\"muted\">",
      HTML.escape(path || ""),
      "</span>\n",
      "  </div>\n",
      "  <div class=\"panel\">\n",
      "    <h2>Identity</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(public_info),
      "</pre>\n",
      "  </div>\n",
      config_form(uuid, config),
      views_summary(uuid, views),
      jobs_summary(safe_jobs),
      lifecycle_actions(uuid),
      related_links(uuid),
      "</section>\n"
    ]
  end

  defp render_created(uuid, path) do
    [
      "<section class=\"stack\">\n",
      Components.page_header("Database ready", "Created or registered successfully."),
      "  <div class=\"panel\">\n",
      "    <p>",
      fragment_link(to_string(uuid), "/ui/fragments/databases/" <> to_string(uuid)),
      "</p>\n",
      "    <p class=\"muted\">",
      HTML.escape(to_string(path)),
      "</p>\n",
      "  </div>\n",
      "</section>\n"
    ]
  end

  defp create_form do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Create database</h2>\n",
      "    <form class=\"stack\" hx-post=\"/ui/actions/databases\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Relative path (optional)",
        [
          " <input name=\"path\" type=\"text\" autocomplete=\"off\" spellcheck=\"false\" placeholder=\"optional.elixirdb\">"
        ]
      ),
      "      <button type=\"submit\">Create</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp register_form do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Register path</h2>\n",
      "    <form class=\"stack\" hx-post=\"/ui/actions/databases/register\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Relative path",
        [" <input name=\"path\" type=\"text\" required autocomplete=\"off\" spellcheck=\"false\">"]
      ),
      "      <button type=\"submit\">Register</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp config_form(uuid, config) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Configuration</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/config"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Config JSON",
        [
          " <textarea name=\"config\" rows=\"16\" spellcheck=\"false\">",
          HTML.textarea(config),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Save configuration</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp views_summary(uuid, views) do
    rows =
      Enum.map(views, fn view ->
        view_id = MapAccess.get(view, :view_id) |> to_string()
        name = MapAccess.get(view, :name) |> to_string()
        status = MapAccess.get(view, :status) |> to_string()

        [HTML.escape(name), HTML.escape(view_id), HTML.escape(status)]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Local views</h2>\n",
      "    <p class=\"row\">\n",
      fragment_link("Open views", "/ui/fragments/databases/#{uuid}/views"),
      "    </p>\n",
      Components.table(nil, ["Name", "View ID", "Status"], rows),
      "  </div>\n"
    ]
  end

  defp jobs_summary(jobs) do
    rows =
      Enum.map(jobs, fn job ->
        [
          HTML.escape(MapAccess.get(job, :job_id) |> to_string()),
          HTML.escape(MapAccess.get(job, :state) |> to_string()),
          HTML.escape(MapAccess.get(job, :enabled) |> to_string())
        ]
      end)

    [
      "  <div class=\"panel\">\n",
      "    <h2>Replication jobs</h2>\n",
      Components.table(nil, ["Job ID", "State", "Enabled"], rows),
      "  </div>\n"
    ]
  end

  defp lifecycle_actions(uuid) do
    [
      "  <div class=\"panel row\">\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/close"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Close this database runtime?\">\n",
      "      <button type=\"submit\" class=\"secondary\">Close</button>\n",
      "    </form>\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/unregister"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Unregister this database?\">\n",
      "      <button type=\"submit\" class=\"secondary\">Unregister</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp related_links(uuid) do
    [
      "  <div class=\"panel row\">\n",
      fragment_link("Documents", "/ui/fragments/databases/#{uuid}/documents"),
      fragment_link("Queries", "/ui/fragments/databases/#{uuid}/queries"),
      fragment_link("Views", "/ui/fragments/databases/#{uuid}/views"),
      fragment_link("Replications", "/ui/fragments/databases/#{uuid}/replications"),
      fragment_link("Maintenance", "/ui/fragments/databases/#{uuid}/maintenance"),
      "  </div>\n"
    ]
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

  defp registration_path(entries, uuid) do
    case Enum.find(entries, &(MapAccess.get(&1, :database_uuid) == uuid)) do
      nil -> nil
      entry -> MapAccess.get(entry, :path)
    end
  end

  defp redact_job(job) when is_map(job) do
    definition =
      job
      |> MapAccess.get(:definition, %{})
      |> HTML.redact_secrets()

    Map.put(job, :definition, definition)
  end

  defp create_path(nil), do: {:ok, "#{ElixirDB.UUID.v4()}.elixirdb"}
  defp create_path(""), do: {:ok, "#{ElixirDB.UUID.v4()}.elixirdb"}
  defp create_path(path) when is_binary(path), do: {:ok, path}

  defp create_path(_),
    do: {:error, Error.invalid_request("database path must be a string")}

  defp kind_tone("derived"), do: :warn
  defp kind_tone(_), do: :ok
end
