defmodule ElixirDB.WebUI.Routes.Replications do
  @moduledoc """
  Replication job console fragments for a single database.

  Handlers call `ElixirDB.Replication.JobManager` for list/create and lifecycle
  actions. Stored remote `auth_token` values are never rendered; create and edit
  forms accept a fresh token input that is cleared after submission.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @example_job ~s({\n  "persist": true,\n  "mode": "one_shot",\n  "direction": "push",\n  "enabled": false,\n  "endpoint": {\n    "kind": "local",\n    "database_uuid": "00000000-0000-0000-0000-000000000000"\n  }\n})

  @active_states MapSet.new([
                   "idle",
                   :idle,
                   "handshake",
                   :handshake,
                   "install_boundaries",
                   :install_boundaries,
                   "bootstrap",
                   :bootstrap,
                   "read_changes",
                   :read_changes,
                   "diff",
                   :diff,
                   "transfer",
                   :transfer,
                   "import",
                   :import,
                   "checkpoint_target",
                   :checkpoint_target,
                   "checkpoint_source",
                   :checkpoint_source,
                   "report_peer",
                   :report_peer,
                   "waiting",
                   :waiting,
                   "backoff",
                   :backoff
                 ])

  @doc "Renders replication jobs for a database."
  @spec list(Plug.Conn.t()) :: Plug.Conn.t()
  def list(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, jobs} <- JobManager.list(uuid) do
      Response.fragment(conn, render_list(uuid, jobs))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders a pollable status panel for one replication job."
  @spec status(Plug.Conn.t()) :: Plug.Conn.t()
  def status(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         job_id when is_binary(job_id) and job_id != "" <- conn.path_params["job_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(job_id),
         {:ok, job} <- JobManager.get(uuid, job_id) do
      Response.fragment(conn, render_status(uuid, redact_job(job)))
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("job id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Creates or replaces a replication job definition."
  @spec create(Plug.Conn.t()) :: Plug.Conn.t()
  def create(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, definition} <- decode_definition(params),
         {:ok, _created} <- JobManager.put(uuid, definition) do
      list(%{conn | path_params: %{"uuid" => uuid}})
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Starts a replication job."
  @spec start(Plug.Conn.t()) :: Plug.Conn.t()
  def start(conn), do: mutate(conn, &JobManager.start/2)

  @doc "Cancels a running replication job."
  @spec cancel(Plug.Conn.t()) :: Plug.Conn.t()
  def cancel(conn), do: mutate(conn, &JobManager.cancel/2)

  @doc "Enables a replication job."
  @spec enable(Plug.Conn.t()) :: Plug.Conn.t()
  def enable(conn), do: mutate(conn, &JobManager.enable/2)

  @doc "Disables a replication job."
  @spec disable(Plug.Conn.t()) :: Plug.Conn.t()
  def disable(conn), do: mutate(conn, &JobManager.disable/2)

  @doc "Deletes a replication job."
  @spec delete(Plug.Conn.t()) :: Plug.Conn.t()
  def delete(conn), do: mutate(conn, &JobManager.delete/2)

  defp mutate(conn, fun) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         job_id when is_binary(job_id) and job_id != "" <- conn.path_params["job_id"],
         :ok <- ElixirDB.HTTP.Request.validate_path_id(job_id),
         {:ok, _result} <- fun.(uuid, job_id) do
      list(%{conn | path_params: %{"uuid" => uuid}})
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("job id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  defp decode_definition(params) do
    with {:ok, definition} <- Request.decode_json_field(params, "definition"),
         true <- is_map(definition) do
      {:ok, maybe_merge_auth_token(definition, Request.param(params, "auth_token"))}
    else
      false -> {:error, Error.invalid_request("replication definition must be a JSON object")}
      {:error, _} = error -> error
    end
  end

  defp maybe_merge_auth_token(definition, token)
       when is_binary(token) and token != "" do
    endpoint = MapAccess.get(definition, :endpoint, %{})
    kind = MapAccess.get(endpoint, :kind)

    if kind in ["remote", :remote] do
      endpoint =
        endpoint
        |> HTML.stringify_keys()
        |> Map.put("auth_token", token)

      definition
      |> HTML.stringify_keys()
      |> Map.put("endpoint", endpoint)
    else
      definition
    end
  end

  defp maybe_merge_auth_token(definition, _token), do: definition

  defp render_list(uuid, jobs) do
    safe_jobs = Enum.map(jobs, &redact_job/1)

    rows =
      Enum.map(safe_jobs, fn job ->
        job_id = MapAccess.get(job, :job_id) |> to_string()
        state = MapAccess.get(job, :state) |> to_string()
        enabled = MapAccess.get(job, :enabled) |> to_string()

        [
          HTML.escape(job_id),
          Components.status_badge(state, job_tone(state)),
          HTML.escape(enabled),
          job_actions(uuid, job_id)
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header("Replication jobs", uuid),
      "  <div class=\"panel row\">\n",
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      fragment_link("Maintenance", "/ui/fragments/databases/#{uuid}/maintenance"),
      "  </div>\n",
      "  <div class=\"panel\">\n",
      Components.table(nil, ["Job ID", "State", "Enabled", "Actions"], rows),
      "  </div>\n",
      Enum.map(safe_jobs, fn job ->
        job_id = MapAccess.get(job, :job_id) |> to_string()

        [
          "  <div id=\"",
          HTML.attr("replication-status-" <> job_id),
          "\">\n",
          render_status(uuid, job),
          "  </div>\n",
          endpoint_panel(job),
          edit_form(uuid, job)
        ]
      end),
      create_form(uuid),
      "</section>\n"
    ]
  end

  defp render_status(uuid, job) do
    job_id = MapAccess.get(job, :job_id) |> to_string()
    state = MapAccess.get(job, :state)
    polling? = MapSet.member?(@active_states, state)

    attrs =
      if polling? do
        [
          " hx-get=\"",
          HTML.attr(
            "/ui/fragments/databases/#{uuid}/replications/#{URI.encode_www_form(job_id)}/status"
          ),
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
      HTML.escape("Job status"),
      "</h2>\n",
      "  <p><strong>",
      HTML.escape(job_id),
      "</strong></p>\n",
      "  <p>",
      Components.status_badge(to_string(state), job_tone(state)),
      "</p>\n",
      "</div>\n"
    ]
  end

  defp endpoint_panel(job) do
    definition = MapAccess.get(job, :definition, %{})
    endpoint = MapAccess.get(definition, :endpoint, %{}) |> public_endpoint()

    [
      "  <div class=\"panel\">\n",
      "    <h2>",
      HTML.escape("Endpoint metadata"),
      "</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(endpoint),
      "</pre>\n",
      "  </div>\n"
    ]
  end

  defp create_form(uuid) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Create job</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/replications"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" autocomplete=\"off\">\n",
      Components.field(
        "Definition JSON",
        [
          " <textarea name=\"definition\" rows=\"14\" required spellcheck=\"false\">",
          HTML.textarea(@example_job),
          "</textarea>"
        ]
      ),
      Components.field(
        "Auth token (optional, remote endpoints)",
        [
          " <input type=\"password\" name=\"auth_token\" autocomplete=\"off\" spellcheck=\"false\" value=\"\">"
        ]
      ),
      "      <button type=\"submit\">Create</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp edit_form(uuid, job) do
    job_id = MapAccess.get(job, :job_id) |> to_string()
    definition = MapAccess.get(job, :definition, %{}) |> public_definition_for_edit()

    [
      "  <div class=\"panel\">\n",
      "    <h2>",
      HTML.escape("Edit #{job_id}"),
      "</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/replications"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" autocomplete=\"off\">\n",
      Components.field(
        "Definition JSON",
        [
          " <textarea name=\"definition\" rows=\"14\" required spellcheck=\"false\">",
          HTML.textarea(Map.put(definition, "job_id", job_id)),
          "</textarea>"
        ]
      ),
      Components.field(
        "New auth token (optional; leave blank to keep stored credential)",
        [
          " <input type=\"password\" name=\"auth_token\" autocomplete=\"off\" spellcheck=\"false\" value=\"\">"
        ]
      ),
      "      <button type=\"submit\">Save</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp job_actions(uuid, job_id) do
    encoded = URI.encode_www_form(job_id)

    [
      "<div class=\"row\">\n",
      action_form(uuid, encoded, "start", "Start"),
      action_form(uuid, encoded, "cancel", "Cancel"),
      action_form(uuid, encoded, "enable", "Enable"),
      action_form(uuid, encoded, "disable", "Disable"),
      action_form(uuid, encoded, "delete", "Delete", confirm?: true),
      "</div>\n"
    ]
  end

  defp action_form(uuid, encoded_job_id, action, label, opts \\ []) do
    confirm =
      if Keyword.get(opts, :confirm?, false) do
        " hx-confirm=\"Delete this replication job?\""
      else
        []
      end

    [
      "  <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/replications/#{encoded_job_id}/#{action}"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\"",
      confirm,
      ">\n",
      "    <button type=\"submit\" class=\"secondary\">",
      HTML.escape(label),
      "</button>\n",
      "  </form>\n"
    ]
  end

  defp redact_job(job) when is_map(job) do
    definition =
      job
      |> MapAccess.get(:definition, %{})
      |> HTML.redact_secrets()

    Map.put(job, :definition, definition)
  end

  defp public_endpoint(endpoint) when is_map(endpoint) do
    endpoint
    |> HTML.stringify_keys()
    |> Map.drop(["auth_token"])
  end

  defp public_endpoint(_), do: %{}

  defp public_definition_for_edit(definition) when is_map(definition) do
    definition
    |> HTML.stringify_keys()
    |> Map.update("endpoint", %{}, fn endpoint ->
      endpoint
      |> HTML.stringify_keys()
      |> Map.drop(["auth_token"])
    end)
    |> Map.drop(["auth_token"])
  end

  defp public_definition_for_edit(_), do: %{}

  defp job_tone(state) when state in ["completed", :completed, "disabled", :disabled], do: :ok
  defp job_tone(state) when state in ["failed", :failed], do: :danger
  defp job_tone(_), do: :warn

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
