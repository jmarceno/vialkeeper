defmodule VialKeeper.WebUI.Routes.Maintenance do
  @moduledoc """
  Database maintenance fragments for integrity, compaction, and attachment GC.

  Operations call existing facades. Domain errors and
  admission safety boundaries are preserved; the UI does not bypass them.
  """

  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Maintenance
  alias VialKeeper.WebUI.{Components, HTML, Request, Response}

  @doc "Renders the maintenance console for a database."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    case Request.require_uuid(conn.path_params["uuid"]) do
      {:ok, uuid} -> Response.fragment(conn, render_console(uuid, nil))
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Runs an integrity check through the maintenance facade."
  @spec integrity_check(Plug.Conn.t()) :: Plug.Conn.t()
  def integrity_check(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, data} <- Maintenance.integrity_check(uuid) do
      Response.fragment(conn, render_console(uuid, {:integrity, data}))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Runs compact retention through the maintenance facade."
  @spec compact(Plug.Conn.t()) :: Plug.Conn.t()
  def compact(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, request} <- decode_optional_json(params, "request"),
         {:ok, stats} <- Maintenance.compact(uuid, request) do
      Response.fragment(conn, render_console(uuid, {:compact, stats}))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Runs attachment garbage collection through the maintenance facade."
  @spec attachment_gc(Plug.Conn.t()) :: Plug.Conn.t()
  def attachment_gc(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, stats} <- Maintenance.attachment_gc(uuid) do
      Response.fragment(conn, render_console(uuid, {:attachment_gc, public_stats(stats)}))
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  defp decode_optional_json(params, key) do
    case Request.param(params, key) do
      value when is_binary(value) -> decode_optional_json_text(String.trim(value), key)
      nil -> {:ok, %{}}
      _ -> {:error, Error.invalid_request("#{key} must be a JSON object")}
    end
  end

  defp decode_optional_json_text("", _key), do: {:ok, %{}}

  defp decode_optional_json_text(trimmed, key) do
    case StrictDecoder.decode(trimmed) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, _} -> {:error, Error.invalid_request("#{key} must be a JSON object")}
      {:error, _} = error -> error
    end
  end

  defp render_console(uuid, result) do
    [
      "<section class=\"stack\">\n",
      Components.page_header("Maintenance", uuid),
      "  <div class=\"panel row\">\n",
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      fragment_link("Replications", "/ui/fragments/databases/#{uuid}/replications"),
      "  </div>\n",
      result_panel(result),
      integrity_form(uuid),
      compact_form(uuid),
      attachment_gc_form(uuid),
      lifecycle_forms(uuid),
      "</section>\n"
    ]
  end

  defp result_panel(nil), do: []

  defp result_panel({kind, data}) do
    title =
      case kind do
        :integrity -> "Integrity check result"
        :compact -> "Compact retention result"
        :attachment_gc -> "Attachment GC result"
      end

    [
      "  <div class=\"panel\">\n",
      "    <h2>",
      HTML.escape(title),
      "</h2>\n",
      "    <pre class=\"mono\">",
      HTML.textarea(public_stats(data)),
      "</pre>\n",
      "  </div>\n"
    ]
  end

  defp integrity_form(uuid) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Integrity check</h2>\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/integrity-check"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      "      <button type=\"submit\">Run integrity check</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp compact_form(uuid) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Compact retention</h2>\n",
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/compact"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Request JSON (optional)",
        [
          " <textarea name=\"request\" rows=\"4\" spellcheck=\"false\">",
          HTML.textarea("{}"),
          "</textarea>"
        ]
      ),
      "      <button type=\"submit\">Compact</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp attachment_gc_form(uuid) do
    [
      "  <div class=\"panel\">\n",
      "    <h2>Attachment garbage collection</h2>\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/attachments/gc"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Run attachment GC?\">\n",
      "      <button type=\"submit\" class=\"secondary\">Run attachment GC</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp lifecycle_forms(uuid) do
    [
      "  <div class=\"panel row\">\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/close"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Close this database runtime?\">\n",
      "      <button type=\"submit\" class=\"secondary\">Close</button>\n",
      "    </form>\n",
      "    <form hx-post=\"",
      HTML.attr("/ui/actions/databases/#{uuid}/unregister"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Unregister this database?\">\n",
      "      <button type=\"submit\" class=\"secondary\">Unregister</button>\n",
      "    </form>\n",
      "  </div>\n"
    ]
  end

  defp public_stats(stats) when is_map(stats) do
    Map.new(stats, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), public_value(value)}
      {key, value} -> {to_string(key), public_value(value)}
    end)
  end

  defp public_value(map) when is_map(map) and not is_struct(map), do: public_stats(map)
  defp public_value(list) when is_list(list), do: Enum.map(list, &public_value/1)
  defp public_value(value) when is_boolean(value), do: value
  defp public_value(nil), do: nil
  defp public_value(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp public_value(other), do: other

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
