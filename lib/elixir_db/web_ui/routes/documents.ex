defmodule ElixirDB.WebUI.Routes.Documents do
  @moduledoc """
  Document browse, read, and CAS mutation fragments for the console.

  Browsing uses the ordinary `Query` facade with a bounded page size. Write
  controls are hidden for derived databases; crafted mutations remain rejected
  by the application facade.
  """

  alias ElixirDB.Documents
  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Query
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.WebUI.{Components, HTML, Request, Response}

  @doc "Renders a bounded document browse page for a database."
  @spec browse(Plug.Conn.t()) :: Plug.Conn.t()
  def browse(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         {:ok, info} <- DatabaseCatalog.info(uuid),
         request <- browse_request(params),
         result <- Query.execute(uuid, request) do
      case result do
        {:ok, page} ->
          Response.fragment(conn, render_browse(uuid, info, page))

        {:error, %Error{code: :index_required} = error} ->
          Response.fragment(conn, render_index_required(uuid, error))

        {:error, %Error{} = error} ->
          Response.error_fragment(conn, error)
      end
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders the create-document form."
  @spec new(Plug.Conn.t()) :: Plug.Conn.t()
  def new(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, info} <- DatabaseCatalog.info(uuid) do
      Response.fragment(
        conn,
        render_form(uuid, info, nil, %{"example" => true}, nil, writable?(info))
      )
    else
      {:error, %Error{} = error} -> Response.error_fragment(conn, error)
    end
  end

  @doc "Renders a document show/edit form."
  @spec show(Plug.Conn.t()) :: Plug.Conn.t()
  def show(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         id when is_binary(id) and id != "" <- Request.param(params, "id"),
         {:ok, info} <- DatabaseCatalog.info(uuid),
         {:ok, doc} <- Documents.get(uuid, %{"id" => id}) do
      Response.fragment(
        conn,
        render_form(
          uuid,
          info,
          MapAccess.get(doc, :id),
          MapAccess.get(doc, :body, %{}),
          MapAccess.get(doc, :revision),
          writable?(info)
        )
      )
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      id when is_binary(id) ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Puts a document with optional if_revision CAS."
  @spec put(Plug.Conn.t()) :: Plug.Conn.t()
  def put(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         id when is_binary(id) and id != "" <- Request.param(params, "id"),
         {:ok, body} <- Request.decode_json_field(params, "body"),
         true <- is_map(body),
         request <- put_request(id, body, Request.param(params, "if_revision")),
         {:ok, _result} <- Documents.put(uuid, request),
         {:ok, info} <- DatabaseCatalog.info(uuid),
         {:ok, doc} <- Documents.get(uuid, %{"id" => id}) do
      conn
      |> Response.put_hx_trigger(%{"elixirdb:document-saved" => %{}})
      |> Response.fragment(
        render_form(
          uuid,
          info,
          MapAccess.get(doc, :id),
          MapAccess.get(doc, :body, %{}),
          MapAccess.get(doc, :revision),
          writable?(info)
        )
      )
    else
      false ->
        Response.error_fragment(conn, Error.invalid_request("document body must be a JSON object"))

      nil ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      id when is_binary(id) ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      {:error, %Error{code: :revision_conflict} = error} ->
        Response.error_fragment(conn, error)

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  @doc "Deletes a document with optional if_revision CAS."
  @spec delete(Plug.Conn.t()) :: Plug.Conn.t()
  def delete(conn) do
    with {:ok, uuid} <- Request.require_uuid(conn.path_params["uuid"]),
         {:ok, params, conn} <- Request.fetch_params(conn),
         id when is_binary(id) and id != "" <- Request.param(params, "id"),
         request <- delete_request(id, Request.param(params, "if_revision")),
         {:ok, _result} <- Documents.delete(uuid, request),
         {:ok, info} <- DatabaseCatalog.info(uuid),
         {:ok, page} <- Query.execute(uuid, %{"selector" => %{}, "limit" => browse_limit()}) do
      Response.fragment(conn, render_browse(uuid, info, page))
    else
      nil ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      id when is_binary(id) ->
        Response.error_fragment(conn, Error.invalid_request("document id is required"))

      {:error, %Error{code: :index_required} = error} ->
        case Request.require_uuid(conn.path_params["uuid"]) do
          {:ok, uuid} -> Response.fragment(conn, render_index_required(uuid, error))
          {:error, %Error{} = err} -> Response.error_fragment(conn, err)
        end

      {:error, %Error{} = error} ->
        Response.error_fragment(conn, error)
    end
  end

  defp browse_request(params) do
    request = %{"selector" => %{}, "limit" => browse_limit()}

    case Request.param(params, "bookmark") do
      bookmark when is_binary(bookmark) and bookmark != "" ->
        Map.put(request, "bookmark", bookmark)

      _ ->
        request
    end
  end

  defp browse_limit do
    max = ElixirDB.Config.host_limits()[:max_query_results] || 500
    min(50, max)
  end

  defp put_request(id, body, if_revision) do
    request = %{"id" => id, "body" => body}

    case if_revision do
      value when is_binary(value) and value != "" -> Map.put(request, "if_revision", value)
      _ -> request
    end
  end

  defp delete_request(id, if_revision) do
    request = %{"id" => id}

    case if_revision do
      value when is_binary(value) and value != "" -> Map.put(request, "if_revision", value)
      _ -> request
    end
  end

  defp writable?(info) do
    MapAccess.get(info, :database_kind, :ordinary) not in [:derived, "derived"]
  end

  defp render_browse(uuid, info, page) do
    results = MapAccess.get(page, :results) || MapAccess.get(page, :documents) || []
    bookmark = MapAccess.get(page, :bookmark)
    can_write = writable?(info)

    rows =
      Enum.map(results, fn doc ->
        id = MapAccess.get(doc, :id) |> to_string()
        revision = MapAccess.get(doc, :revision) |> to_string()
        preview = body_preview(MapAccess.get(doc, :body))

        [
          document_link(uuid, id),
          HTML.escape(revision),
          ["<pre class=\"mono\">", HTML.escape(preview), "</pre>"]
        ]
      end)

    [
      "<section class=\"stack\">\n",
      Components.page_header("Documents", uuid),
      kind_banner(info),
      "  <div class=\"panel row\">\n",
      if(can_write,
        do: fragment_link("New document", "/ui/fragments/databases/#{uuid}/documents/new"),
        else: []
      ),
      fragment_link("Queries", "/ui/fragments/databases/#{uuid}/queries"),
      fragment_link("Database", "/ui/fragments/databases/#{uuid}"),
      "  </div>\n",
      "  <div class=\"panel\">\n",
      Components.table("Documents", ["ID", "Revision", "Preview"], rows),
      Components.pagination("/ui/fragments/databases/#{uuid}/documents", bookmark),
      "  </div>\n",
      "</section>\n"
    ]
  end

  defp render_index_required(uuid, error) do
    [
      "<section class=\"stack\">\n",
      Components.page_header("Documents", uuid),
      Components.error_block(error),
      "  <div class=\"panel\">\n",
      "    <p>",
      HTML.escape("Browsing requires a compatible index once the scan threshold is reached."),
      "</p>\n",
      fragment_link("Open query and index console", "/ui/fragments/databases/#{uuid}/queries"),
      "  </div>\n",
      "</section>\n"
    ]
  end

  defp render_form(uuid, info, id, body, revision, can_write) do
    [
      "<section class=\"stack\">\n",
      Components.page_header(if(id, do: "Document", else: "New document"), uuid),
      kind_banner(info),
      "  <div class=\"panel\">\n",
      if(can_write,
        do: write_form(uuid, id, body, revision),
        else: read_only_form(id, body, revision)
      ),
      "  </div>\n",
      fragment_link("Back to documents", "/ui/fragments/databases/#{uuid}/documents"),
      "</section>\n"
    ]
  end

  defp write_form(uuid, id, body, revision) do
    [
      "    <form class=\"stack\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/documents/put"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
      Components.field(
        "Document ID",
        [
          " <input name=\"id\" type=\"text\" required autocomplete=\"off\" spellcheck=\"false\" value=\"",
          HTML.attr(id || ""),
          "\"",
          if(id, do: " readonly", else: ""),
          ">"
        ]
      ),
      if(revision,
        do: [
          "      <input type=\"hidden\" name=\"if_revision\" value=\"",
          HTML.attr(revision),
          "\">\n",
          "      <p class=\"muted\">",
          HTML.escape("Current revision: #{revision}"),
          "</p>\n"
        ],
        else: []
      ),
      Components.field(
        "Body JSON",
        [
          " <textarea name=\"body\" rows=\"16\" required spellcheck=\"false\">",
          HTML.textarea(body || %{}),
          "</textarea>"
        ]
      ),
      "      <div class=\"row\">\n",
      "        <button type=\"submit\">Save</button>\n",
      "      </div>\n",
      "    </form>\n",
      if(id && revision, do: delete_form(uuid, id, revision), else: [])
    ]
  end

  defp delete_form(uuid, id, revision) do
    [
      "    <form class=\"stack form-follow\" hx-post=\"",
      HTML.attr("/ui/actions/databases/" <> uuid <> "/documents/delete"),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\" hx-confirm=\"Delete this document?\">\n",
      "      <input type=\"hidden\" name=\"id\" value=\"",
      HTML.attr(id),
      "\">\n",
      "      <input type=\"hidden\" name=\"if_revision\" value=\"",
      HTML.attr(revision),
      "\">\n",
      "      <button type=\"submit\" class=\"secondary\">Delete</button>\n",
      "    </form>\n"
    ]
  end

  defp read_only_form(id, body, revision) do
    [
      "    <p class=\"muted\">",
      HTML.escape("Derived databases are read-only for document mutations."),
      "</p>\n",
      "    <p><strong>",
      HTML.escape(to_string(id || "")),
      "</strong></p>\n",
      if(revision,
        do: ["    <p class=\"muted\">", HTML.escape("Revision: #{revision}"), "</p>\n"],
        else: []
      ),
      "    <pre class=\"mono\">",
      HTML.textarea(body || %{}),
      "</pre>\n"
    ]
  end

  defp kind_banner(info) do
    kind = MapAccess.get(info, :database_kind, :ordinary) |> to_string()

    [
      "  <div class=\"panel\">\n",
      Components.status_badge(kind, if(kind == "derived", do: :warn, else: :ok)),
      "  </div>\n"
    ]
  end

  defp document_link(uuid, id) do
    [
      "<a href=\"#\" hx-get=\"",
      HTML.attr("/ui/fragments/databases/#{uuid}/documents/show?id=" <> URI.encode_www_form(id)),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">",
      HTML.escape(id),
      "</a>"
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

  defp body_preview(nil), do: ""

  defp body_preview(body) do
    encoded = HTML.encode_json(body)

    if byte_size(encoded) > 180 do
      binary_part(encoded, 0, 180) <> "…"
    else
      encoded
    end
  end
end
