defmodule ElixirDB.WebUI.ConsoleCoreTest do
  @moduledoc """
  Database, document, query, index, and local-view console proofs for the
  embedded administration UI.
  """
  use ExUnit.Case, async: false

  @moduletag :integration
  import Plug.Test

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.HTTP.Router
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Query
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Views

  @token :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
  @digest String.downcase(:crypto.hash(:sha256, @token) |> Base.encode16(case: :lower))

  setup do
    previous_auth = Application.get_env(:elixir_db, :auth)
    previous_web_ui = Application.get_env(:elixir_db, :web_ui)
    previous_dashboard = Application.get_env(:elixir_db, :observability_dashboard)

    on_exit(fn ->
      Application.put_env(:elixir_db, :auth, previous_auth)
      Application.put_env(:elixir_db, :web_ui, previous_web_ui)
      Application.put_env(:elixir_db, :observability_dashboard, previous_dashboard)
    end)

    Application.put_env(:elixir_db, :web_ui, enabled: true)
    Application.put_env(:elixir_db, :auth, enabled: false, token_digests: [])
    Application.put_env(:elixir_db, :observability_dashboard, false)

    path = "webui-core-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, path: path}
  end

  defp request(method, path, opts \\ []) do
    body = Keyword.get(opts, :body, "")
    headers = Keyword.get(opts, :headers, [])
    token = Keyword.get(opts, :token)

    conn =
      conn(method, path, body)
      |> put_req_headers(headers)
      |> maybe_auth(token)

    Router.call(conn, Router.init([]))
  end

  defp form_post(path, fields, opts \\ []) do
    body = URI.encode_query(fields)

    request("POST", path,
      body: body,
      headers: [
        {"content-type", "application/x-www-form-urlencoded"} | Keyword.get(opts, :headers, [])
      ],
      token: Keyword.get(opts, :token)
    )
  end

  defp put_req_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      Plug.Conn.put_req_header(acc, name, value)
    end)
  end

  defp maybe_auth(conn, nil), do: conn

  defp maybe_auth(conn, token),
    do: Plug.Conn.put_req_header(conn, "authorization", "Bearer " <> token)

  defp enable_auth,
    do: Application.put_env(:elixir_db, :auth, enabled: true, token_digests: [@digest])

  defp get_header(conn, name) do
    conn
    |> Plug.Conn.get_resp_header(name)
    |> List.first()
  end

  test "home lists databases through catalog without secrets", %{uuid: uuid, path: path} do
    home = request("GET", "/ui/fragments/home")
    assert home.status == 200
    assert get_header(home, "cache-control") == "no-store"
    assert home.resp_body =~ uuid
    assert home.resp_body =~ path
    assert home.resp_body =~ "/ui/fragments/databases/" <> uuid
    assert home.resp_body =~ "Replication"
    assert home.resp_body =~ "Tracked workers"
    refute home.resp_body =~ @token
  end

  test "database list, detail, config, and lifecycle use facades", %{uuid: uuid, path: path} do
    list = request("GET", "/ui/fragments/databases")
    assert list.status == 200
    assert list.resp_body =~ uuid
    assert list.resp_body =~ path

    detail = request("GET", "/ui/fragments/databases/#{uuid}")
    assert detail.status == 200
    assert detail.resp_body =~ "Configuration"
    assert detail.resp_body =~ "scan_threshold"
    refute detail.resp_body =~ "auth_token"

    config_json =
      JSON.encode!(%{
        "queries" => %{
          "default_limit" => 25,
          "max_limit" => 500,
          "scan_threshold" => 1_000,
          "max_execution_ms" => 5_000
        }
      })

    updated =
      form_post("/ui/actions/databases/#{uuid}/config", %{"config" => config_json})

    assert updated.status == 200
    assert updated.resp_body =~ "&quot;default_limit&quot;:25"

    closed = form_post("/ui/actions/databases/#{uuid}/close", %{})
    assert closed.status == 200

    created =
      form_post("/ui/actions/databases", %{
        "path" => "webui-created-#{System.unique_integer([:positive])}.elixirdb"
      })

    assert created.status == 200
    assert created.resp_body =~ "Database ready"
    refute get_header(created, "hx-redirect")
    refute get_header(created, "location")
  end

  test "document create, read, CAS update, delete, and conflict display", %{uuid: uuid} do
    created =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{
        "id" => "doc-1",
        "body" => ~s({"type":"note","n":1})
      })

    assert created.status == 200
    assert created.resp_body =~ "doc-1"
    assert created.resp_body =~ "Current revision:"
    refute get_header(created, "hx-redirect")
    trigger = get_header(created, "hx-trigger")
    assert is_binary(trigger)
    refute trigger =~ "secret"
    refute trigger =~ ~s("body")

    show = request("GET", "/ui/fragments/databases/#{uuid}/documents/show?id=doc-1")
    assert show.status == 200
    assert show.resp_body =~ "doc-1"

    assert {:ok, current} = Documents.get(uuid, %{"id" => "doc-1"})
    revision = current.revision

    conflict =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{
        "id" => "doc-1",
        "if_revision" => "1-deadbeef",
        "body" => ~s({"type":"note","n":99})
      })

    assert conflict.status == 409
    assert conflict.resp_body =~ "revision_conflict"

    updated =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{
        "id" => "doc-1",
        "if_revision" => revision,
        "body" => ~s({"type":"note","n":2})
      })

    assert updated.status == 200
    assert updated.resp_body =~ "&quot;n&quot;:2"

    assert {:ok, after_update} = Documents.get(uuid, %{"id" => "doc-1"})

    deleted =
      form_post("/ui/actions/databases/#{uuid}/documents/delete", %{
        "id" => "doc-1",
        "if_revision" => after_update.revision
      })

    assert deleted.status == 200

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Documents.get(uuid, %{"id" => "doc-1"})
  end

  test "derived database hides writes and rejects crafted actions", %{uuid: source_uuid} do
    assert {:ok, identity} =
             MaterializedViews.create(%{
               "name" => "WebUI Derived #{System.unique_integer([:positive])}",
               "sources" => [source_uuid],
               "map" => %{"key" => [%{"path" => "/type"}]}
             })

    derived = identity.database_uuid
    bundle = Path.join(ElixirDB.Config.database_root(), identity.database_path)

    on_exit(fn ->
      _ = DatabaseCatalog.command(derived, {:command, :set_derived_enabled, %{enabled: false}})
      _ = DatabaseCatalog.close(derived)
      _ = DatabaseCatalog.unregister(derived)
      ElixirDB.TempDatabase.cleanup(bundle)
    end)

    browse = request("GET", "/ui/fragments/databases/#{derived}/documents")
    assert browse.status == 200
    assert browse.resp_body =~ "derived"
    refute browse.resp_body =~ "New document"

    form = request("GET", "/ui/fragments/databases/#{derived}/documents/new")
    assert form.status == 200
    assert form.resp_body =~ "read-only"
    refute form.resp_body =~ "hx-post=\"/ui/actions/databases/#{derived}/documents/put\""

    crafted =
      form_post("/ui/actions/databases/#{derived}/documents/put", %{
        "id" => "crafted",
        "body" => ~s({"x":1})
      })

    assert crafted.status == 403
    assert crafted.resp_body =~ "derived_database_read_only"
  end

  test "document browsing honors index_required after lowering scan_threshold", %{uuid: uuid} do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config, %{"queries" => %{"scan_threshold" => 2}}}
             )

    for n <- 1..2 do
      assert {:ok, _} =
               Documents.put(uuid, %{id: "scan-#{n}", body: %{"unindexed" => n}})
    end

    browse = request("GET", "/ui/fragments/databases/#{uuid}/documents")
    assert browse.status == 200
    assert browse.resp_body =~ "index_required"
    assert browse.resp_body =~ "/ui/fragments/databases/#{uuid}/queries"
    refute browse.resp_body =~ "scan-1"
  end

  test "query, explain, pagination, and index CRUD", %{uuid: uuid} do
    for n <- 1..3 do
      assert {:ok, _} =
               Documents.put(uuid, %{id: "q-#{n}", body: %{"type" => "note", "n" => n}})
    end

    query_json = ~s({"selector":{"/type":"note"},"limit":2})

    executed =
      form_post("/ui/actions/databases/#{uuid}/queries/execute", %{"query" => query_json})

    assert executed.status == 200
    assert executed.resp_body =~ "Results"
    assert executed.resp_body =~ "q-"
    assert executed.resp_body =~ "Next page" or executed.resp_body =~ "bookmark"

    explained =
      form_post("/ui/actions/databases/#{uuid}/queries/explain", %{"query" => query_json})

    assert explained.status == 200
    assert explained.resp_body =~ "plan_kind"
    refute explained.resp_body =~ "CREATE INDEX"
    refute explained.resp_body =~ "sqlite"

    created =
      form_post("/ui/actions/databases/#{uuid}/indexes", %{
        "definition" =>
          ~s({"name":"by-type","type":"structured","fields":[{"path":"/type","type":"string","direction":"asc"}]})
      })

    assert created.status == 200
    assert created.resp_body =~ "by-type"

    assert {:ok, indexes} = Query.list_indexes(uuid)
    index_id = hd(indexes)["index_id"]

    rebuilt =
      form_post("/ui/actions/databases/#{uuid}/indexes/#{index_id}/rebuild", %{})

    assert rebuilt.status == 200

    deleted =
      form_post("/ui/actions/databases/#{uuid}/indexes/#{index_id}/delete", %{})

    assert deleted.status == 200
    assert {:ok, []} = Query.list_indexes(uuid)
  end

  test "hostile document ids and bodies are escaped", %{uuid: uuid} do
    hostile_id = "a<img src=x onerror=\"alert(1)\">"
    hostile_body = "{\"note\":\"<script>alert(1)</script>&\"}"

    created =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{
        "id" => hostile_id,
        "body" => hostile_body
      })

    assert created.status == 200
    refute created.resp_body =~ "<script>alert(1)</script>"
    refute created.resp_body =~ "<img src=x onerror=\"alert(1)\">"

    assert String.contains?(created.resp_body, "&lt;script&gt;") or
             String.contains?(created.resp_body, "alert(1)")

    assert String.contains?(created.resp_body, "&lt;img") or
             String.contains?(created.resp_body, "onerror")

    browse = request("GET", "/ui/fragments/databases/#{uuid}/documents")
    assert browse.status == 200
    refute browse.resp_body =~ "<img src=x onerror=\"alert(1)\">"
  end

  test "local view create, list, query, rebuild, and delete", %{uuid: uuid} do
    assert {:ok, _} =
             Documents.put(uuid, %{id: "a", body: %{"kind" => "task", "score" => 2}})

    assert {:ok, _} =
             Documents.put(uuid, %{id: "b", body: %{"kind" => "task", "score" => 3}})

    definition =
      ~s({"name":"scores","key":[{"path":"/kind"}],"value":{"path":"/score"},"reducer":"_sum"})

    created =
      form_post("/ui/actions/databases/#{uuid}/views", %{"definition" => definition})

    assert created.status == 200
    assert created.resp_body =~ "scores"

    assert {:ok, [view | _]} = Views.list(uuid)
    view_id = view["view_id"]

    Eventual.eventually(fn ->
      case Views.state(uuid, view_id) do
        {:ok, state} ->
          status = ElixirDB.MapAccess.get(state, :status)
          status in ["ready", :ready]

        _ ->
          false
      end
    end)

    status = request("GET", "/ui/fragments/databases/#{uuid}/views/#{view_id}/status")
    assert status.status == 200
    refute status.resp_body =~ "every 2s"

    queried =
      form_post("/ui/actions/databases/#{uuid}/views/#{view_id}/query", %{
        "query" => ~s({"consistency":"stale_ok","limit":50})
      })

    assert queried.status == 200
    assert queried.resp_body =~ "Results for"

    rebuilt =
      form_post("/ui/actions/databases/#{uuid}/views/#{view_id}/rebuild", %{})

    assert rebuilt.status == 200

    deleted =
      form_post("/ui/actions/databases/#{uuid}/views/#{view_id}/delete", %{})

    assert deleted.status == 200
    assert {:ok, []} = Views.list(uuid)
  end

  test "fragments are no-store and require auth when enabled", %{uuid: uuid} do
    enable_auth()

    missing = request("GET", "/ui/fragments/databases")
    assert missing.status == 401

    ok = request("GET", "/ui/fragments/databases/#{uuid}", token: @token)
    assert ok.status == 200
    assert get_header(ok, "cache-control") == "no-store"

    action =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{"id" => "x", "body" => "{}"},
        token: @token
      )

    assert action.status in [200, 201, 400]
    refute get_header(action, "hx-redirect")
    refute get_header(action, "location")
  end

  test "actions never put document bodies or tokens into redirect headers", %{uuid: uuid} do
    body = ~s({"secret":"do-not-leak","token":"#{@token}"})

    response =
      form_post("/ui/actions/databases/#{uuid}/documents/put", %{
        "id" => "safe-headers",
        "body" => body
      })

    assert response.status == 200

    for header <- ["hx-redirect", "hx-location", "location", "hx-push-url"] do
      value = get_header(response, header)
      refute value && String.contains?(value, "do-not-leak")
      refute value && String.contains?(value, @token)
    end

    trigger = get_header(response, "hx-trigger") || ""
    refute trigger =~ "do-not-leak"
    refute trigger =~ @token
  end
end
