defmodule ElixirDB.WebUI.ConsoleOpsTest do
  @moduledoc """
  Federation, materialized-view, replication, maintenance, and observability
  proofs for the embedded administration console.
  """
  use ExUnit.Case, async: false
  import Plug.Test

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Federation.Normalizer
  alias ElixirDB.HTTP.Router
  alias ElixirDB.MapAccess
  alias ElixirDB.MaterializedViews
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Runtime.DatabaseCatalog

  @token :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
  @digest String.downcase(:crypto.hash(:sha256, @token) |> Base.encode16(case: :lower))

  setup do
    previous_auth = Application.get_env(:elixir_db, :auth)
    previous_web_ui = Application.get_env(:elixir_db, :web_ui)
    previous_dashboard = Application.get_env(:elixir_db, :observability_dashboard)
    previous_saved = Application.get_env(:elixir_db, :federation_saved_queries, :missing)

    on_exit(fn ->
      Application.put_env(:elixir_db, :auth, previous_auth)
      Application.put_env(:elixir_db, :web_ui, previous_web_ui)
      Application.put_env(:elixir_db, :observability_dashboard, previous_dashboard)

      case previous_saved do
        :missing -> Application.delete_env(:elixir_db, :federation_saved_queries)
        value -> Application.put_env(:elixir_db, :federation_saved_queries, value)
      end
    end)

    Application.put_env(:elixir_db, :web_ui, enabled: true)
    Application.put_env(:elixir_db, :auth, enabled: false, token_digests: [])
    Application.put_env(:elixir_db, :observability_dashboard, false)
    Application.put_env(:elixir_db, :federation_saved_queries, [])

    root = ElixirDB.Config.database_root()
    first_path = "webui-ops-a-#{System.unique_integer([:positive])}.elixirdb"
    second_path = "webui-ops-b-#{System.unique_integer([:positive])}.elixirdb"

    for path <- [first_path, second_path] do
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end

    assert {:ok, first} = DatabaseCatalog.create(first_path)
    assert {:ok, second} = DatabaseCatalog.create(second_path)
    assert {:ok, _} = DatabaseCatalog.open(first.database_uuid)
    assert {:ok, _} = DatabaseCatalog.open(second.database_uuid)

    on_exit(fn ->
      for {identity, path} <- [{first, first_path}, {second, second_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, first_uuid: first.database_uuid, second_uuid: second.database_uuid}
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

  defp seed(uuid, pairs) do
    Enum.each(pairs, fn {id, value} ->
      assert {:ok, _} =
               Documents.put(uuid, %{id: id, body: %{"kind" => "task", "value" => value}})
    end)
  end

  defp cleanup_derived(created) do
    uuid = MapAccess.get(created, :database_uuid)
    path = MapAccess.get(created, :database_path)
    bundle = if is_binary(path), do: Path.join(ElixirDB.Config.database_root(), path), else: nil

    _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)
    if bundle, do: ElixirDB.TempDatabase.cleanup(bundle)
  end

  test "ad-hoc federation renders source vector, order, and bookmark pagination", %{
    first_uuid: first_uuid,
    second_uuid: second_uuid
  } do
    seed(first_uuid, [{"a", 1}, {"c", 3}])
    seed(second_uuid, [{"b", 2}, {"d", 4}])

    query =
      ~s({"selector":{"/kind":"task"},"fields":["/value"],"sort":[{"path":"/value","direction":"asc"}],"limit":2})

    page =
      form_post("/ui/actions/federation/query", %{
        "databases" => "#{first_uuid}\n#{second_uuid}",
        "query" => query
      })

    assert page.status == 200
    assert page.resp_body =~ "Source sequence vector"
    assert page.resp_body =~ first_uuid
    assert page.resp_body =~ second_uuid
    first_pos = :binary.match(page.resp_body, first_uuid) |> elem(0)
    second_pos = :binary.match(page.resp_body, second_uuid) |> elem(0)
    assert first_pos < second_pos
    assert page.resp_body =~ "a"
    assert page.resp_body =~ "b"
    assert page.resp_body =~ "name=\"bookmark\""
    assert page.resp_body =~ "Next page"
    refute page.resp_body =~ "hx-push-url"
    refute get_header(page, "location")

    assert [_, bookmark] = Regex.run(~r/name="bookmark" value="([^"]+)"/, page.resp_body)

    next_page =
      form_post("/ui/actions/federation/query", %{
        "databases" => "#{first_uuid}\n#{second_uuid}",
        "query" => query,
        "bookmark" => bookmark
      })

    assert next_page.status == 200
    assert next_page.resp_body =~ "c"
    assert next_page.resp_body =~ "d"
  end

  test "saved federation queries list and execute without a UI mutation path", %{
    first_uuid: first_uuid
  } do
    seed(first_uuid, [{"open", 1}])

    assert {:ok, normalized} =
             Normalizer.normalize(%{
               databases: [first_uuid],
               query: %{
                 selector: %{"/kind" => "task"},
                 fields: ["/value"],
                 sort: [%{path: "/value", direction: "asc"}],
                 limit: 5
               }
             })

    Application.put_env(:elixir_db, :federation_saved_queries, [
      %{
        name: "open-tasks",
        databases: normalized.databases,
        query: normalized.query,
        fingerprint: normalized.fingerprint
      }
    ])

    listed = request("GET", "/ui/fragments/federation")
    assert listed.status == 200
    assert listed.resp_body =~ "open-tasks"
    assert listed.resp_body =~ "host.toml"
    refute listed.resp_body =~ "/ui/actions/federation/saved-queries/create"
    refute listed.resp_body =~ "/ui/actions/federation/saved-queries/delete"
    refute listed.resp_body =~ "Create saved query"
    assert request("POST", "/ui/actions/federation/saved-queries/create").status == 400

    executed =
      form_post("/ui/actions/federation/saved-queries/execute", %{"name" => "open-tasks"})

    assert executed.status == 200
    assert executed.resp_body =~ "open"
    assert executed.resp_body =~ "Source sequence vector"
  end

  test "materialized create, status, lifecycle, and derived database links", %{
    first_uuid: source_uuid
  } do
    assert {:ok, _} =
             Documents.put(source_uuid, %{id: "one", body: %{"kind" => "sale", "amount" => 7}})

    name = "webui-sales-#{System.unique_integer([:positive])}"

    definition =
      JSON.encode!(%{
        "name" => name,
        "sources" => [source_uuid],
        "map" => %{
          "key" => [%{"path" => "/kind"}],
          "value" => %{"path" => "/amount"}
        }
      })

    created = form_post("/ui/actions/materialized-views", %{"definition" => definition})
    assert created.status == 200
    assert created.resp_body =~ "derived"
    assert created.resp_body =~ "/ui/fragments/databases/"
    assert created.resp_body =~ "/documents"
    assert created.resp_body =~ "/queries"

    assert {:ok, views} = MaterializedViews.list()

    view =
      Enum.find(views, fn entry ->
        MapAccess.get(entry, :name) == name
      end)

    assert view
    uuid = MapAccess.get(view, :database_uuid)
    on_exit(fn -> cleanup_derived(view) end)

    detail = request("GET", "/ui/fragments/materialized-views/#{uuid}")
    assert detail.status == 200
    assert detail.resp_body =~ uuid
    assert detail.resp_body =~ source_uuid
    assert detail.resp_body =~ "Ordered sources"
    assert detail.resp_body =~ "/ui/fragments/databases/#{uuid}"

    disabled = form_post("/ui/actions/materialized-views/#{uuid}/disable", %{})
    assert disabled.status == 200

    assert form_post("/ui/actions/materialized-views/#{uuid}/refresh", %{}).status in [200, 400]

    enabled = form_post("/ui/actions/materialized-views/#{uuid}/enable", %{})
    assert enabled.status == 200

    refreshed = form_post("/ui/actions/materialized-views/#{uuid}/refresh", %{})
    assert refreshed.status == 200

    rebuilt = form_post("/ui/actions/materialized-views/#{uuid}/rebuild", %{})
    assert rebuilt.status == 200
  end

  test "materialized rebuilding status polls and stops when stable", %{first_uuid: source_uuid} do
    assert {:ok, created} =
             MaterializedViews.create(%{
               "name" => "WebUI Poll #{System.unique_integer([:positive])}",
               "sources" => [source_uuid],
               "map" => %{"key" => [%{"path" => "/kind"}]}
             })

    derived = MapAccess.get(created, :database_uuid)
    on_exit(fn -> cleanup_derived(created) end)

    transitioning =
      Eventual.eventually(
        fn ->
          status = request("GET", "/ui/fragments/materialized-views/#{derived}/status")

          if status.status == 200 and String.contains?(status.resp_body, "every 2s") do
            status
          else
            case MaterializedViews.get(derived) do
              {:ok, view} ->
                runtime =
                  to_string(MapAccess.get(view, :runtime_status) || MapAccess.get(view, :status))

                if runtime == "current", do: _ = MaterializedViews.rebuild(derived)
                false

              _ ->
                false
            end
          end
        end,
        timeout: 10_000,
        message: "expected rebuilding status to poll"
      )

    assert transitioning.resp_body =~ "Rebuilding"
    assert transitioning.resp_body =~ "every 2s"
    assert transitioning.resp_body =~ ~s(hx-target="this")
    refute transitioning.resp_body =~ "hx-ws"
    refute transitioning.resp_body =~ "EventSource"

    Eventual.eventually(
      fn ->
        case MaterializedViews.get(derived) do
          {:ok, view} ->
            to_string(MapAccess.get(view, :runtime_status) || MapAccess.get(view, :status)) ==
              "current"

          _ ->
            false
        end
      end,
      timeout: 10_000,
      message: "materialized view did not stabilize"
    )

    stable = request("GET", "/ui/fragments/materialized-views/#{derived}/status")
    assert stable.status == 200
    refute stable.resp_body =~ "every 2s"
    assert stable.resp_body =~ "current"
  end

  test "replication job UI never renders raw auth tokens", %{
    first_uuid: first_uuid,
    second_uuid: second_uuid
  } do
    secret = "super-secret-token-#{System.unique_integer([:positive])}"

    definition =
      JSON.encode!(%{
        "persist" => true,
        "mode" => "one_shot",
        "direction" => "push",
        "enabled" => false,
        "endpoint" => %{
          "kind" => "remote",
          "database_uuid" => second_uuid,
          "base_url" => "http://127.0.0.1:9"
        }
      })

    created =
      form_post("/ui/actions/databases/#{first_uuid}/replications", %{
        "definition" => definition,
        "auth_token" => secret
      })

    assert created.status == 200
    refute created.resp_body =~ secret
    assert created.resp_body =~ "[redacted]" or created.resp_body =~ "base_url"
    refute created.resp_body =~ ~s(value="#{secret}")

    listed = request("GET", "/ui/fragments/databases/#{first_uuid}/replications")
    assert listed.status == 200
    refute listed.resp_body =~ secret

    assert {:ok, [job | _]} = JobManager.list(first_uuid)

    status =
      request(
        "GET",
        "/ui/fragments/databases/#{first_uuid}/replications/#{job.job_id}/status"
      )

    assert status.status == 200
    refute status.resp_body =~ secret
    refute status.resp_body =~ "every 2s"

    edit_definition =
      JSON.encode!(%{
        "job_id" => job.job_id,
        "persist" => true,
        "mode" => "one_shot",
        "direction" => "push",
        "enabled" => false,
        "endpoint" => %{
          "kind" => "remote",
          "database_uuid" => second_uuid,
          "base_url" => "http://127.0.0.1:9"
        }
      })

    saved =
      form_post("/ui/actions/databases/#{first_uuid}/replications", %{
        "definition" => edit_definition,
        "auth_token" => ""
      })

    assert saved.status == 200
    refute saved.resp_body =~ secret
    assert saved.resp_body =~ job.job_id
    refute saved.resp_body =~ "error-block"

    assert {:ok, updated} = JobManager.get(first_uuid, job.job_id)
    stored = get_in(updated.definition, ["endpoint", "auth_token"])
    assert stored == secret
  end

  test "maintenance preserves domain errors and runs integrity through facades", %{
    first_uuid: uuid
  } do
    page = request("GET", "/ui/fragments/databases/#{uuid}/maintenance")
    assert page.status == 200
    assert page.resp_body =~ "Integrity check"
    assert page.resp_body =~ "Compact retention"
    assert page.resp_body =~ "Attachment garbage collection"

    integrity = form_post("/ui/actions/databases/#{uuid}/integrity-check", %{})
    assert integrity.status == 200
    assert integrity.resp_body =~ "Integrity check result"

    compact = form_post("/ui/actions/databases/#{uuid}/compact", %{"request" => "{}"})
    assert compact.status == 200

    bad = form_post("/ui/actions/databases/#{uuid}/compact", %{"request" => "{not-json"})
    assert bad.status == 400
    assert bad.resp_body =~ "error-block"
    refute get_header(bad, "hx-redirect")

    missing =
      form_post(
        "/ui/actions/databases/00000000-0000-0000-0000-000000000099/integrity-check",
        %{}
      )

    assert missing.status in [200, 400, 404, 409, 503]

    assert missing.resp_body =~ "error-block" or missing.resp_body =~ "not registered" or
             missing.resp_body =~ "database"

    gc = form_post("/ui/actions/databases/#{uuid}/attachments/gc", %{})
    assert gc.status == 200
    assert gc.resp_body =~ "Attachment GC result"
  end

  test "observability disabled and enabled behavior" do
    disabled = request("GET", "/ui/fragments/observability")
    assert disabled.status == 200
    assert disabled.resp_body =~ "disabled"
    refute disabled.resp_body =~ "memory_bytes"

    Application.put_env(:elixir_db, :observability_dashboard, true)
    enabled = request("GET", "/ui/fragments/observability")
    assert enabled.status == 200
    assert enabled.resp_body =~ "Runtime"
    assert enabled.resp_body =~ "memory_bytes" or enabled.resp_body =~ "registered_databases"
    refute enabled.resp_body =~ "mailbox"
  end

  test "polling is bounded and state-bearing ops routes require bearer", %{
    first_uuid: first_uuid,
    second_uuid: second_uuid
  } do
    assert {:ok, created} =
             MaterializedViews.create(%{
               "name" => "WebUI Auth #{System.unique_integer([:positive])}",
               "sources" => [first_uuid],
               "map" => %{"key" => [%{"path" => "/kind"}]}
             })

    derived = MapAccess.get(created, :database_uuid)
    on_exit(fn -> cleanup_derived(created) end)

    Eventual.eventually(fn ->
      case MaterializedViews.get(derived) do
        {:ok, view} ->
          to_string(MapAccess.get(view, :runtime_status) || MapAccess.get(view, :status)) ==
            "current"

        _ ->
          false
      end
    end)

    stable = request("GET", "/ui/fragments/materialized-views/#{derived}/status")
    assert stable.status == 200
    refute stable.resp_body =~ "every 2s"

    assert {:ok, %{job_id: job_id}} =
             JobManager.put(first_uuid, %{
               "persist" => true,
               "mode" => "one_shot",
               "direction" => "push",
               "enabled" => false,
               "endpoint" => %{"kind" => "local", "database_uuid" => second_uuid}
             })

    job_status =
      request("GET", "/ui/fragments/databases/#{first_uuid}/replications/#{job_id}/status")

    assert job_status.status == 200
    refute job_status.resp_body =~ "every 2s"

    enable_auth()

    for path <- [
          "/ui/fragments/federation",
          "/ui/fragments/materialized-views",
          "/ui/fragments/observability",
          "/ui/fragments/databases/#{first_uuid}/replications",
          "/ui/fragments/databases/#{first_uuid}/maintenance",
          "/ui/fragments/materialized-views/#{derived}/status"
        ] do
      assert request("GET", path).status == 401
      ok = request("GET", path, token: @token)
      assert ok.status == 200
      assert get_header(ok, "cache-control") == "no-store"
    end

    assert form_post("/ui/actions/federation/query", %{
             "databases" => first_uuid,
             "query" => ~s({"limit":1})
           }).status == 401
  end
end
