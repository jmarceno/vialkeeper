defmodule ElixirDB.HTTP.ViewsTest do
  @moduledoc "HTTP lifecycle and query tests for declarative views."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer
  alias ElixirDB.TestSupport.ViewBuilderProbe
  alias ElixirDB.View.Manager

  @view %{
    "name" => "scores",
    "key" => [%{"path" => "/kind"}],
    "value" => %{"path" => "/score"},
    "reducer" => "_sum"
  }

  test "covers view lifecycle, strict schemas, logical metadata, and query paging" do
    server = TestServer.start_supervised!()
    path = "http-views-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => uuid}}}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "a", "body" => %{"kind" => "task", "score" => 2}}
             )

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "b", "body" => %{"kind" => "task", "score" => 3}}
             )

    assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views",
               json: Map.put(@view, "unexpected", true)
             )

    assert {:ok, %{status: 201, body: %{"data" => created}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views", json: @view)

    assert is_binary(created["view_id"])
    assert created["name"] == "scores"
    assert is_binary(created["definition_digest"])
    assert created["definition_json"] =~ "\"reducer\":\"_sum\""
    refute Map.has_key?(created, "physical_table")
    refute Map.has_key?(created, "physical_index")

    view_id = created["view_id"]

    assert {:ok, %{status: 409, body: %{"error" => %{"code" => "view_name_conflict"}}}} =
             Req.post(
               server.base_url <> "/v1/databases/#{uuid}/views",
               json: Map.put(@view, "key", [%{"literal" => "other"}])
             )

    await_rows(server, uuid, view_id, 5.0)

    assert {:ok, %{status: 200, body: %{"data" => listed}}} =
             Req.get(server.base_url <> "/v1/databases/#{uuid}/views")

    assert Enum.any?(listed, &(&1["view_id"] == view_id and &1["name"] == "scores"))
    refute IO.iodata_to_binary(JSON.encode_to_iodata!(listed)) =~ "physical"

    assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"limit" => 10, "unexpected" => true}
             )

    assert {:ok, %{status: 200, body: %{"data" => first_page}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{
                 "start_key" => ["task"],
                 "end_key" => ["task"],
                 "group_level" => 1,
                 "limit" => 1
               }
             )

    assert [%{"key" => ["task"], "value" => 5.0}] = first_page["results"]

    assert {:ok, %{status: 201, body: %{"data" => %{"view_id" => rows_view_id}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views",
               json: %{
                 "name" => "rows",
                 "key" => [%{"path" => "/kind"}, %{"path" => "/score"}],
                 "value" => %{"path" => "/score"}
               }
             )

    await_rows(server, uuid, rows_view_id, {2, nil})

    assert {:ok, %{status: 200, body: %{"data" => rows_page}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{rows_view_id}/query",
               json: %{"start_key" => ["task", 2], "end_key" => ["task", 3], "limit" => 1}
             )

    assert [%{"id" => "a", "key" => ["task", 2], "value" => 2}] = rows_page["results"]
    assert is_binary(rows_page["bookmark"])

    assert {:ok, %{status: 200, body: %{"data" => second_page}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{rows_view_id}/query",
               json: %{"bookmark" => rows_page["bookmark"], "limit" => 10}
             )

    assert [%{"id" => "b", "key" => ["task", 3], "value" => 3}] = second_page["results"]

    assert {:ok, %{status: 200, body: %{"data" => %{"accepted" => true, "state" => state}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/rebuild",
               json: %{}
             )

    assert state["view_id"] == view_id

    assert {:ok, %{status: 400, body: %{"error" => %{"code" => "invalid_request"}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/rebuild",
               json: %{"unexpected" => true}
             )

    assert {:ok, %{status: 200, body: %{"data" => %{"deleted" => true}}}} =
             Req.delete(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}")

    assert {:ok, %{status: 404, body: %{"error" => %{"code" => "view_not_found"}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{}
             )
  end

  test "consistency modes distinguish stale, update-after, and caught-up results" do
    server = TestServer.start_supervised!()
    path = "http-views-consistency-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => uuid}}}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "first", "body" => %{"kind" => "task", "score" => 1}}
             )

    assert {:ok, %{status: 201, body: %{"data" => %{"view_id" => view_id}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views", json: @view)

    await_rows(server, uuid, view_id, 1.0)

    assert :ok = Manager.stop_builder(uuid, view_id)

    Eventual.eventually(
      fn -> Manager.builder_pid(uuid, view_id) == :error end,
      timeout: 5_000,
      message: "view builder did not stop"
    )

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "second", "body" => %{"kind" => "task", "score" => 4}}
             )

    assert {:ok, %{status: 200, body: %{"data" => %{"current_sequence" => target}}}} =
             ElixirDB.TestReplicationWire.request(
               :get,
               server.base_url <> "/v1/databases/#{uuid}/replication/identity"
             )

    assert {:ok, %{status: 200, body: %{"data" => %{"results" => stale_results}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"consistency" => "stale_ok", "limit" => 10}
             )

    assert [%{"key" => ["task"], "value" => 1.0}] = stale_results

    assert {:ok, %{status: 200, body: %{"data" => %{"results" => update_after_results}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"consistency" => "update_after", "limit" => 10}
             )

    assert [%{"key" => ["task"], "value" => 1.0}] = update_after_results

    probe = ViewBuilderProbe.install()
    on_exit(fn -> ViewBuilderProbe.uninstall() end)
    assert :ok = Manager.start_builder(uuid, view_id)
    ViewBuilderProbe.await(probe, :before_incremental_apply)
    metadata = ViewBuilderProbe.await(probe, :after_incremental_apply)
    assert metadata.indexed_through >= target

    assert {:ok, %{status: 200, body: %{"data" => %{"results" => consistent_results}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"consistency" => "consistent", "limit" => 10}
             )

    assert [%{"key" => ["task"], "value" => 5.0}] = consistent_results

    assert {:ok, %{status: 404, body: %{"error" => %{"code" => "view_not_found"}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/not-a-view/query",
               json: %{}
             )
  end

  test "consistent query returns view_not_caught_up at the deadline" do
    server = TestServer.start_supervised!()
    path = "http-views-timeout-#{System.unique_integer([:positive])}.elixirdb"

    assert {:ok, %{status: 201, body: %{"data" => %{"database_uuid" => uuid}}}} =
             Req.post(server.base_url <> "/v1/databases", json: %{"path" => path})

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    assert {:ok, %{status: 201}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "first", "body" => %{"kind" => "task", "score" => 1}}
             )

    assert {:ok, %{status: 201, body: %{"data" => %{"view_id" => view_id}}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views", json: @view)

    await_rows(server, uuid, view_id, 1.0)

    assert {:ok, _identity} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config, %{"views" => %{"consistent_wait_ms" => 25}}}
             )

    assert :ok = Manager.stop_builder(uuid, view_id)

    assert {:ok, %{status: 409, body: %{"error" => error}}} =
             Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"consistency" => "consistent", "limit" => 10}
             )

    assert error["code"] == "view_not_caught_up"
    assert error["retryable"] == true
  end

  defp await_rows(server, uuid, view_id, expected) do
    Eventual.eventually(
      fn ->
        case Req.post(server.base_url <> "/v1/databases/#{uuid}/views/#{view_id}/query",
               json: %{"consistency" => "consistent", "limit" => 10}
             ) do
          {:ok, %{status: 200, body: %{"data" => %{"results" => results}}}} ->
            matches_expected?(results, expected)

          _ ->
            false
        end
      end,
      timeout: 5_000,
      message: "view did not become queryable"
    )
  end

  defp matches_expected?([result], expected_value) when is_number(expected_value),
    do: result["value"] == expected_value

  defp matches_expected?(results, {expected_length, nil}),
    do: Enum.count(results) == expected_length

  defp matches_expected?(_results, _expected), do: false
end
