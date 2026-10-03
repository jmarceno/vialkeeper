defmodule VialKeeper.Query.QueryServiceTest do
  @moduledoc """
  Covers the public query service through the catalog: one database trip per
  query, with the configured limit and bookmark staleness checked in the
  query's own snapshot.
  """
  # Uses VM-global call tracing, so this module does not run async.
  use ExUnit.Case, async: false

  alias VialKeeper.Error
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.Manager

  setup do
    relative = "query-service-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert :ok = Manager.await_resumed(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    for id <- ["a", "b", "c"] do
      assert {:ok, _} = VialKeeper.Documents.put(uuid, %{id: id, body: %{"type" => "task"}})
    end

    {:ok, uuid: uuid}
  end

  test "a query and an explain each make one database trip", %{uuid: uuid} do
    request = %{"selector" => %{"/type" => "task"}, "limit" => 2}

    {result, calls} =
      VialKeeper.CallTrace.run(catalog_entry_points(), fn ->
        VialKeeper.Query.execute(uuid, request)
      end)

    assert {:ok, %{documents: [_, _], has_more: true, bookmark: bookmark}} = result
    assert is_binary(bookmark)
    assert [{DatabaseCatalog, :command, [^uuid, {:command, :query, _}]}] = calls

    {result, calls} =
      VialKeeper.CallTrace.run(catalog_entry_points(), fn ->
        VialKeeper.Query.explain(uuid, request)
      end)

    assert {:ok, _plan} = result
    assert [{DatabaseCatalog, :command, [^uuid, {:command, :explain_query, _}]}] = calls
  end

  test "a bookmark resumes on the same state and is stale after a write", %{uuid: uuid} do
    request = %{"selector" => %{"/type" => "task"}, "limit" => 2}

    assert {:ok, %{documents: first, bookmark: bookmark}} = VialKeeper.Query.execute(uuid, request)

    assert {:ok, %{documents: second, has_more: false, bookmark: nil}} =
             VialKeeper.Query.execute(uuid, Map.put(request, "bookmark", bookmark))

    assert Enum.map(first ++ second, & &1.id) == ["a", "b", "c"]

    assert {:ok, _} = VialKeeper.Documents.put(uuid, %{id: "d", body: %{"type" => "task"}})

    assert {:error, %Error{code: :bookmark_stale}} =
             VialKeeper.Query.execute(uuid, Map.put(request, "bookmark", bookmark))
  end

  test "the database's configured limit bounds queries and explains", %{uuid: uuid} do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config, %{"queries" => %{"max_limit" => 2}}}
             )

    request = %{"selector" => %{"/type" => "task"}, "limit" => 3}

    assert {:error, %Error{code: :resource_limit}} = VialKeeper.Query.execute(uuid, request)
    assert {:error, %Error{code: :resource_limit}} = VialKeeper.Query.explain(uuid, request)

    # The limit check precedes bookmark decoding, as before.
    assert {:error, %Error{code: :resource_limit}} =
             VialKeeper.Query.execute(uuid, Map.put(request, "bookmark", "not-a-bookmark"))

    assert {:ok, %{documents: [_, _]}} =
             VialKeeper.Query.execute(uuid, Map.put(request, "limit", 2))
  end

  defp catalog_entry_points do
    [
      {DatabaseCatalog, :command, 2},
      {DatabaseCatalog, :command, 3},
      {DatabaseCatalog, :command_with_deadline, 3}
    ]
  end
end
