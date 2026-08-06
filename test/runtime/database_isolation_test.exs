defmodule ElixirDB.Runtime.DatabaseIsolationTest do
  @moduledoc """
  Gap D3: independent database isolation.

  Killing one database runtime must not prevent a sibling database from serving reads,
  and must not leak documents or sequences across databases.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    a_rel = "iso-a-#{System.unique_integer([:positive])}.db"
    b_rel = "iso-b-#{System.unique_integer([:positive])}.db"
    root = ElixirDB.Config.database_root()
    a_abs = Path.join(root, a_rel)
    b_abs = Path.join(root, b_rel)

    for path <- [a_abs, b_abs, a_abs <> ".lease", b_abs <> ".lease"] do
      _ = File.rm(path)
    end

    assert {:ok, a} = DatabaseCatalog.create(a_rel)
    assert {:ok, b} = DatabaseCatalog.create(b_rel)

    on_exit(fn ->
      for identity <- [a, b] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
      end

      for path <- [a_abs, b_abs, a_abs <> ".lease", b_abs <> ".lease"] do
        _ = File.rm(path)
      end
    end)

    {:ok, a: a.database_uuid, b: b.database_uuid}
  end

  test "killing one database runtime leaves sibling reads intact", %{a: uuid_a, b: uuid_b} do
    assert {:ok, _} = DatabaseCatalog.open(uuid_a)
    assert {:ok, _} = DatabaseCatalog.open(uuid_b)

    assert {:ok, %{revision: rev_a}} =
             ElixirDB.Documents.put(uuid_a, %{id: "a-doc", body: %{"side" => "a"}})

    assert {:ok, %{revision: rev_b}} =
             ElixirDB.Documents.put(uuid_b, %{id: "b-doc", body: %{"side" => "b"}})

    assert {:ok, identity_b_before} = DatabaseCatalog.command(uuid_b, {:command, :identity, %{}})
    seq_b_before = identity_b_before[:current_sequence] || identity_b_before["current_sequence"]

    assert [{runtime_a, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_a})

    assert [{runtime_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    assert runtime_a != runtime_b
    assert Process.alive?(runtime_a)
    assert Process.alive?(runtime_b)

    ref = Process.monitor(runtime_a)
    Process.exit(runtime_a, :kill)
    assert_receive {:DOWN, ^ref, :process, ^runtime_a, :killed}, 2_000

    assert Process.alive?(runtime_b)

    assert [{^runtime_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    assert {:ok, %{revision: ^rev_b, body: %{"side" => "b"}}} =
             ElixirDB.Documents.get(uuid_b, %{id: "b-doc"})

    # Cross-database isolation: B must never surface A's document.
    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             ElixirDB.Documents.get(uuid_b, %{id: "a-doc"})

    assert {:ok, %{results: b_changes}} = ElixirDB.Changes.read(uuid_b, %{since: 0, limit: 100})

    refute Enum.any?(b_changes, fn change ->
             (change.document_id || change["document_id"]) == "a-doc"
           end)

    assert {:ok, %{revision: _}} =
             ElixirDB.Documents.put(uuid_b, %{
               id: "b-doc",
               if_revision: rev_b,
               body: %{"side" => "b", "after" => true}
             })

    assert {:ok, %{body: %{"side" => "b", "after" => true}}} =
             ElixirDB.Documents.get(uuid_b, %{id: "b-doc"})

    assert {:ok, identity_b_after} = DatabaseCatalog.command(uuid_b, {:command, :identity, %{}})
    seq_b_after = identity_b_after[:current_sequence] || identity_b_after["current_sequence"]
    assert seq_b_after == seq_b_before + 1

    # Victim may be restarted by DynamicSupervisor (:transient + abnormal exit).
    # Assert a deterministic contract: either owner is back and serves A's doc,
    # or the database is closed until an explicit open.
    ElixirDB.Eventual.eventually(
      fn ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid_a}) do
          [{pid, _}] when is_pid(pid) and pid != runtime_a ->
            Process.alive?(pid)

          [] ->
            true

          _ ->
            false
        end
      end,
      timeout: 5_000,
      message: "A owner neither restarted nor cleared after kill"
    )

    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid_a}) do
      [{pid, _}] when is_pid(pid) ->
        assert Process.alive?(pid)

        assert {:ok, %{revision: ^rev_a, body: %{"side" => "a"}}} =
                 ElixirDB.Documents.get(uuid_a, %{id: "a-doc"})

      [] ->
        assert {:error, %ElixirDB.Error{code: code}} =
                 ElixirDB.Documents.get(uuid_a, %{id: "a-doc"})

        assert code in [:database_closed, :database_unavailable, :database_not_registered]

        assert {:ok, _} = DatabaseCatalog.open(uuid_a)

        assert {:ok, %{revision: ^rev_a, body: %{"side" => "a"}}} =
                 ElixirDB.Documents.get(uuid_a, %{id: "a-doc"})
    end

    assert [{still_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    assert still_b == runtime_b
  end
end
