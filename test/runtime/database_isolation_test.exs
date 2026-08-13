defmodule ElixirDB.Runtime.DatabaseIsolationTest do
  @moduledoc """
  Independent database runtime isolation: killing one runtime must not prevent
  a sibling from serving reads or leak documents and sequences across databases.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.{AdmissionPolicy, DatabaseCatalog}
  alias ElixirDB.View.Manager

  setup do
    a_rel = "iso-a-#{System.unique_integer([:positive])}.elixirdb"
    b_rel = "iso-b-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    a_abs = Path.join(root, a_rel)
    b_abs = Path.join(root, b_rel)

    for path <- [a_abs, b_abs] do
      ElixirDB.TempDatabase.cleanup(path)
    end

    previous_limits = Application.get_env(:elixir_db, :host_limits)
    previous_policy = Application.get_env(:elixir_db, :admission_policy)

    limits = Keyword.put(previous_limits || [], :admission_limit, 256)

    policy =
      Keyword.merge(
        previous_policy || AdmissionPolicy.default_keyword(),
        foreground_reserved_slots: 0,
        subscription_reserved_slots: 0,
        replication_reserved_slots: 0,
        maintenance_reserved_slots: 0
      )

    Application.put_env(:elixir_db, :host_limits, limits)
    Application.put_env(:elixir_db, :admission_policy, policy)

    assert {:ok, a} = DatabaseCatalog.create(a_rel)
    assert {:ok, b} = DatabaseCatalog.create(b_rel)

    on_exit(fn ->
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      Application.put_env(:elixir_db, :admission_policy, previous_policy)

      for identity <- [a, b] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
      end

      for path <- [a_abs, b_abs] do
        ElixirDB.TempDatabase.cleanup(path)
      end
    end)

    {:ok, a: a.database_uuid, b: b.database_uuid}
  end

  test "killing one database runtime leaves sibling reads intact", %{a: uuid_a, b: uuid_b} do
    assert {:ok, _} = DatabaseCatalog.open(uuid_a)
    assert {:ok, _} = DatabaseCatalog.open(uuid_b)
    assert :ok = Manager.await_resumed(uuid_a)
    assert :ok = Manager.await_resumed(uuid_b)

    assert {:ok, %{revision: rev_a}} =
             ElixirDB.Documents.put(uuid_a, %{id: "a-doc", body: %{"side" => "a"}})

    assert {:ok, %{revision: rev_b}} =
             ElixirDB.Documents.put(uuid_b, %{id: "b-doc", body: %{"side" => "b"}})

    assert {:ok, identity_b_before} = DatabaseCatalog.command(uuid_b, {:command, :identity, %{}})
    seq_b_before = MapAccess.get(identity_b_before, :current_sequence)

    assert [{runtime_a, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_a})

    assert [{runtime_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    owner_a = pids_for({:owner, uuid_a})
    pool_a = pids_for({:read_pool, uuid_a})

    workers_a =
      1..32
      |> Enum.flat_map(&pids_for({:read_worker, uuid_a, &1}))

    runtime_children_a =
      runtime_a
      |> Supervisor.which_children()
      |> Enum.flat_map(fn
        {_id, pid, _type, _modules} when is_pid(pid) -> [pid]
        _child -> []
      end)

    victim_processes = Enum.uniq(owner_a ++ pool_a ++ workers_a ++ runtime_children_a)

    assert runtime_a != runtime_b
    assert Process.alive?(runtime_a)
    assert Process.alive?(runtime_b)

    ref = Process.monitor(runtime_a)
    Process.exit(runtime_a, :kill)
    assert_receive {:DOWN, ^ref, :process, ^runtime_a, :killed}, 2_000

    refute Process.alive?(runtime_a)

    ElixirDB.Eventual.eventually(
      fn ->
        Enum.all?(victim_processes, fn pid -> not Process.alive?(pid) end)
      end,
      timeout: 2_000,
      message: "killed runtime must not leave owner or reader processes behind"
    )

    assert_sibling_admission_ready(uuid_b)

    assert Process.alive?(runtime_b)

    assert [{^runtime_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    assert {:ok, %{revision: ^rev_b, body: %{"side" => "b"}}} =
             retry_admission(
               fn -> ElixirDB.Documents.get(uuid_b, %{id: "b-doc"}) end,
               "get sibling document after runtime kill"
             )

    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             retry_admission(
               fn -> ElixirDB.Documents.get(uuid_b, %{id: "a-doc"}) end,
               "cross-database isolation read after runtime kill"
             )

    assert {:ok, %ElixirDB.Storage.Results.ReadChanges{results: b_changes}} =
             retry_admission(
               fn -> ElixirDB.Changes.read(uuid_b, %{since: 0, limit: 100}) end,
               "changes read on sibling database after runtime kill"
             )

    refute Enum.any?(b_changes, fn change ->
             MapAccess.get(change, :document_id) == "a-doc"
           end)

    assert {:ok, %{revision: _}} =
             retry_admission(
               fn ->
                 ElixirDB.Documents.put(uuid_b, %{
                   id: "b-doc",
                   if_revision: rev_b,
                   body: %{"side" => "b", "after" => true}
                 })
               end,
               "conditional put on sibling database after runtime kill"
             )

    assert {:ok, %{body: %{"side" => "b", "after" => true}}} =
             retry_admission(
               fn -> ElixirDB.Documents.get(uuid_b, %{id: "b-doc"}) end,
               "read updated sibling document after runtime kill"
             )

    assert {:ok, identity_b_after} =
             retry_admission(
               fn -> DatabaseCatalog.command(uuid_b, {:command, :identity, %{}}) end,
               "identity read on sibling database after runtime kill"
             )

    seq_b_after = MapAccess.get(identity_b_after, :current_sequence)
    assert seq_b_after == seq_b_before + 1

    ElixirDB.Eventual.eventually(
      fn ->
        case ElixirDB.Documents.get(uuid_a, %{id: "a-doc"}) do
          {:ok, %{revision: ^rev_a, body: %{"side" => "a"}}} ->
            true

          {:error, %ElixirDB.Error{code: code}}
          when code in [:database_closed, :database_unavailable, :database_not_registered] ->
            case DatabaseCatalog.open(uuid_a) do
              {:ok, _} ->
                match?(
                  {:ok, %{revision: ^rev_a, body: %{"side" => "a"}}},
                  ElixirDB.Documents.get(uuid_a, %{id: "a-doc"})
                )

              _ ->
                false
            end

          _ ->
            false
        end
      end,
      timeout: 5_000,
      message: "A did not recover readable state after runtime kill"
    )

    assert [{still_b, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:runtime, uuid_b})

    assert Process.alive?(still_b)

    assert {:ok, %{body: %{"side" => "b"}}} =
             retry_admission(
               fn -> ElixirDB.Documents.get(uuid_b, %{id: "b-doc"}) end,
               "sibling database remained readable after victim runtime kill"
             )
  end

  defp pids_for(key) do
    Enum.map(Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, key), &elem(&1, 0))
  end

  defp assert_sibling_admission_ready(uuid) do
    ElixirDB.Eventual.eventually(
      fn ->
        case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:admission, uuid}) do
          [{pid, _}] when is_pid(pid) -> Process.alive?(pid)
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "sibling admission did not stabilize after runtime kill"
    )
  end

  defp retry_admission(fun, message) do
    ElixirDB.Eventual.eventually(
      fn ->
        try do
          case fun.() do
            {:ok, _} = ok ->
              {:ok, ok}

            {:error, %ElixirDB.Error{code: :document_not_found}} = error ->
              {:ok, error}

            {:error, %ElixirDB.Error{code: code}}
            when code in [:database_closed, :database_unavailable, :database_not_registered] ->
              false

            other ->
              other
          end
        catch
          :exit, _ -> false
        end
      end,
      timeout: 5_000,
      message: message
    )
  end

  @tag :slow
  test "concurrent puts on one document yield exactly one winner through DatabaseOwner", %{
    a: uuid_a
  } do
    assert {:ok, _} = DatabaseCatalog.open(uuid_a)

    assert {:ok, %{revision: base}} =
             ElixirDB.Documents.put(uuid_a, %{id: "doc", body: %{"n" => 0}})

    n = 200

    results =
      1..n
      |> Enum.map(fn i ->
        Task.async(fn ->
          ElixirDB.Documents.put(uuid_a, %{id: "doc", if_revision: base, body: %{"n" => i}})
        end)
      end)
      |> Task.await_many(30_000)

    {oks, errors} =
      Enum.split_with(results, fn
        {:ok, %{revision: _}} -> true
        _ -> false
      end)

    assert [{:ok, %{revision: winner}}] = oks
    assert Enum.count_until(errors, n) == n - 1

    assert Enum.all?(errors, fn
             {:error, %ElixirDB.Error{code: :revision_conflict}} -> true
             _ -> false
           end)

    assert winner != base

    assert {:ok, %{revision: ^winner, conflicts: []}} =
             ElixirDB.Documents.get(uuid_a, %{id: "doc", include_conflicts: true})
  end
end
