defmodule VialKeeper.Replication.ChaosEndpointTest do
  @moduledoc """
  Deterministic action and legality checks for the local chaos endpoint.
  """

  use ExUnit.Case, async: false

  alias VialKeeper.ChaosEndpoint
  alias VialKeeper.Documents
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Runtime.DatabaseCatalog

  @actions [:pass, :error, :delay, :duplicate, :reorder]

  setup do
    prefix = "chaos-endpoint-#{System.unique_integer([:positive])}"
    source_path = prefix <> "-source.vialkeeper"
    target_path = prefix <> "-target.vialkeeper"
    root = VialKeeper.Config.database_root()

    for path <- [source_path, target_path] do
      VialKeeper.TempDatabase.cleanup(Path.join(root, path))
    end

    assert {:ok, source_identity} = DatabaseCatalog.create(source_path)
    assert {:ok, target_identity} = DatabaseCatalog.create(target_path)
    assert {:ok, source} = LocalEndpoint.new(source_identity.database_uuid)
    assert {:ok, target} = LocalEndpoint.new(target_identity.database_uuid)

    on_exit(fn ->
      for {identity, path} <- [
            {source_identity, source_path},
            {target_identity, target_path}
          ] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        VialKeeper.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok,
     source_uuid: source_identity.database_uuid,
     target_uuid: target_identity.database_uuid,
     source: source,
     target: target}
  end

  test "pass calls the real endpoint and exposes its seed", %{source: source} do
    endpoint = forced(source, :pass, seed: 27)

    assert {:ok, %{database_uuid: _database_uuid}} = ChaosEndpoint.identity(endpoint)
    assert ChaosEndpoint.seed(endpoint) == 27
    assert ChaosEndpoint.stats(endpoint) == counts(pass: 1)
  end

  test "error is retryable and does not call the real endpoint", %{target: target} do
    endpoint = forced(target, :error)

    assert {:error,
            %VialKeeper.Error{
              code: :database_unavailable,
              message: "chaos injected",
              retryable: true
            }} =
             ChaosEndpoint.put_checkpoint(
               endpoint,
               "error-not-called",
               checkpoint("error-not-called", 1)
             )

    assert {:ok, nil} = LocalEndpoint.get_checkpoint(target, "error-not-called")
    assert ChaosEndpoint.stats(endpoint) == counts(error: 1)
  end

  test "delay waits coarsely before calling the real endpoint", %{source: source} do
    endpoint = forced(source, :delay, delay_ms: 10)
    started_at = System.monotonic_time(:millisecond)

    assert {:ok, _identity} = ChaosEndpoint.identity(endpoint)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert elapsed_ms >= 5
    assert ChaosEndpoint.stats(endpoint) == counts(delay: 1)
  end

  test "duplicate imports twice, returns the second result, and stays idempotent", context do
    %{source: source, target: target, source_uuid: source_uuid, target_uuid: target_uuid} = context
    {request, revisions} = import_request(source, source_uuid, ["duplicate"])
    endpoint = forced(target, :duplicate)

    {result, calls} =
      trace_local_calls({LocalEndpoint, :import_revision_chains, 2}, fn ->
        ChaosEndpoint.import_revision_chains(endpoint, request)
      end)

    assert {:ok, _second_import} = result
    assert [_first_import, _second_import] = calls
    assert ChaosEndpoint.stats(endpoint) == counts(duplicate: 1)
    assert_documents(target_uuid, revisions)
  end

  test "duplicate checkpoint reads execute twice", %{target: target} do
    assert {:ok, _} =
             LocalEndpoint.put_checkpoint(
               target,
               "duplicate-read",
               checkpoint("duplicate-read", 3)
             )

    endpoint = forced(target, :duplicate)

    {result, calls} =
      trace_local_calls({LocalEndpoint, :get_checkpoint, 2}, fn ->
        ChaosEndpoint.get_checkpoint(endpoint, "duplicate-read")
      end)

    assert {:ok, %{value: %{"source_sequence" => 3}}} = result
    assert [_first_read, _second_read] = calls
    assert ChaosEndpoint.stats(endpoint) == counts(duplicate: 1)
  end

  test "reorder reverses the current chain list and converges", context do
    %{source: source, target: target, source_uuid: source_uuid, target_uuid: target_uuid} = context
    {request, revisions} = import_request(source, source_uuid, ["first", "second"])
    endpoint = forced(target, :reorder)

    {result, [[_inner, traced_request]]} =
      trace_local_calls({LocalEndpoint, :import_revision_chains, 2}, fn ->
        ChaosEndpoint.import_revision_chains(endpoint, request)
      end)

    assert {:ok, _imported} = result
    assert traced_request.chains == Enum.reverse(request.chains)
    assert ChaosEndpoint.stats(endpoint) == counts(reorder: 1)
    assert ChaosEndpoint.effective_reorders(endpoint) == 1
    assert_documents(target_uuid, revisions)
  end

  test "checkpoint writes normalize duplicate and reorder to pass", %{target: target} do
    duplicate = forced(target, :duplicate)

    assert {:ok, _} =
             ChaosEndpoint.put_checkpoint(
               duplicate,
               "write-once",
               checkpoint("write-once", 4)
             )

    assert ChaosEndpoint.stats(duplicate) == counts(pass: 1)

    reorder = forced(target, :reorder)

    assert {:ok, _} =
             ChaosEndpoint.put_shadow_checkpoint(
               reorder,
               "shadow-write-once",
               checkpoint("shadow-write-once", 5)
             )

    assert ChaosEndpoint.stats(reorder) == counts(pass: 1)

    assert {:ok, %{value: %{"source_sequence" => 4}}} =
             LocalEndpoint.get_checkpoint(target, "write-once")

    assert {:ok, %{value: %{"source_sequence" => 5}}} =
             LocalEndpoint.get_shadow_checkpoint(target, "shadow-write-once")
  end

  test "default weights produce a reproducible deterministic action distribution", %{
    target: target
  } do
    first = ChaosEndpoint.wrap(target, seed: 2026, delay_ms: 0)
    second = ChaosEndpoint.wrap(target, seed: 2026, delay_ms: 0)
    legal_import = %{chains: []}

    {first_pattern, first_stats} = run_action_sequence(first, legal_import, 250)
    {second_pattern, second_stats} = run_action_sequence(second, legal_import, 250)

    expected = %{pass: 160, error: 33, delay: 28, duplicate: 19, reorder: 10}

    assert first_pattern == second_pattern
    assert first_stats == second_stats
    assert first_stats == expected
    assert Enum.all?(@actions, &(first_stats[&1] > 0))
  end

  defp forced(endpoint, action, opts \\ []) do
    weights = Map.new(@actions, &{&1, if(&1 == action, do: 100, else: 0)})
    ChaosEndpoint.wrap(endpoint, Keyword.merge([seed: 11, weights: weights], opts))
  end

  defp counts(overrides) do
    Map.merge(Map.new(@actions, &{&1, 0}), Map.new(overrides))
  end

  defp checkpoint(replication_id, source_sequence) do
    %{
      "expected_checkpoint_version" => 0,
      "version" => 1,
      "replication_id" => replication_id,
      "checkpoint_version" => 1,
      "session_id" => "chaos-session",
      "source_sequence" => source_sequence,
      "history" => [],
      "source_history_epoch" => "chaos-history-epoch",
      "source_compaction_epoch" => 0,
      "safe_source_sequence" => source_sequence,
      "installed_source_compaction_epoch" => 0
    }
  end

  defp import_request(source, source_uuid, document_ids) do
    revisions =
      Map.new(document_ids, fn document_id ->
        assert {:ok, %{revision: revision}} =
                 Documents.put(source_uuid, %{
                   id: document_id,
                   body: %{"document" => document_id}
                 })

        {document_id, revision}
      end)

    documents =
      Enum.map(revisions, fn {document_id, revision} ->
        %{document_id: document_id, leaf_revisions: [revision]}
      end)

    assert {:ok, %{chains: chains}} =
             LocalEndpoint.get_revision_chains(source, %{documents: documents})

    {%{chains: chains}, revisions}
  end

  defp assert_documents(target_uuid, revisions) do
    Enum.each(revisions, fn {document_id, revision} ->
      assert {:ok, %{revision: ^revision, body: %{"document" => ^document_id}}} =
               Documents.get(target_uuid, %{id: document_id})
    end)
  end

  defp run_action_sequence(endpoint, request, count) do
    Enum.map_reduce(1..count, ChaosEndpoint.stats(endpoint), fn _ordinal, previous_stats ->
      result = ChaosEndpoint.import_revision_chains(endpoint, request)
      current_stats = ChaosEndpoint.stats(endpoint)
      action = single_increment(previous_stats, current_stats)

      {{action, result_kind(result)}, current_stats}
    end)
  end

  defp single_increment(previous_stats, current_stats) do
    increments =
      Map.new(@actions, fn action ->
        {action, current_stats[action] - previous_stats[action]}
      end)

    assert Enum.reduce(increments, 0, fn {_action, increment}, total -> total + increment end) == 1
    assert Enum.all?(increments, fn {_action, increment} -> increment in [0, 1] end)

    Enum.find(@actions, &(increments[&1] == 1))
  end

  defp result_kind({:ok, _result}), do: :ok

  defp result_kind({:error, %VialKeeper.Error{} = error}),
    do: {:error, error.code, error.message, error.retryable}

  defp trace_local_calls({module, function, arity} = mfa, operation) do
    parent = self()

    task =
      Task.async(fn ->
        receive do
          :run_traced_operation -> operation.()
        end
      end)

    :erlang.trace_pattern(mfa, true, [:local])
    :erlang.trace(task.pid, true, [:call, {:tracer, parent}])

    try do
      send(task.pid, :run_traced_operation)
      result = Task.await(task, 5_000)
      {result, collect_calls(task.pid, module, function, arity, [])}
    after
      :erlang.trace_pattern(mfa, false, [:local])
    end
  end

  defp collect_calls(pid, module, function, arity, calls) do
    receive do
      {:trace, ^pid, :call, {^module, ^function, arguments}} ->
        if length(arguments) == arity do
          collect_calls(pid, module, function, arity, [arguments | calls])
        else
          collect_calls(pid, module, function, arity, calls)
        end
    after
      20 -> Enum.reverse(calls)
    end
  end
end
