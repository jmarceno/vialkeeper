defmodule VialKeeper.Documents.BulkWriteTransactionTest do
  @moduledoc """
  Public bulk_write must amortize one storage transaction and one search flush
  for the whole batch instead of decomposing into N public puts.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Documents
  alias VialKeeper.Runtime.DatabaseCatalog

  @event [:vial_keeper, :document, :mutation, :phase]

  setup do
    relative = "bulk-write-tx-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  test "one bulk_write batch is one transaction and one search flush", %{uuid: uuid} do
    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        @event,
        &__MODULE__.handle_mutation_phase/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    operations =
      for index <- 1..8 do
        %{"type" => "put", "id" => "doc-#{index}", "body" => %{"n" => index}}
      end

    assert {:ok, results} = Documents.bulk_write(uuid, operations)
    assert match?([_, _, _, _, _, _, _, _], results)
    refute Enum.any?(results, &Map.get(&1, :replayed, false))

    phases = drain_mutation_phases([])
    refute Enum.any?(phases, &(&1.operation == :put))
    assert count_phase(phases, :bulk_write, :transaction_begin) == 1
    assert count_phase(phases, :bulk_write, :transaction_commit) == 1
    assert count_phase(phases, :bulk_write, :search_flush) == 1
  end

  def handle_mutation_phase(_event, _measurements, metadata, pid) do
    send(pid, {:mutation_phase, metadata})
  end

  defp drain_mutation_phases(acc) do
    receive do
      {:mutation_phase, metadata} -> drain_mutation_phases([metadata | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end

  defp count_phase(phases, operation, phase) do
    Enum.count(phases, &(&1.operation == operation and &1.phase == phase))
  end
end
