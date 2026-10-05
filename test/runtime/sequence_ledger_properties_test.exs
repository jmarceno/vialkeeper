defmodule VialKeeper.Runtime.SequenceLedgerPropertiesTest do
  @moduledoc """
  Random interleavings of reserve, complete and kill against the sequence
  ledger. The visible watermark never decreases and never reaches an
  outstanding reservation; the data version moves exactly once per committed
  reservation.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias VialKeeper.Runtime.SequenceLedger
  alias VialKeeper.Storage.Memory.Adapter

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-ledger-props")
    {:ok, adapter} = Adapter.create(bundle, %{})
    context = Adapter.to_context(adapter)

    on_exit(fn ->
      _ = Adapter.close(adapter)
      VialKeeper.TempDatabase.cleanup(bundle)
    end)

    {:ok, context: context, uuid: context.identity.database_uuid}
  end

  property "watermark and data version follow every interleaving", %{
    context: context,
    uuid: uuid
  } do
    check all(operations <- list_of(operation(), min_length: 1, max_length: 40), max_runs: 60) do
      ledger = start_supervised!(SequenceLedger.child_spec(uuid))
      assert :ok = SequenceLedger.initialize(uuid, context)

      model = Enum.reduce(operations, initial_model(), &step(&1, &2, uuid, ledger))
      Enum.each(model.outstanding, fn {_first, holder} -> Process.exit(holder, :kill) end)
      :ok = stop_supervised(SequenceLedger.child_spec(uuid).id)
    end
  end

  defp operation do
    one_of([
      tuple({constant(:reserve), integer(1..5)}),
      tuple({constant(:complete), integer(0..10), member_of([:committed, :aborted])}),
      tuple({constant(:kill), integer(0..10)})
    ])
  end

  defp initial_model, do: %{next: 1, outstanding: [], visible: 0, version: 1}

  defp step({:reserve, count}, model, uuid, _ledger) do
    holder = start_holder(uuid, count)
    first = model.next
    last = first + count - 1
    assert_receive {:reserved, ^holder, {:ok, _token, ^first, ^last}}, 5_000

    check(
      %{model | next: last + 1, outstanding: [{first, holder} | model.outstanding]},
      model,
      uuid
    )
  end

  defp step({_finish, _index}, %{outstanding: []} = model, _uuid, _ledger), do: model
  defp step({_finish, _index, _outcome}, %{outstanding: []} = model, _uuid, _ledger), do: model

  defp step({:complete, index, outcome}, model, uuid, _ledger) do
    {{_first, holder} = entry, _rest} = pick(model.outstanding, index)
    send(holder, {:complete, outcome, self()})
    assert_receive {:completed, ^holder}, 5_000

    version = if outcome == :committed, do: model.version + 1, else: model.version
    finish(model, entry, version, uuid)
  end

  defp step({:kill, index}, model, uuid, ledger) do
    {{_first, holder} = entry, _rest} = pick(model.outstanding, index)
    monitor = Process.monitor(holder)
    Process.exit(holder, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^holder, :killed}, 5_000

    VialKeeper.Eventual.eventually(
      fn -> map_size(:sys.get_state(ledger).by_token) == length(model.outstanding) - 1 end,
      timeout: 5_000,
      message: "killed reservation was not aborted"
    )

    finish(model, entry, model.version, uuid)
  end

  defp finish(model, entry, version, uuid) do
    outstanding = List.delete(model.outstanding, entry)
    check(%{model | outstanding: outstanding, version: version}, model, uuid)
  end

  defp check(model, previous, uuid) do
    visible =
      case model.outstanding do
        [] -> model.next - 1
        outstanding -> (outstanding |> Enum.map(&elem(&1, 0)) |> Enum.min()) - 1
      end

    assert {:ok, %{visible: ^visible, data_version: version}} = SequenceLedger.view(uuid)
    assert visible >= previous.visible
    assert Enum.all?(model.outstanding, fn {first, _holder} -> visible < first end)
    assert version == model.version
    assert version >= previous.version

    %{model | visible: visible}
  end

  defp pick(outstanding, index) do
    entry = Enum.at(outstanding, rem(index, length(outstanding)))
    {entry, List.delete(outstanding, entry)}
  end

  defp start_holder(uuid, count) do
    test_pid = self()

    spawn(fn ->
      {:ok, token, _first, _last} = reserved = SequenceLedger.reserve(uuid, count, :infinity)
      send(test_pid, {:reserved, self(), reserved})

      receive do
        {:complete, outcome, reply_to} ->
          :ok = SequenceLedger.complete(uuid, token, outcome)
          send(reply_to, {:completed, self()})
      end
    end)
  end
end
