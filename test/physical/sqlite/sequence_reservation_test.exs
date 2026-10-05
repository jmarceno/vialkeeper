defmodule VialKeeper.StorageAdapter.SequenceReservationTest do
  @moduledoc """
  The SQLite reserved-through value: its initial value, its bound over every
  stored sequence, max-only persistence, and no sequence reuse after the
  process holding a ledger is SIGKILLed.
  """
  use ExUnit.Case, async: false

  @moduletag :sqlite_physical

  alias VialKeeper.Runtime.SequenceLedger
  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.{Adapter, ChangeLog, Connection}

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-reservation")
    path = VialKeeper.TempDatabase.sqlite_path(bundle)
    on_exit(fn -> VialKeeper.TempDatabase.cleanup(bundle) end)
    {:ok, path: path}
  end

  test "a new bundle reserves nothing and writes never pass the reserved value", %{path: path} do
    {:ok, adapter} = Adapter.create(path, %{})
    context = Adapter.to_context(adapter)

    try do
      assert {:ok, 0} = ChangeLog.sequence_high_water(context)

      assert {:ok, %{revision: rev}} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "a",
                 body: %{"n" => 1}
               })

      assert {:ok, _} =
               Adapter.apply_bulk_mutation(adapter, %{
                 operations: [
                   %{operation: :put, document_id: "a", if_revision: rev, body: %{"n" => 2}},
                   %{operation: :put, document_id: "b", body: %{"n" => 1}}
                 ]
               })

      assert {:ok, high_water} = ChangeLog.sequence_high_water(context)
      assert high_water >= max_stored_sequence(path)
      assert max_stored_sequence(path) == 3
    after
      Adapter.close(adapter)
    end
  end

  test "persisting a reservation only ever raises it", %{path: path} do
    {:ok, adapter} = Adapter.create(path, %{})
    context = Adapter.to_context(adapter)

    try do
      assert :ok = ChangeLog.persist_sequence_reservation(context, 10)
      assert :ok = ChangeLog.persist_sequence_reservation(context, 5)
      assert reserved_through(path) == 10
      assert {:ok, 10} = ChangeLog.sequence_high_water(context)
    after
      Adapter.close(adapter)
    end
  end

  @tag :slow
  @tag timeout: 120_000
  test "a restart after SIGKILL never reuses a handed-out sequence", %{path: path} do
    {:ok, adapter} = Adapter.create(path, %{})
    :ok = Adapter.close(adapter)

    holder = start_holder!(path)
    on_exit(fn -> kill(holder.pid) end)

    kill(holder.pid)
    VialKeeper.Eventual.eventually(fn -> not alive?(holder.pid) end, timeout: 15_000)

    {:ok, adapter} = Adapter.open(path)
    context = Adapter.to_context(adapter)
    uuid = context.identity.database_uuid
    {:ok, ledger} = SequenceLedger.start_link(uuid)

    try do
      assert :ok = SequenceLedger.initialize(uuid, context)
      assert reserved_through(path) >= holder.last_reserved
      assert max_stored_sequence(path) == holder.visible
      assert SequenceLedger.visible(uuid) >= holder.last_reserved

      assert {{:ok, %{sequence: sequence}}, max_used} =
               SequenceLedger.with_reservation(uuid, 1, :infinity, fn ->
                 Services.apply_local_mutation(context, %{
                   operation: :put,
                   document_id: "after-crash",
                   body: %{"n" => 1}
                 })
               end)

      assert max_used == sequence
      assert sequence > holder.last_reserved
      assert sequence > holder.visible
    after
      GenServer.stop(ledger)
      Adapter.close(adapter)
    end
  end

  # A child OS process opens the bundle, commits five writes through a ledger,
  # then holds a reservation it never finishes and waits to be killed.
  defp start_holder!(path) do
    ready = path <> ".ready"
    _ = File.rm(ready)

    script = """
    alias VialKeeper.Runtime.SequenceLedger
    alias VialKeeper.Storage.Services
    alias VialKeeper.Storage.SQLite.Adapter
    {:ok, _} = Application.ensure_all_started(:vial_keeper)
    {:ok, adapter} = Adapter.open(#{inspect(path)})
    context = Adapter.to_context(adapter)
    uuid = context.identity.database_uuid
    {:ok, _} = SequenceLedger.start_link(uuid)
    :ok = SequenceLedger.initialize(uuid, context)

    for n <- 1..5 do
      {{:ok, _}, _} =
        SequenceLedger.with_reservation(uuid, 1, :infinity, fn ->
          Services.apply_local_mutation(context, %{
            operation: :put,
            document_id: "doc-\#{n}",
            body: %{"n" => n}
          })
        end)
    end

    visible = SequenceLedger.visible(uuid)
    {:ok, _token, _first, last} = SequenceLedger.reserve(uuid, 3, :infinity)
    File.write!(#{inspect(ready)}, "\#{System.pid()} \#{visible} \#{last}")
    Process.sleep(:infinity)
    """

    mix = System.find_executable("mix") || flunk("mix is required for the crash test")

    _port =
      Port.open({:spawn_executable, mix}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["run", "--no-start", "-e", script],
        cd: File.cwd!(),
        env: [{~c"MIX_ENV", ~c"test"}]
      ])

    VialKeeper.Eventual.eventually(fn -> File.exists?(ready) end,
      timeout: 60_000,
      message: "ledger holder did not start"
    )

    [pid, visible, last] =
      ready |> File.read!() |> String.split() |> Enum.map(&String.to_integer/1)

    File.rm!(ready)
    %{pid: pid, visible: visible, last_reserved: last}
  end

  defp kill(pid), do: System.cmd("kill", ["-9", Integer.to_string(pid)], stderr_to_stdout: true)

  defp alive?(pid) do
    match?({_, 0}, System.cmd("kill", ["-0", Integer.to_string(pid)], stderr_to_stdout: true))
  end

  defp reserved_through(path),
    do: scalar(path, "SELECT sequence_reserved_through FROM db_meta WHERE id = 1")

  defp max_stored_sequence(path) do
    Enum.max([
      scalar(path, "SELECT coalesce(max(sequence), 0) FROM changes"),
      scalar(path, "SELECT coalesce(max(update_sequence), 0) FROM documents"),
      scalar(path, "SELECT coalesce(max(insertion_sequence), 0) FROM revisions")
    ])
  end

  defp scalar(path, sql) do
    {:ok, conn} = Connection.open(path, mode: [:readonly])

    try do
      {:ok, [[value]]} = Connection.query(conn, sql)
      value
    after
      Connection.close(conn)
    end
  end
end
