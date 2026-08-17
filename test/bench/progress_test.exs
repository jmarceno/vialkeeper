defmodule VialKeeper.Bench.ProgressTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.Progress

  test "prints at 10% steps and keeps a consistent snapshot" do
    parent = self()
    printer = fn level, message -> send(parent, {:printed, level, message}) end

    {:ok, pid} =
      Progress.start(
        owner: self(),
        stall_timeout_ms: 0,
        heartbeat_ms: 60_000,
        check_interval_ms: 60_000,
        printer: printer
      )

    on_exit(fn -> Progress.stop(pid) end)

    Progress.phase(pid, "ingest", 100)
    assert_receive {:printed, :info, start_line}
    assert start_line =~ "start ingest total=100"

    Enum.each(1..10, fn _ -> Progress.tick(pid) end)
    snapshot = Progress.snapshot(pid)
    assert snapshot.processed == 10
    assert snapshot.total == 100
    assert snapshot.phase == "ingest"

    assert_receive {:printed, :info, progress_line}
    assert progress_line =~ "ingest 10/100 (10%)"
    refute progress_line =~ "heartbeat"
  end

  test "heartbeats when percent has not moved" do
    parent = self()
    printer = fn level, message -> send(parent, {:printed, level, message}) end

    {:ok, pid} =
      Progress.start(
        owner: self(),
        stall_timeout_ms: 0,
        heartbeat_ms: 30,
        check_interval_ms: 10,
        printer: printer
      )

    on_exit(fn -> Progress.stop(pid) end)

    Progress.phase(pid, "ingest", 100)
    assert_receive {:printed, :info, _}
    Progress.tick(pid)

    assert_receive {:printed, :info, heartbeat}, 500
    assert heartbeat =~ "heartbeat"
    assert heartbeat =~ "ingest 1/100"
  end

  test "stall exits the owner, runs cleanup, and prints diagnostics" do
    parent = self()

    owner =
      spawn(fn ->
        send(parent, {:owner_ready, self()})
        Process.sleep(:infinity)
      end)

    owner_ref = Process.monitor(owner)
    assert_receive {:owner_ready, ^owner}

    printer = fn level, message -> send(parent, {:printed, level, message}) end
    cleanup = fn -> send(parent, :cleaned) end

    {:ok, pid} =
      Progress.start(
        owner: owner,
        stall_timeout_ms: 40,
        heartbeat_ms: 10_000,
        check_interval_ms: 10,
        printer: printer,
        cleanup: cleanup,
        label: "torture"
      )

    Progress.phase(pid, "attachment_ingest", 1000)

    assert_receive {:DOWN, ^owner_ref, :process, ^owner, {:bench_stall, message}}, 500
    assert_receive :cleaned, 500
    assert_receive {:printed, :error, dump}, 500
    assert dump =~ "STALL"
    assert dump =~ "attachment_ingest"
    assert dump =~ "0/1000"
    assert message =~ "STALL"

    Progress.stop(pid)
  end

  test "ticks prevent stall" do
    parent = self()
    owner = spawn(fn -> Process.sleep(400) end)
    owner_ref = Process.monitor(owner)
    printer = fn level, message -> send(parent, {:printed, level, message}) end

    {:ok, pid} =
      Progress.start(
        owner: owner,
        stall_timeout_ms: 80,
        heartbeat_ms: 10_000,
        check_interval_ms: 15,
        printer: printer
      )

    on_exit(fn -> Progress.stop(pid) end)

    Progress.phase(pid, "ingest", 20)

    Enum.each(1..6, fn _ ->
      Process.sleep(25)
      Progress.tick(pid)
    end)

    refute_receive {:printed, :error, _}, 50
    assert Process.alive?(owner)
    Progress.stop(pid)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, _reason}, 500
  end

  test "concurrent ticks are counted exactly once each" do
    {:ok, pid} =
      Progress.start(
        owner: self(),
        stall_timeout_ms: 0,
        heartbeat_ms: 60_000,
        check_interval_ms: 60_000,
        printer: fn _level, _message -> :ok end
      )

    on_exit(fn -> Progress.stop(pid) end)

    Progress.phase(pid, "concurrent_write:16", 100)

    1..100
    |> Task.async_stream(fn _ -> Progress.tick(pid) end,
      max_concurrency: 16,
      timeout: 5_000,
      ordered: false
    )
    |> Stream.run()

    assert Progress.snapshot(pid).processed == 100
  end
end
