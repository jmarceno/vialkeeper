defmodule VialKeeper.Runtime.AdmittedCommandStartFlagTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Runtime.AdmittedCommand

  test "the scheduler cancels a granted command once and for good" do
    flag = AdmittedCommand.new_start_flag()

    assert AdmittedCommand.claim_unstarted(flag) == :unstarted
    assert AdmittedCommand.claim_unstarted(flag) == :unstarted
  end

  test "a command without an executor is unstarted" do
    assert AdmittedCommand.claim_unstarted(nil) == :unstarted
  end

  test "an executor does not start a command the scheduler cancelled" do
    flag = AdmittedCommand.new_start_flag()
    assert AdmittedCommand.claim_unstarted(flag) == :unstarted

    {:ok, executor} = AdmittedCommand.start_link(self())
    parent = self()
    {caller, caller_ref} = spawn_monitor(fn -> receive(do: (:stop -> :ok)) end)

    send(
      executor,
      {:run,
       %{
         uuid: "start-flag-test",
         request_ref: make_ref(),
         caller_pid: caller,
         start_flag: flag,
         from: nil,
         owner_fun: fn -> send(parent, :owner_ran) end,
         deadline_ms: :infinity,
         trace_context: OpenTelemetry.Ctx.get_current(),
         probe_op: nil
       }}
    )

    _ = :sys.get_state(executor, 5_000)
    refute_received :owner_ran
    refute_received {:admitted_command_done, _ref, ^executor}

    send(caller, :stop)
    assert_receive {:DOWN, ^caller_ref, :process, ^caller, :normal}, 1_000
  end

  test "an executor that started a command keeps it" do
    flag = AdmittedCommand.new_start_flag()
    {:ok, executor} = AdmittedCommand.start_link(self())
    parent = self()
    request_ref = make_ref()

    send(
      executor,
      {:run,
       %{
         uuid: "start-flag-test",
         request_ref: request_ref,
         caller_pid: self(),
         start_flag: flag,
         from: nil,
         owner_fun: fn -> send(parent, :owner_ran) end,
         deadline_ms: :infinity,
         trace_context: OpenTelemetry.Ctx.get_current(),
         probe_op: nil
       }}
    )

    assert_receive :owner_ran, 1_000
    assert_receive {:admitted_command_done, ^request_ref, ^executor}, 1_000
    assert AdmittedCommand.claim_unstarted(flag) == :started
  end
end
