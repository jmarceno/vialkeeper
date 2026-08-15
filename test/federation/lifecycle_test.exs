defmodule VialKeeper.Federation.LifecycleTest do
  @moduledoc "Covers ownership and shutdown of federation source tasks."

  use ExUnit.Case, async: true

  alias VialKeeper.Federation.Executor

  @source "123e4567-e89b-12d3-a456-426614174000"

  test "caller termination takes down the private source task supervisor" do
    parent = self()

    caller =
      spawn(fn ->
        Executor.run(
          %{databases: [@source], query: %{limit: 1}},
          source_fetcher: fn _source_uuid, _request, _deadline ->
            send(parent, {:source_started, self()})

            receive do
              :release -> {:ok, %{documents: [], sequence: 1, has_more: false}}
            end
          end
        )
      end)

    caller_ref = Process.monitor(caller)
    assert_receive {:source_started, source_pid}, 1_000
    source_ref = Process.monitor(source_pid)

    Process.exit(caller, :kill)

    assert_receive {:DOWN, ^caller_ref, :process, ^caller, :killed}, 1_000
    assert_receive {:DOWN, ^source_ref, :process, ^source_pid, _reason}, 1_000
  end
end
