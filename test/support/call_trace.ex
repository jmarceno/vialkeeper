defmodule VialKeeper.CallTrace do
  @moduledoc """
  Records calls to selected functions made while running one function.

  The function runs in a separate process traced by the caller (a process
  cannot usefully trace itself), so only calls made by that function are
  recorded. Trace patterns are VM-global: test modules using this helper must
  not run async with other modules that trace the same functions.
  """

  @doc """
  Runs `fun` and returns `{result, calls}`, where `calls` lists
  `{module, function, args}` for every call to one of `mfas`, in call order.
  """
  @spec run([mfa()], (-> term()), timeout()) :: {term(), [{module(), atom(), [term()]}]}
  def run(mfas, fun, timeout \\ 5_000) when is_list(mfas) and is_function(fun, 0) do
    parent = self()
    ref = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        receive do
          {:go, ^ref} -> send(parent, {ref, fun.()})
        end
      end)

    Enum.each(mfas, &:erlang.trace_pattern(&1, true, [:global]))
    _ = :erlang.trace(pid, true, [:call])

    try do
      send(pid, {:go, ref})
      result = await(ref, monitor, timeout)
      {result, collect(pid, [])}
    after
      Enum.each(mfas, &:erlang.trace_pattern(&1, false, [:global]))
    end
  end

  defp await(ref, monitor, timeout) do
    receive do
      {^ref, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, _pid, reason} ->
        exit({:traced_process_failed, reason})
    after
      timeout -> exit(:traced_process_timeout)
    end
  end

  # Drain with a short quiet period rather than relying on trace messages
  # arriving before the result message.
  defp collect(pid, acc) do
    receive do
      {:trace, ^pid, :call, {module, function, args}} ->
        collect(pid, [{module, function, args} | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end
