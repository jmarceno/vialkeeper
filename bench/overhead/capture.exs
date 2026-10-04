defmodule VialKeeper.Benchmarks.Overhead.Capture do
  @moduledoc """
  Records the exact SQL an adapter operation executes, for replay controls.

  A dedicated worker process owns a private capture database (seeded like every
  other variant) and runs each sample's adapter operation there, untimed, with
  Erlang call tracing on the `Connection` functions every SQLite statement
  passes through:

    * `Connection.query/3` and `Connection.execute/3`, and their
      `point_query/3` / `point_execute/3` variants — statements the driver
      prepares from its statement cache (recorded with SQL, parameters, and,
      for queries, the returned row count),
    * `Connection.exec/2` — uncached control SQL such as `BEGIN IMMEDIATE`.

  The worker is a separate process on its own connection so its caches
  (driver statement cache, decoded bodies, index catalogs) never warm the
  measured adapter variant. Trace patterns are installed only for the duration of one capture
  and removed before any timed code runs.
  """

  alias VialKeeper.Storage.SQLite.Connection

  @timeout 120_000
  @traced [
    {Connection, :query, 3},
    {Connection, :execute, 3},
    {Connection, :point_query, 3},
    {Connection, :point_execute, 3},
    {Connection, :exec, 2}
  ]
  @queries [:query, :point_query]
  @executes [:execute, :point_execute]

  @type op ::
          %{kind: :query | :execute, sql: binary(), params: list(), rows: non_neg_integer() | nil}
          | %{kind: :exec, sql: binary()}

  @doc "Starts a linked worker whose state is the result of `init`."
  @spec start((-> term())) :: pid()
  def start(init) when is_function(init, 0) do
    parent = self()

    pid =
      spawn_link(fn ->
        state = init.()
        send(parent, {:capture_ready, self()})
        loop(state)
      end)

    receive do
      {:capture_ready, ^pid} -> pid
    after
      @timeout -> Mix.raise("capture worker did not start")
    end
  end

  @doc "Runs `fun.(state)` in the worker without tracing."
  @spec run(pid(), (term() -> result)) :: result when result: term()
  def run(worker, fun) do
    ref = make_ref()
    send(worker, {:run, ref, self(), fun})

    receive do
      {^ref, result} -> result
    after
      @timeout -> Mix.raise("capture worker did not reply")
    end
  end

  @doc "Runs `fun.(state)` in the worker and returns `{result, ops}`."
  @spec capture(pid(), (term() -> result)) :: {result, [op()]} when result: term()
  def capture(worker, fun) do
    Enum.each(@traced, fn mfa ->
      :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])
    end)

    _ = :erlang.trace(worker, true, [:call, {:tracer, self()}])

    result =
      try do
        run(worker, fun)
      after
        _ = :erlang.trace(worker, false, [:call])
        Enum.each(@traced, fn mfa -> :erlang.trace_pattern(mfa, false, [:local]) end)
      end

    ref = :erlang.trace_delivered(worker)

    receive do
      {:trace_delivered, ^worker, ^ref} -> :ok
    after
      @timeout -> Mix.raise("capture trace was not delivered")
    end

    {result, collect(worker, [])}
  end

  @doc "Stops the worker."
  @spec stop(pid()) :: :ok
  def stop(worker) do
    Process.unlink(worker)
    Process.exit(worker, :shutdown)
    :ok
  end

  defp loop(state) do
    receive do
      {:run, ref, from, fun} ->
        send(from, {ref, fun.(state)})
        loop(state)
    end
  end

  defp collect(worker, ops) do
    receive do
      {:trace, ^worker, :call, {Connection, name, [_conn, sql, params]}} when name in @queries ->
        collect(worker, [
          %{kind: :query, sql: IO.iodata_to_binary(sql), params: params, rows: nil} | ops
        ])

      {:trace, ^worker, :call, {Connection, name, [_conn, sql, params]}} when name in @executes ->
        collect(worker, [
          %{kind: :execute, sql: IO.iodata_to_binary(sql), params: params, rows: nil} | ops
        ])

      {:trace, ^worker, :call, {Connection, :exec, [_conn, sql]}} ->
        collect(worker, [%{kind: :exec, sql: sql} | ops])

      {:trace, ^worker, :return_from, {Connection, name, 3}, result} when name in @queries ->
        [last | rest] = ops
        collect(worker, [%{last | rows: returned_rows(result)} | rest])

      {:trace, ^worker, :return_from, {Connection, name, _arity}, result}
      when name in [:exec | @executes] ->
        unless result == :ok, do: Mix.raise("captured #{name} failed: #{inspect(result)}")
        collect(worker, ops)
    after
      0 -> Enum.reverse(ops)
    end
  end

  defp returned_rows({:ok, rows}), do: length(rows)
  defp returned_rows(other), do: Mix.raise("captured query failed: #{inspect(other)}")
end
