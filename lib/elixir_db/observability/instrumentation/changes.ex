defmodule ElixirDB.Observability.Instrumentation.Changes do
  @moduledoc """
  Emitter for the Plan §11 changes event:

    * `elixir_db.changes.read` — span + histogram

  Wraps each bounded changes read. The long-poll wait itself is NOT inside this
  span — only each bounded read is (avoids a span that lasts the whole wait_ms).
  """

  alias ElixirDB.Observability.{Meters, Tracer}

  @read_span "elixir_db.changes.read"

  @doc """
  Wraps a changes read. `entries` is the bounded count of results returned.
  """
  @spec read(binary(), non_neg_integer(), (-> {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def read(uuid, entries, fun)
      when is_binary(uuid) and is_integer(entries) and is_function(fun, 0) do
    started = System.monotonic_time()

    Tracer.with_span(@read_span, [db_uuid: uuid, entries: entries], fn ->
      result = fun.()
      duration = System.monotonic_time() - started
      emit_read_result(uuid, duration, result)
      result
    end)
  end

  defp emit_read_result(uuid, duration, {:ok, %{results: results}}) do
    count = length(results)

    Meters.record(:"elixir_db.changes.read.duration", duration, db_uuid: uuid, entries: count)

    _ = Tracer.set_attributes(entries: count)
    :ok
  end

  defp emit_read_result(uuid, duration, {:error, %ElixirDB.Error{} = error}) do
    Meters.record(:"elixir_db.changes.read.duration", duration,
      db_uuid: uuid,
      error_code: error.code
    )

    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_read_result(uuid, duration, _other) do
    Meters.record(:"elixir_db.changes.read.duration", duration, db_uuid: uuid)
    :ok
  end
end
