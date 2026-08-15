defmodule VialKeeper.Observability.Instrumentation.Changes do
  @moduledoc """
  Emitter for the changes event:

    * `vial_keeper.changes.read` — span + histogram

  Wraps each bounded changes read. The long-poll wait itself is NOT inside this
  span — only each bounded read is (avoids a span that lasts the whole wait_ms).
  """

  alias VialKeeper.Observability.{Meters, Tracer}

  @read_span "vial_keeper.changes.read"

  @doc """
  Wraps a changes read. `entries` is the bounded count of results returned.
  """
  @spec read(binary(), non_neg_integer(), (-> {:ok, term()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
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

    Meters.record(:"vial_keeper.changes.read.duration", duration, db_uuid: uuid, entries: count)

    _ = Tracer.set_attributes(entries: count)
    :ok
  end

  defp emit_read_result(
         uuid,
         duration,
         {:error, %VialKeeper.Error{code: :history_truncated} = error}
       ) do
    Meters.record(:"vial_keeper.changes.read.duration", duration,
      db_uuid: uuid,
      error_code: error.code
    )

    Meters.add(:"vial_keeper.changes.history_truncated.count", db_uuid: uuid)

    _ = Tracer.record_error(error)
    :ok
  end

  defp emit_read_result(uuid, duration, {:error, %VialKeeper.Error{} = error}) do
    Meters.record(:"vial_keeper.changes.read.duration", duration,
      db_uuid: uuid,
      error_code: error.code
    )

    _ = Tracer.record_error(error)
    _ = Tracer.apply_error_status(error)
    :ok
  end

  defp emit_read_result(uuid, duration, _other) do
    Meters.record(:"vial_keeper.changes.read.duration", duration, db_uuid: uuid)
    :ok
  end
end
