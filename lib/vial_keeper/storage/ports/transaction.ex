defmodule VialKeeper.Storage.Ports.Transaction do
  @moduledoc """
  Atomic transaction port.

  Callers supply a function; the backend selects isolation and commit strategy.
  Callers never supply engine transaction text such as BEGIN/COMMIT/ROLLBACK.
  The function receives an opaque `VialKeeper.Storage.BackendContext` and must not
  pattern-match backend-private fields.
  """

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}
  @type fun :: (BackendContext.t() -> result(term()))

  @callback run(BackendContext.t(), fun()) :: result(term())
  @callback run_snapshot(BackendContext.t(), fun()) :: result(term())

  @doc """
  Runs a write transaction that may commit concurrently with other writer
  connections. A write-write conflict returns a retryable `:write_conflict`
  error. A backend with one writer implements it as `run/2`.
  """
  @callback run_concurrent(BackendContext.t(), fun()) :: result(term())
end
