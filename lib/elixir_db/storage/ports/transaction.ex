defmodule ElixirDB.Storage.Ports.Transaction do
  @moduledoc """
  Atomic transaction port.

  Callers supply a function; the backend selects isolation and commit strategy.
  Callers never supply engine transaction text such as BEGIN/COMMIT/ROLLBACK.
  The function receives an opaque `ElixirDB.Storage.BackendContext` and must not
  pattern-match backend-private fields.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}
  @type fun :: (BackendContext.t() -> result(term()))

  @callback run(BackendContext.t(), fun()) :: result(term())
end
