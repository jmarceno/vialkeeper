defmodule ElixirDB.Storage.Transaction do
  @moduledoc """
  Backend-agnostic atomic transaction entry point.

  Dispatches to the transaction port of the backend recorded in
  `ElixirDB.Storage.BackendContext`. Callers never pass engine transaction text.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors

  @type fun :: (BackendContext.t() -> {:ok, term()} | {:error, ElixirDB.Error.t()})

  @doc """
  Runs `fun` atomically for `context`.

  `fun` receives an opaque backend context and must return `{:ok, value}` or
  `{:error, ElixirDB.Error.t()}`.
  """
  @spec run(BackendContext.t(), fun()) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    backend = BackendContext.backend(context)

    cond do
      function_exported?(backend, :run_transaction, 2) ->
        Errors.wrap(backend.run_transaction(context, fun))

      function_exported?(backend, :transaction_port, 0) ->
        port = backend.transaction_port()
        Errors.wrap(port.run(context, fun))

      true ->
        {:error,
         ElixirDB.Error.internal_error("storage backend does not implement the transaction port")}
    end
  end
end
