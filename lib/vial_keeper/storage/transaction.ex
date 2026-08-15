defmodule VialKeeper.Storage.Transaction do
  @moduledoc """
  Backend-agnostic atomic transaction entry point.

  Dispatches to the transaction port of the backend recorded in
  `VialKeeper.Storage.BackendContext`. Callers never pass engine transaction text.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors

  @type fun :: (BackendContext.t() -> {:ok, term()} | {:error, VialKeeper.Error.t()})

  @doc """
  Runs `fun` atomically for `context`.

  `fun` receives an opaque backend context and must return `{:ok, value}` or
  `{:error, VialKeeper.Error.t()}`.
  """
  @spec run(BackendContext.t(), fun()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    backend = BackendContext.backend(context)

    cond do
      function_exported?(backend, :run_transaction, 2) ->
        Errors.wrap(backend.run_transaction(context, fun))

      function_exported?(backend, :transaction_port, 0) ->
        Errors.wrap(backend.transaction_port().run(context, fun))

      true ->
        missing_transaction_port()
    end
  end

  @doc """
  Runs `fun` against one consistent snapshot of `context`.

  `fun` receives an opaque backend context and must return `{:ok, value}` or
  `{:error, VialKeeper.Error.t()}`. Nested snapshots are an error.
  """
  @spec run_snapshot(BackendContext.t(), fun()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def run_snapshot(%BackendContext{} = context, fun) when is_function(fun, 1) do
    backend = BackendContext.backend(context)

    cond do
      function_exported?(backend, :run_snapshot, 2) ->
        Errors.wrap(backend.run_snapshot(context, fun))

      function_exported?(backend, :transaction_port, 0) ->
        Errors.wrap(backend.transaction_port().run_snapshot(context, fun))

      true ->
        missing_transaction_port()
    end
  end

  defp missing_transaction_port do
    {:error,
     VialKeeper.Error.internal_error("storage backend does not implement the transaction port")}
  end
end
