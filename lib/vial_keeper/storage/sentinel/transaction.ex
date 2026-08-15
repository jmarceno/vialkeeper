defmodule VialKeeper.Storage.Sentinel.Transaction do
  @moduledoc """
  SQL-free transaction port for the sentinel backend.

  Atomicity is process-local: the function runs to completion or returns an
  error without engine transaction text.
  """
  @behaviour VialKeeper.Storage.Ports.Transaction

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.Sentinel.Context

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    case Context.unwrap(context) do
      {:ok, _adapter} -> Errors.wrap(fun.(context))
      {:error, _} = error -> error
    end
  end

  @impl true
  def run_snapshot(%BackendContext{} = context, fun) when is_function(fun, 1) do
    run(context, fun)
  end
end
