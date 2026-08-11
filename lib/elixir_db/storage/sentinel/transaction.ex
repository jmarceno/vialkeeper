defmodule ElixirDB.Storage.Sentinel.Transaction do
  @moduledoc """
  SQL-free transaction port for the sentinel backend.

  Atomicity is process-local: the function runs to completion or returns an
  error without engine transaction text.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Sentinel.Context

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    case Context.unwrap(context) do
      {:ok, _adapter} -> Errors.wrap(fun.(context))
      {:error, _} = error -> error
    end
  end
end
