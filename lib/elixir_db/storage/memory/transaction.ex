defmodule ElixirDB.Storage.Memory.Transaction do
  @moduledoc """
  Memory implementation of the storage transaction port.

  Snapshots store state before running `fun` and rolls back on error.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      execute(adapter, context, Store.get(adapter.store), fun)
    end
  end

  defp execute(adapter, context, snapshot, fun) do
    case fun.(context) do
      {:ok, value} ->
        {:ok, value}

      {:error, error} ->
        _ = Store.update(adapter.store, fn _ -> {:ok, snapshot, :rolled_back} end)
        {:error, Errors.normalize(error)}
    end
  end
end
