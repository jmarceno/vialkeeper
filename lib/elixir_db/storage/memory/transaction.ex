defmodule ElixirDB.Storage.Memory.Transaction do
  @moduledoc """
  Memory implementation of the storage transaction port.

  Serializes callbacks per store, snapshots state before running `fun`, and
  restores the snapshot on errors or exceptions.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def run(%BackendContext{} = context, fun) when is_function(fun, 1) do
    with {:ok, adapter} <- Context.unwrap(context) do
      run_locked(adapter, context, fun)
    end
  end

  @impl true
  def run_snapshot(%BackendContext{} = context, fun) when is_function(fun, 1) do
    run(context, fun)
  end

  defp run_locked(adapter, context, fun) do
    case :global.trans({__MODULE__, adapter.store}, fn ->
           execute(adapter, context, Store.get(adapter.store), fun)
         end) do
      {:aborted, reason} ->
        {:error,
         ElixirDB.Error.internal_error("memory transaction could not acquire its lock", %{
           cause: inspect(reason)
         })}

      result ->
        result
    end
  end

  defp execute(adapter, context, snapshot, fun) do
    case fun.(context) do
      {:ok, value} ->
        {:ok, value}

      {:error, error} ->
        rollback(adapter.store, snapshot)
        {:error, Errors.normalize(error)}

      other ->
        rollback(adapter.store, snapshot)
        {:error, Errors.normalize(other)}
    end
  catch
    kind, reason ->
      rollback(adapter.store, snapshot)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp rollback(store, snapshot) do
    # Preserve the original callback result or exception if restoring the
    # snapshot itself fails. The Agent is still serialized with the callback,
    # so there is no concurrent writer that can supersede this restore.
    _ = Store.update(store, fn _ -> {:ok, snapshot, :rolled_back} end)
    :ok
  end
end
