defmodule ElixirDB.Storage.Memory.Transaction do
  @moduledoc """
  Memory implementation of the storage transaction port.

  Write transactions serialize callbacks per store, copy state before running
  `fun`, and restore that copy on errors. Read snapshots take the same lock
  without copying; snapshot bodies must not mutate the store.
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
    with {:ok, adapter} <- Context.unwrap(context) do
      trans_result(:global.trans({__MODULE__, adapter.store}, fn -> fun.(context) end))
    end
  end

  defp run_locked(adapter, context, fun) do
    trans_result(
      :global.trans({__MODULE__, adapter.store}, fn ->
        execute(adapter, context, Store.get(adapter.store), fun)
      end)
    )
  end

  defp trans_result({:aborted, reason}) do
    {:error,
     ElixirDB.Error.internal_error("memory transaction could not acquire its lock", %{
       cause: inspect(reason)
     })}
  end

  defp trans_result(result), do: result

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
