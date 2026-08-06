defmodule ElixirDB.Runtime.DatabaseOwner do
  @moduledoc false
  use GenServer
  alias ElixirDB.Storage.SQLite.Adapter

  def start_link({uuid, path}), do: GenServer.start_link(__MODULE__, {uuid, path}, name: via(uuid))
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}}}

  def command(uuid, command, timeout \\ 30_000) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] -> GenServer.call(pid, command, timeout)
      [] -> {:error, ElixirDB.Error.database_closed("database owner is not running")}
    end
  end

  @impl true
  def init({uuid, path}) do
    case Adapter.open(path) do
      {:ok, adapter} -> {:ok, %{uuid: uuid, path: path, adapter: adapter}}
      {:error, %ElixirDB.Error{} = error} -> {:stop, error}
    end
  end

  @impl true
  def handle_call({:command, :identity, _request}, _from, state),
    do: reply(Adapter.identity(state.adapter), state)

  def handle_call({:command, :update_config, request}, _from, state),
    do: reply(Adapter.update_config(state.adapter, request), state)

  def handle_call({:command, :integrity_check, request}, _from, state),
    do: reply(Adapter.integrity_check(state.adapter, request), state)

  def handle_call({:command, :get_document, request}, _from, state),
    do: reply(Adapter.get_document(state.adapter, request), state)

  def handle_call({:command, :get_revision, request}, _from, state),
    do: reply(Adapter.get_revision(state.adapter, request), state)

  def handle_call({:command, :put, request}, _from, state),
    do:
      mutate(Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :put)), state)

  def handle_call({:command, :delete, request}, _from, state),
    do:
      mutate(
        Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :delete)),
        state
      )

  def handle_call({:command, :bulk_write, request}, _from, state),
    do: mutate(Adapter.apply_bulk_mutation(state.adapter, request), state)

  def handle_call({:command, :resolve, request}, _from, state),
    do: mutate(Adapter.resolve_conflict(state.adapter, request), state)

  def handle_call({:command, :read_changes, request}, _from, state),
    do: reply(Adapter.read_changes(state.adapter, request), state)

  def handle_call({:command, :diff_revisions, request}, _from, state),
    do: reply(Adapter.diff_revisions(state.adapter, request), state)

  def handle_call({:command, :get_revision_chains, request}, _from, state),
    do: reply(Adapter.get_revision_chains(state.adapter, request), state)

  def handle_call({:command, :import_revision_chains, request}, _from, state),
    do: mutate(Adapter.import_revision_chains(state.adapter, request), state)

  def handle_call({:command, :get_local_record, namespace, key}, _from, state),
    do: reply(Adapter.get_local_record(state.adapter, namespace, key), state)

  def handle_call({:command, :put_local_record, request}, _from, state),
    do: reply(Adapter.put_local_record_cas(state.adapter, request), state)

  def handle_call({:command, :list_indexes, _request}, _from, state),
    do: reply(Adapter.list_indexes(state.adapter), state)

  def handle_call({:command, :create_index, request}, _from, state),
    do: reply(Adapter.create_index(state.adapter, request), state)

  def handle_call({:command, :delete_index, index_id}, _from, state),
    do: reply(Adapter.delete_index(state.adapter, index_id), state)

  def handle_call({:command, :rebuild_index, index_id}, _from, state),
    do: reply(Adapter.rebuild_index(state.adapter, index_id), state)

  def handle_call({:command, :query, request}, _from, state),
    do: reply(Adapter.execute_query(state.adapter, request), state)

  def handle_call({:command, :explain_query, request}, _from, state),
    do: reply(Adapter.explain_query(state.adapter, request), state)

  def handle_call({:command, :list_jobs, _request}, _from, state),
    do: reply(Adapter.list_replication_jobs(state.adapter), state)

  def handle_call({:command, :put_job, request}, _from, state),
    do: reply(Adapter.put_replication_job(state.adapter, request), state)

  def handle_call({:command, :delete_job, job_id}, _from, state),
    do: reply(Adapter.delete_replication_job(state.adapter, job_id), state)

  def handle_call({:command, :close}, _from, state), do: {:stop, :shutdown, :ok, state}

  def handle_call(_unknown, _from, state),
    do: {:reply, {:error, ElixirDB.Error.invalid_request("unknown database command")}, state}

  @impl true
  def terminate(_reason, %{adapter: adapter}), do: Adapter.close(adapter)

  defp mutate({:ok, %{sequence: sequence} = value}, state) do
    ElixirDB.Runtime.ChangeNotifier.publish(state.uuid, sequence)
    {:reply, {:ok, value}, state}
  end

  defp mutate({:ok, value}, state), do: {:reply, {:ok, value}, state}
  defp mutate({:error, _} = result, state), do: {:reply, result, state}
  defp reply(result, state), do: {:reply, result, state}
end
