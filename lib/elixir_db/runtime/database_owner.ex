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
      {:ok, adapter} ->
        case Map.get(adapter.identity, :database_uuid) do
          ^uuid ->
            {:ok, %{uuid: uuid, path: path, adapter: adapter}}

          actual ->
            _ = Adapter.close(adapter)

            {:stop,
             ElixirDB.Error.database_unavailable("database UUID mismatch", %{
               reason: :uuid_mismatch,
               expected: uuid,
               actual: actual
             })}
        end

      {:error, %ElixirDB.Error{} = error} ->
        {:stop, error}
    end
  end

  @impl true
  def handle_call(command, from, state) do
    handle_command(ElixirDB.Storage.Commands.normalize(command), from, state)
  end

  defp handle_command(%ElixirDB.Storage.Commands.Identity{}, _from, state),
    do: reply(Adapter.identity(state.adapter), state)

  defp handle_command(%ElixirDB.Storage.Commands.UpdateConfig{request: request}, _from, state),
    do: reply(Adapter.update_config(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.IntegrityCheck{request: request}, _from, state),
    do: reply(Adapter.integrity_check(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.GetDocument{request: request}, _from, state),
    do: reply(wrap_get(Adapter.get_document(state.adapter, request)), state)

  defp handle_command(%ElixirDB.Storage.Commands.GetRevision{request: request}, _from, state),
    do: reply(wrap_get(Adapter.get_revision(state.adapter, request)), state)

  defp handle_command(%ElixirDB.Storage.Commands.PutDocument{request: request}, _from, state),
    do:
      mutate(
        wrap_put(
          Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :put))
        ),
        state
      )

  defp handle_command(%ElixirDB.Storage.Commands.DeleteDocument{request: request}, _from, state),
    do:
      mutate(
        wrap_put(
          Adapter.apply_local_mutation(state.adapter, Map.put(request, :operation, :delete))
        ),
        state
      )

  defp handle_command(%ElixirDB.Storage.Commands.BulkWrite{request: request}, _from, state),
    do: mutate(Adapter.apply_bulk_mutation(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.ResolveConflict{request: request}, _from, state),
    do: mutate(Adapter.resolve_conflict(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.ReadChanges{request: request}, _from, state),
    do: reply(wrap_changes(Adapter.read_changes(state.adapter, request)), state)

  defp handle_command(%ElixirDB.Storage.Commands.DiffRevisions{request: request}, _from, state),
    do: reply(Adapter.diff_revisions(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.GetRevisionChains{request: request}, _from, state),
    do: reply(Adapter.get_revision_chains(state.adapter, request), state)

  defp handle_command(
         %ElixirDB.Storage.Commands.ImportRevisionChains{request: request},
         _from,
         state
       ),
       do: mutate(Adapter.import_revision_chains(state.adapter, request), state)

  defp handle_command(
         %ElixirDB.Storage.Commands.GetLocalRecord{namespace: namespace, key: key},
         _from,
         state
       ),
       do: reply(Adapter.get_local_record(state.adapter, namespace, key), state)

  defp handle_command(%ElixirDB.Storage.Commands.GetCheckpoint{replication_id: id}, _from, state),
    do: reply(Adapter.get_local_record(state.adapter, "checkpoints", id), state)

  defp handle_command(%ElixirDB.Storage.Commands.PutLocalRecord{request: request}, _from, state),
    do: reply(Adapter.put_local_record_cas(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.PutCheckpoint{request: request}, _from, state),
    do: reply(Adapter.put_local_record_cas(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.ListIndexes{}, _from, state),
    do: reply(Adapter.list_indexes(state.adapter), state)

  defp handle_command(%ElixirDB.Storage.Commands.CreateIndex{request: request}, _from, state),
    do: reply(Adapter.create_index(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.DeleteIndex{index_id: index_id}, _from, state),
    do: reply(Adapter.delete_index(state.adapter, index_id), state)

  defp handle_command(%ElixirDB.Storage.Commands.RebuildIndex{index_id: index_id}, _from, state),
    do: reply(Adapter.rebuild_index(state.adapter, index_id), state)

  defp handle_command(%ElixirDB.Storage.Commands.ExecuteQuery{request: request}, _from, state),
    do: reply(Adapter.execute_query(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.ExplainQuery{request: request}, _from, state),
    do: reply(Adapter.explain_query(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.ListJobs{}, _from, state),
    do: reply(Adapter.list_replication_jobs(state.adapter), state)

  defp handle_command(%ElixirDB.Storage.Commands.PutJob{request: request}, _from, state),
    do: reply(Adapter.put_replication_job(state.adapter, request), state)

  defp handle_command(%ElixirDB.Storage.Commands.DeleteJob{job_id: job_id}, _from, state),
    do: reply(Adapter.delete_replication_job(state.adapter, job_id), state)

  defp handle_command(%ElixirDB.Storage.Commands.Close{}, _from, state),
    do: {:stop, :shutdown, :ok, state}

  defp handle_command(_unknown, _from, state),
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

  defp wrap_get({:ok, map}) when is_map(map),
    do: {:ok, ElixirDB.Storage.Results.get_document(map)}

  defp wrap_get(other), do: other

  defp wrap_put({:ok, map}) when is_map(map),
    do: {:ok, ElixirDB.Storage.Results.put_document(map)}

  defp wrap_put(other), do: other

  defp wrap_changes({:ok, map}) when is_map(map),
    do: {:ok, ElixirDB.Storage.Results.read_changes(map)}

  defp wrap_changes(other), do: other
end
