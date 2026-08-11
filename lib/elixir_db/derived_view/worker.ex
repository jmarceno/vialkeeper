defmodule ElixirDB.DerivedView.Worker do
  @moduledoc "Durable materializer worker for one derived database."
  use GenServer
  require Logger

  alias ElixirDB.Changes
  alias ElixirDB.DerivedView.Supervisor
  alias ElixirDB.Error
  alias ElixirDB.Runtime.{ChangeNotifier, ChildSpec, DatabaseCatalog}
  alias ElixirDB.Storage.Results.ReadChanges
  alias ElixirDB.View.Program

  @work_timeout 30_000

  @spec child_spec(binary()) :: map()
  def child_spec(uuid) when is_binary(uuid) do
    ChildSpec.worker({:derived_worker, uuid}, {__MODULE__, :start_link, [uuid]}, :permanent)
  end

  @spec start_link(binary()) :: GenServer.on_start()
  def start_link(uuid), do: GenServer.start_link(__MODULE__, uuid, name: via(uuid))

  @spec via(binary()) :: {:via, Registry, {module(), term()}}
  def via(uuid), do: {:via, Registry, {ElixirDB.Runtime.DatabaseRegistry, {:derived_worker, uuid}}}

  @spec pid(binary()) :: {:ok, pid()} | :error
  def pid(uuid) when is_binary(uuid) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:derived_worker, uuid}) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @spec refresh(binary()) :: :ok
  def refresh(uuid) when is_binary(uuid) do
    case pid(uuid) do
      {:ok, worker} -> GenServer.cast(worker, :refresh)
      :error -> :ok
    end

    :ok
  end

  @spec status(binary()) :: {:ok, map()} | {:error, Error.t()}
  def status(uuid) when is_binary(uuid) do
    case pid(uuid) do
      {:ok, worker} -> GenServer.call(worker, :status, @work_timeout)
      :error -> {:error, Error.database_closed("derived materializer is not running")}
    end
  catch
    :exit, reason ->
      {:error,
       Error.database_unavailable("derived materializer status failed", %{cause: inspect(reason)})}
  end

  @impl true
  def init(uuid) do
    send(self(), :bootstrap)

    {:ok,
     %{
       uuid: uuid,
       metadata: nil,
       definition: nil,
       sources: %{},
       subscriptions: %{},
       subscription_since: %{},
       operation: nil,
       tasks: %{},
       work_queued: false,
       work_requested: false,
       retry_attempt: 0,
       retry_timer: nil,
       needs_rebuild: MapSet.new(),
       status: :starting
     }}
  end

  @impl true
  def terminate(_reason, state) do
    cancel_tasks(state)
    unsubscribe_all(state)
    cancel_retry_timer(state)
    :ok
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply,
     {:ok,
      %{
        database_uuid: state.uuid,
        status: state.status,
        enabled: enabled?(state),
        operation: operation_name(state.operation),
        source_count: map_size(state.sources)
      }}, state}
  end

  @impl true
  def handle_cast(:refresh, state) do
    state =
      state
      |> cancel_tasks()
      |> cancel_retry_timer()
      |> Map.merge(%{
        metadata: nil,
        definition: nil,
        operation: nil,
        work_queued: false,
        work_requested: false,
        retry_attempt: 0,
        status: :starting
      })

    send(self(), :bootstrap)
    {:noreply, state}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    case load_context(state.uuid) do
      {:ok, metadata, sources} ->
        state = put_context(state, metadata, sources)

        case configure_subscriptions(state) do
          {:ok, state} ->
            {:noreply, request_work(reset_retry(state))}

          {:error, source_uuid, error, state} ->
            {:noreply, handle_failure(state, source_uuid, error)}
        end

      {:error, error} ->
        {:noreply, handle_failure(state, nil, error)}
    end
  end

  @impl true
  def handle_info(:retry_work, state) do
    {:noreply, request_work(%{state | retry_timer: nil})}
  end

  @impl true
  def handle_info(:work, state) do
    state = %{state | work_queued: false}

    cond do
      state.metadata == nil ->
        {:noreply, state}

      not enabled?(state) ->
        {:noreply, %{state | status: :disabled}}

      map_size(state.tasks) > 0 ->
        {:noreply, %{state | work_requested: true}}

      state.operation != nil ->
        {:noreply, run_operation_phase(state)}

      true ->
        {:noreply, start_next_work(state)}
    end
  end

  @impl true
  def handle_info({:database_changed, source_uuid, _sequence}, state) do
    if Map.has_key?(state.sources, source_uuid),
      do: {:noreply, request_work(state)},
      else: {:noreply, state}
  end

  @impl true
  def handle_info({:database_maintenance, source_uuid, _event}, state) do
    if Map.has_key?(state.sources, source_uuid),
      do: {:noreply, request_work(state)},
      else: {:noreply, state}
  end

  @impl true
  def handle_info({:database_closed, source_uuid}, state) do
    {:noreply, request_work(remove_subscription(state, source_uuid))}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {%{token: token}, tasks} ->
        Process.demonitor(ref, [:flush])
        handle_task_result(token, result, %{state | tasks: tasks})
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) when is_reference(ref) do
    case Map.pop(state.tasks, ref) do
      {nil, _tasks} ->
        {:noreply, state}

      {%{token: token}, tasks} ->
        handle_task_result(token, {:error, task_error(reason)}, %{state | tasks: tasks})
    end
  end

  defp load_context(uuid) do
    with {:ok, metadata} <-
           DatabaseCatalog.command_as(uuid, :maintenance, {:command, :get_derived_view, %{}}),
         {:ok, sources} <-
           DatabaseCatalog.command_as(uuid, :maintenance, {:command, :list_derived_sources, %{}}) do
      {:ok, metadata, sources}
    end
  end

  defp put_context(state, metadata, sources) do
    %{
      state
      | metadata: metadata,
        definition: metadata.definition,
        sources: Map.new(sources, &{&1.source_database_uuid, &1}),
        status: if(metadata.enabled, do: metadata.status, else: :disabled)
    }
  end

  defp configure_subscriptions(state) do
    if enabled?(state) do
      subscribe_sources(state, sorted_sources(state))
    else
      unsubscribe_all(state)
      |> then(&{:ok, &1})
    end
  end

  defp subscribe_sources(state, sources) do
    Enum.reduce_while(sources, {:ok, state}, fn source, {:ok, state} ->
      subscribe_source(state, source)
    end)
  end

  defp subscribe_source(state, source) do
    case ensure_subscription(state, source) do
      {:ok, next} -> {:cont, {:ok, next}}
      {:error, error, next} -> {:halt, {:error, source.source_database_uuid, error, next}}
    end
  end

  defp ensure_subscription(state, source) do
    since = source_since(source)

    if Map.get(state.subscription_since, source.source_database_uuid) == since and
         Map.has_key?(state.subscriptions, source.source_database_uuid) do
      {:ok, state}
    else
      state = remove_subscription(state, source.source_database_uuid)

      with {:ok, _identity} <- source_identity(source.source_database_uuid),
           {:ok, ref, current} <- ChangeNotifier.subscribe(source.source_database_uuid, since) do
        {:ok, put_subscription(state, source.source_database_uuid, ref, since, current)}
      else
        {:error, error} -> {:error, error, state}
      end
    end
  end

  defp replace_subscription(state, source_uuid, since) do
    state = remove_subscription(state, source_uuid)

    case ChangeNotifier.subscribe(source_uuid, since) do
      {:ok, ref, current} ->
        {:ok, put_subscription(state, source_uuid, ref, since, current)}

      {:error, error} ->
        {:error, error, state}
    end
  end

  defp put_subscription(state, source_uuid, ref, since, current) do
    state = %{
      state
      | subscriptions: Map.put(state.subscriptions, source_uuid, ref),
        subscription_since: Map.put(state.subscription_since, source_uuid, since)
    }

    if is_integer(current) and current > since, do: %{state | work_requested: true}, else: state
  end

  defp remove_subscription(state, source_uuid) do
    case Map.pop(state.subscriptions, source_uuid) do
      {nil, subscriptions} ->
        %{
          state
          | subscriptions: subscriptions,
            subscription_since: Map.delete(state.subscription_since, source_uuid)
        }

      {ref, subscriptions} ->
        ChangeNotifier.unsubscribe(source_uuid, ref)
        Process.demonitor(ref, [:flush])

        %{
          state
          | subscriptions: subscriptions,
            subscription_since: Map.delete(state.subscription_since, source_uuid)
        }
    end
  end

  defp unsubscribe_all(state) do
    Enum.each(state.subscriptions, fn {source_uuid, ref} ->
      ChangeNotifier.unsubscribe(source_uuid, ref)
      Process.demonitor(ref, [:flush])
    end)

    %{state | subscriptions: %{}, subscription_since: %{}}
  end

  defp start_next_work(state) do
    case next_rebuild_source(state) do
      nil -> start_active_fetch(state)
      source -> start_rebuild(state, source)
    end
  end

  defp next_rebuild_source(state) do
    state.sources
    |> Map.values()
    |> Enum.sort_by(& &1.source_ordinal)
    |> Enum.find(fn source ->
      source.state in [:pending, :rebuilding] or
        MapSet.member?(state.needs_rebuild, source.source_database_uuid)
    end)
  end

  defp start_active_fetch(state) do
    sources = Enum.filter(sorted_sources(state), &(&1.state == :active))

    if sources == [] do
      %{state | status: :rebuilding}
    else
      state =
        %{state | operation: %{kind: :active, queue: sources, in_flight: %{}, results: %{}}}

      fill_active_tasks(state)
    end
  end

  defp fill_active_tasks(%{operation: %{kind: :active} = operation} = state) do
    capacity = max_concurrent_sources(state) - map_size(operation.in_flight)

    cond do
      capacity > 0 and operation.queue != [] ->
        start_active_task(state, operation)

      operation.queue == [] and map_size(operation.in_flight) == 0 ->
        apply_active_results(state, operation.results)

      true ->
        state
    end
  end

  defp start_active_task(state, operation) do
    [source | queue] = operation.queue

    case start_task(state, {:active, source.source_database_uuid}, fn ->
           fetch_incremental(source, state.definition)
         end) do
      {:ok, next, ref} ->
        operation = %{
          operation
          | queue: queue,
            in_flight: Map.put(operation.in_flight, ref, source)
        }

        fill_active_tasks(%{next | operation: operation})

      {:error, error, next} ->
        handle_failure(%{next | operation: operation}, source.source_database_uuid, error)
    end
  end

  defp sorted_sources(state),
    do: state.sources |> Map.values() |> Enum.sort_by(& &1.source_ordinal)

  defp handle_task_result({:active, source_uuid}, result, state) do
    case state.operation do
      %{kind: :active, in_flight: in_flight, results: results} = operation ->
        in_flight = Map.delete(in_flight, task_ref_for_source(operation, source_uuid))

        case result do
          {:ok, value} ->
            state = %{
              state
              | operation: %{
                  operation
                  | in_flight: in_flight,
                    results: Map.put(results, source_uuid, value)
                }
            }

            {:noreply, fill_active_tasks(state)}

          {:error, error} ->
            {:noreply,
             handle_failure(
               %{state | operation: %{operation | in_flight: in_flight}},
               source_uuid,
               error
             )}
        end

      _ ->
        {:noreply, state}
    end
  end

  defp handle_task_result({:begin_rebuild, source_uuid}, result, state) do
    case result do
      {:ok, identity} ->
        begin_rebuild_after_identity(state, source_uuid, identity)

      {:error, error} ->
        {:noreply, handle_failure(state, source_uuid, error)}
    end
  end

  defp handle_task_result({:rebuild_snapshot, source_uuid}, result, state) do
    case {state.operation, result} do
      {%{kind: :rebuild, source_uuid: ^source_uuid, phase: :snapshot} = operation, {:ok, page}} ->
        apply_snapshot_page(state, operation, page)

      {_operation, {:error, error}} ->
        {:noreply, handle_failure(state, source_uuid, error)}

      _ ->
        {:noreply, state}
    end
  end

  defp handle_task_result({:rebuild_target, source_uuid}, result, state) do
    case {state.operation, result} do
      {%{kind: :rebuild, source_uuid: ^source_uuid, phase: :target} = operation, {:ok, identity}} ->
        target = max(operation.catchup_sequence, identity.current_sequence)

        {:noreply,
         request_work(%{
           state
           | operation: %{
               operation
               | phase: :replay,
                 target_sequence: target,
                 replay_has_more: false,
                 history_epoch: identity.history_epoch
             }
         })}

      {_operation, {:error, error}} ->
        {:noreply, handle_failure(state, source_uuid, error)}

      _ ->
        {:noreply, state}
    end
  end

  defp handle_task_result({:rebuild_replay, source_uuid}, result, state) do
    case {state.operation, result} do
      {%{kind: :rebuild, source_uuid: ^source_uuid, phase: :replay} = operation, {:ok, data}} ->
        apply_replay_page(state, operation, data)

      {_operation, {:error, error}} ->
        {:noreply, handle_failure(state, source_uuid, error)}

      _ ->
        {:noreply, state}
    end
  end

  defp handle_task_result(_token, {:error, error}, state),
    do: {:noreply, handle_failure(state, nil, error)}

  defp begin_rebuild_after_identity(state, source_uuid, identity) do
    request = %{
      materialization_id: state.metadata.materialization_id,
      source_database_uuid: source_uuid,
      start_sequence: identity.current_sequence,
      catchup_sequence: identity.current_sequence
    }

    case destination_command(state.uuid, :begin_derived_source_rebuild, request) do
      {:ok, %{generation: generation}} ->
        source = Map.fetch!(state.sources, source_uuid)

        source = %{
          source
          | state: :rebuilding,
            rebuild_generation: generation,
            rebuild_start_sequence: identity.current_sequence,
            rebuild_after_document_id: nil,
            rebuild_catchup_sequence: identity.current_sequence
        }

        state =
          %{
            state
            | sources: Map.put(state.sources, source_uuid, source),
              needs_rebuild: MapSet.delete(state.needs_rebuild, source_uuid),
              operation: rebuild_operation(source)
          }

        case replace_subscription(state, source_uuid, identity.current_sequence) do
          {:ok, next} -> {:noreply, request_work(next)}
          {:error, error, next} -> {:noreply, handle_failure(next, source_uuid, error)}
        end

      {:error, error} ->
        {:noreply, handle_failure(state, source_uuid, error)}
    end
  end

  defp apply_snapshot_page(state, operation, page) do
    with {:ok, rows, removals} <- documents_to_rows(state.definition, Map.get(page, :documents, [])),
         {:ok, _result} <-
           destination_command(
             state.uuid,
             :apply_derived_rebuild_page,
             rebuild_page_request(state, operation, rows, removals, page.next_after)
           ) do
      next_after = page.next_after
      source = Map.fetch!(state.sources, operation.source_uuid)
      source = %{source | rebuild_after_document_id: next_after}

      operation =
        if is_nil(next_after),
          do: %{operation | phase: :prune, prune_cursor: nil},
          else: %{operation | cursor: next_after}

      state = %{
        state
        | sources: Map.put(state.sources, operation.source_uuid, source),
          operation: operation
      }

      {:noreply, request_work(state)}
    else
      {:error, error} -> {:noreply, handle_failure(state, operation.source_uuid, error)}
    end
  end

  defp run_operation_phase(%{operation: %{kind: :rebuild, phase: :snapshot} = operation} = state) do
    source = Map.fetch!(state.sources, operation.source_uuid)

    case start_task(state, {:rebuild_snapshot, operation.source_uuid}, fn ->
           fetch_snapshot_page(source, operation.cursor, batch_limit(state.definition))
         end) do
      {:ok, next, _ref} -> next
      {:error, error, next} -> handle_failure(next, operation.source_uuid, error)
    end
  end

  defp run_operation_phase(%{operation: %{kind: :rebuild, phase: :prune} = operation} = state) do
    request = %{
      materialization_id: state.metadata.materialization_id,
      source_database_uuid: operation.source_uuid,
      generation: operation.generation,
      after_document_id: operation.prune_cursor,
      limit: batch_limit(state.definition)
    }

    case destination_command(state.uuid, :prune_derived_rebuild_stale_page, request) do
      {:ok, %{has_more: true, next_after_document_id: next_after}} ->
        request_work(%{state | operation: %{operation | prune_cursor: next_after}})

      {:ok, %{has_more: false}} ->
        request_work(%{state | operation: %{operation | phase: :target, prune_cursor: nil}})

      {:error, error} ->
        handle_failure(state, operation.source_uuid, error)
    end
  end

  defp run_operation_phase(%{operation: %{kind: :rebuild, phase: :target} = operation} = state) do
    case start_task(state, {:rebuild_target, operation.source_uuid}, fn ->
           source_identity(operation.source_uuid)
         end) do
      {:ok, next, _ref} -> next
      {:error, error, next} -> handle_failure(next, operation.source_uuid, error)
    end
  end

  defp run_operation_phase(%{operation: %{kind: :rebuild, phase: :replay} = operation} = state) do
    if operation.replay_cursor >= operation.target_sequence and not operation.replay_has_more do
      request_work(%{state | operation: %{operation | phase: :finish}})
    else
      start_replay_task(state, operation)
    end
  end

  defp run_operation_phase(%{operation: %{kind: :rebuild, phase: :finish} = operation} = state) do
    request = %{
      materialization_id: state.metadata.materialization_id,
      source_database_uuid: operation.source_uuid,
      generation: operation.generation,
      catchup_sequence: operation.catchup_sequence,
      source_history_epoch: operation.history_epoch
    }

    case destination_command(state.uuid, :finish_derived_source_rebuild, request) do
      {:ok, _result} ->
        complete_rebuild(state)

      {:error, error} ->
        handle_failure(state, operation.source_uuid, error)
    end
  end

  defp start_replay_task(state, operation) do
    case start_task(state, {:rebuild_replay, operation.source_uuid}, fn ->
           fetch_incremental(
             %{
               source_database_uuid: operation.source_uuid,
               checkpoint_sequence: operation.replay_cursor
             },
             state.definition
           )
         end) do
      {:ok, next, _ref} -> next
      {:error, error, next} -> handle_failure(next, operation.source_uuid, error)
    end
  end

  defp apply_replay_page(state, operation, data) do
    %ReadChanges{results: results, last_sequence: last_sequence, has_more: has_more?} = data.changes
    next_cursor = max(operation.replay_cursor, last_sequence)

    with {:ok, rows, removals} <- changes_to_rows(state.definition, data.changes, data.envelopes),
         {:ok, next_state} <-
           apply_replay_contribution_page(
             state,
             operation,
             rows,
             removals,
             next_cursor,
             results,
             has_more?,
             data.identity.history_epoch
           ) do
      {:noreply, next_state}
    else
      {:error, error} -> {:noreply, handle_failure(state, operation.source_uuid, error)}
    end
  end

  defp apply_replay_contribution_page(
         state,
         operation,
         rows,
         removals,
         next_cursor,
         results,
         has_more?,
         history_epoch
       ) do
    if next_cursor == operation.replay_cursor and results == [] do
      {:error,
       Error.internal_error("source changes replay made no progress", %{
         source_database_uuid: operation.source_uuid
       })}
    else
      apply_replay_transaction(
        state,
        operation,
        rows,
        removals,
        next_cursor,
        has_more?,
        history_epoch
      )
    end
  end

  defp apply_replay_transaction(
         state,
         operation,
         rows,
         removals,
         next_cursor,
         has_more?,
         history_epoch
       ) do
    request = rebuild_page_request(state, operation, rows, removals, nil)
    request = Map.put(request, :catchup_sequence, next_cursor)

    with {:ok, _result} <- destination_command(state.uuid, :apply_derived_rebuild_page, request) do
      source = Map.fetch!(state.sources, operation.source_uuid)
      source = %{source | rebuild_catchup_sequence: next_cursor}
      operation = next_replay_operation(operation, next_cursor, has_more?, history_epoch)

      {:ok,
       request_work(%{
         state
         | sources: Map.put(state.sources, operation.source_uuid, source),
           operation: operation
       })}
    end
  end

  defp next_replay_operation(operation, next_cursor, has_more?, history_epoch) do
    operation = %{
      operation
      | replay_cursor: next_cursor,
        catchup_sequence: next_cursor,
        history_epoch: history_epoch
    }

    cond do
      has_more? -> %{operation | replay_has_more: true}
      next_cursor >= operation.target_sequence -> %{operation | phase: :finish}
      true -> %{operation | phase: :target}
    end
  end

  defp complete_rebuild(state) do
    case load_context(state.uuid) do
      {:ok, metadata, sources} ->
        state = put_context(%{state | operation: nil}, metadata, sources)

        case configure_subscriptions(state) do
          {:ok, next} -> request_work(reset_retry(next))
          {:error, source_uuid, error, next} -> handle_failure(next, source_uuid, error)
        end

      {:error, error} ->
        handle_failure(%{state | operation: nil}, nil, error)
    end
  end

  defp apply_active_results(state, results) do
    ordered_sources = Enum.sort_by(Map.values(results), & &1.source.source_ordinal)
    state = %{state | operation: nil}

    case Enum.reduce_while(ordered_sources, {:ok, state}, &reduce_active_source/2) do
      {:ok, state} ->
        state = %{state | status: :ready, retry_attempt: 0}

        if state.work_requested,
          do: request_work(%{state | work_requested: false}),
          else: state

      {:error, source_uuid, error, state} ->
        handle_failure(state, source_uuid, error)
    end
  end

  defp reduce_active_source(result, {:ok, state}) do
    case apply_active_source(state, result) do
      {:ok, next} -> {:cont, {:ok, next}}
      {:error, source_uuid, error, next} -> {:halt, {:error, source_uuid, error, next}}
    end
  end

  defp apply_active_source(state, %{
         source: source,
         identity: identity,
         changes: changes,
         envelopes: envelopes
       }) do
    with {:ok, rows, removals} <- changes_to_rows(state.definition, changes, envelopes),
         {:ok, applied} <-
           destination_command(
             state.uuid,
             :apply_derived_source_batch,
             %{
               materialization_id: state.metadata.materialization_id,
               source_database_uuid: source.source_database_uuid,
               source_history_epoch: identity.history_epoch,
               expected_checkpoint_sequence: source.checkpoint_sequence,
               through_sequence: changes.last_sequence,
               rows: rows,
               removals: removals
             }
           ),
         {:ok, next} <- update_active_source(state, source, applied),
         {:ok, next} <-
           replace_subscription(next, source.source_database_uuid, applied.checkpoint_sequence) do
      next =
        if changes.has_more or identity.current_sequence > applied.checkpoint_sequence,
          do: %{next | work_requested: true},
          else: next

      {:ok, next}
    else
      {:error, error} -> {:error, source.source_database_uuid, error, state}
    end
  end

  defp update_active_source(state, source, result) do
    source =
      Map.merge(source, %{
        state: :active,
        checkpoint_sequence: result.checkpoint_sequence,
        history_epoch: result.history_epoch
      })

    {:ok, %{state | sources: Map.put(state.sources, source.source_database_uuid, source)}}
  end

  defp start_rebuild(state, source) do
    if source.state == :rebuilding and
         not MapSet.member?(state.needs_rebuild, source.source_database_uuid) do
      state = %{state | operation: rebuild_operation(source)}
      run_operation_phase(state)
    else
      state =
        %{state | operation: %{kind: :begin_rebuild, source_uuid: source.source_database_uuid}}

      case start_task(state, {:begin_rebuild, source.source_database_uuid}, fn ->
             source_identity(source.source_database_uuid)
           end) do
        {:ok, next, _ref} -> next
        {:error, error, next} -> handle_failure(next, source.source_database_uuid, error)
      end
    end
  end

  defp rebuild_operation(source) do
    start =
      source.rebuild_start_sequence || source.rebuild_catchup_sequence || source.checkpoint_sequence

    catchup = source.rebuild_catchup_sequence || start

    %{
      kind: :rebuild,
      source_uuid: source.source_database_uuid,
      generation: source.rebuild_generation,
      phase: :snapshot,
      cursor: source.rebuild_after_document_id,
      prune_cursor: nil,
      replay_cursor: start,
      start_sequence: start,
      catchup_sequence: catchup,
      target_sequence: nil,
      replay_has_more: false,
      history_epoch: nil
    }
  end

  defp fetch_incremental(source, definition) do
    with {:ok, identity} <- source_identity(source.source_database_uuid),
         {:ok, changes} <-
           Changes.read(
             source.source_database_uuid,
             %{
               since: source.checkpoint_sequence,
               limit: source_batch_limit(definition, identity),
               wait_ms: 0
             },
             admission_class: :maintenance
           ),
         {:ok, envelopes} <- fetch_envelopes(source.source_database_uuid, changes.results) do
      {:ok, %{source: source, identity: identity, changes: changes, envelopes: envelopes}}
    end
  end

  defp fetch_snapshot_page(source, cursor, limit) do
    with {:ok, _identity} <- source_identity(source.source_database_uuid),
         {:ok, page} <-
           DatabaseCatalog.command_as(
             source.source_database_uuid,
             :maintenance,
             {:command, :read_winning_documents_page,
              %{"after_document_id" => cursor, "limit" => limit}}
           ) do
      {:ok, %{documents: page.documents, next_after: page.next_after}}
    end
  end

  defp source_identity(source_uuid) do
    case DatabaseCatalog.command_as(source_uuid, :maintenance, {:command, :identity, %{}}) do
      {:ok, %{database_kind: :ordinary} = identity} ->
        {:ok, identity}

      {:ok, %{"database_kind" => "ordinary"} = identity} ->
        {:ok, identity}

      {:ok, %{database_kind: :derived}} ->
        {:error, Error.invalid_request("derived materializer sources must be ordinary databases")}

      {:ok, %{"database_kind" => "derived"}} ->
        {:error, Error.invalid_request("derived materializer sources must be ordinary databases")}

      {:ok, _identity} ->
        {:error, Error.integrity_violation("source database kind is missing")}

      {:error, _} = error ->
        error
    end
  end

  defp fetch_envelopes(_source_uuid, []), do: {:ok, []}

  defp fetch_envelopes(source_uuid, changes) do
    requests =
      changes
      |> Enum.reject(& &1.deleted)
      |> Enum.map(fn change ->
        %{document_id: change.document_id, revision_id: change.winning_revision}
      end)

    if requests == [] do
      {:ok, []}
    else
      DatabaseCatalog.command_as(
        source_uuid,
        :maintenance,
        {:command, :get_revisions_batch, %{requests: requests}}
      )
    end
  end

  defp documents_to_rows(definition, documents) do
    Enum.reduce_while(documents, {:ok, [], []}, fn document, {:ok, rows, removals} ->
      case Program.map(
             definition,
             document["document_id"],
             document["revision_id"],
             document["body"] || %{}
           ) do
        {:ok, :remove} -> {:cont, {:ok, rows, [document["document_id"] | removals]}}
        {:ok, row} -> {:cont, {:ok, [public_row(row) | rows], removals}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> sort_rows_result()
  end

  defp changes_to_rows(definition, %ReadChanges{results: changes}, envelopes),
    do: changes_to_rows(definition, changes, envelopes)

  defp changes_to_rows(definition, changes, envelopes) when is_list(changes) do
    envelope_by_id = Map.new(envelopes, &{&1.id, &1})

    changes
    |> Map.new(&{&1.document_id, &1})
    |> Map.values()
    |> Enum.sort_by(& &1.document_id)
    |> Enum.reduce_while({:ok, [], []}, fn change, acc ->
      map_change(definition, envelope_by_id, change, acc)
    end)
    |> sort_rows_result()
  end

  defp map_change(
         _definition,
         _envelope_by_id,
         %{deleted: true, document_id: document_id},
         {:ok, rows, removals}
       ),
       do: {:cont, {:ok, rows, [document_id | removals]}}

  defp map_change(definition, envelope_by_id, change, {:ok, rows, removals}) do
    case Map.get(envelope_by_id, change.document_id) do
      nil -> {:halt, {:error, Error.integrity_violation("change entry has no revision envelope")}}
      envelope -> map_envelope_change(definition, envelope, rows, removals)
    end
  end

  defp map_envelope_change(definition, envelope, rows, removals) do
    case Program.map(definition, envelope.id, envelope.revision, envelope.body || %{}) do
      {:ok, :remove} -> {:cont, {:ok, rows, [envelope.id | removals]}}
      {:ok, row} -> {:cont, {:ok, [public_row(row) | rows], removals}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp sort_rows_result({:ok, rows, removals}) do
    {:ok, Enum.sort_by(rows, & &1.source_document_id), Enum.sort(removals)}
  end

  defp sort_rows_result({:error, _} = error), do: error

  defp public_row(%{document_id: document_id, revision_id: revision_id, key: key} = row) do
    base = %{source_document_id: document_id, source_revision_id: revision_id, key: key}

    case Map.fetch(row, :value) do
      {:ok, value} -> Map.put(base, :value, value)
      :error -> base
    end
  end

  defp rebuild_page_request(state, operation, rows, removals, cursor) do
    %{
      materialization_id: state.metadata.materialization_id,
      source_database_uuid: operation.source_uuid,
      generation: operation.generation,
      after_document_id: cursor,
      catchup_sequence: operation.catchup_sequence,
      rows: rows,
      removals: removals
    }
  end

  defp destination_command(uuid, command, request),
    do: DatabaseCatalog.command_as(uuid, :maintenance, {:command, command, request})

  defp start_task(state, token, fun) do
    with {:ok, supervisor} <- Supervisor.task_supervisor_pid(state.uuid) do
      task = Task.Supervisor.async_nolink(supervisor, fun)

      {:ok,
       %{
         state
         | tasks: Map.put(state.tasks, task.ref, %{token: token, task: task}),
           work_requested: false
       }, task.ref}
    end
    |> case do
      {:ok, _state, _ref} = ok -> ok
      {:error, error} -> {:error, error, state}
    end
  end

  defp task_ref_for_source(operation, source_uuid) do
    case Enum.find(operation.in_flight, fn {_ref, source} ->
           source.source_database_uuid == source_uuid
         end) do
      {ref, _source} -> ref
      nil -> nil
    end
  end

  defp handle_failure(state, source_uuid, %Error{} = error) do
    state = cancel_tasks(%{state | operation: nil})

    cond do
      error.code in [:history_truncated, :source_history_reset] and is_binary(source_uuid) ->
        log_failure(state, source_uuid, error)

        state
        |> Map.update!(:needs_rebuild, &MapSet.put(&1, source_uuid))
        |> Map.put(:status, :rebuilding)
        |> request_work()

      error.code == :revision_conflict ->
        resynchronize_after_conflict(state)

      true ->
        log_failure(state, source_uuid, error)

        state
        |> maybe_remove_subscription(source_uuid)
        |> schedule_retry()
    end
  end

  defp handle_failure(state, source_uuid, reason),
    do: handle_failure(state, source_uuid, task_error(reason))

  defp maybe_remove_subscription(state, source_uuid) when is_binary(source_uuid),
    do: remove_subscription(state, source_uuid)

  defp maybe_remove_subscription(state, _source_uuid), do: state

  defp resynchronize_after_conflict(state) do
    case load_context(state.uuid) do
      {:ok, metadata, sources} ->
        state = put_context(state, metadata, sources)

        case configure_subscriptions(state) do
          {:ok, next} -> request_work(next)
          {:error, source_uuid, error, next} -> handle_failure(next, source_uuid, error)
        end

      {:error, error} ->
        log_failure(state, nil, error)
        schedule_retry(state)
    end
  end

  defp log_failure(state, source_uuid, error) do
    Logger.warning(
      "derived materializer work failed database=#{state.uuid} source=#{source_uuid || "none"} code=#{error.code} error=#{error.message}"
    )
  end

  defp schedule_retry(state) do
    if is_reference(state.retry_timer) do
      state
    else
      attempt = state.retry_attempt + 1
      delay = retry_delay(state, attempt)
      %{state | retry_attempt: attempt, retry_timer: Process.send_after(self(), :retry_work, delay)}
    end
  end

  defp retry_delay(state, attempt) do
    options = if is_map(state.definition), do: state.definition.options, else: %{}
    base = Map.get(options, "retry_base_delay_ms", 500)
    maximum = Map.get(options, "retry_max_delay_ms", 30_000)
    min(maximum, base * Integer.pow(2, min(attempt - 1, 16)))
  end

  defp reset_retry(state) do
    state
    |> cancel_retry_timer()
    |> Map.put(:retry_attempt, 0)
  end

  defp cancel_retry_timer(state) do
    if is_reference(state.retry_timer), do: Process.cancel_timer(state.retry_timer)
    %{state | retry_timer: nil}
  end

  defp cancel_tasks(state) do
    Enum.each(state.tasks, fn {_ref, %{task: task}} ->
      _ = Task.shutdown(task, :brutal_kill)
    end)

    %{state | tasks: %{}}
  end

  defp request_work(state) do
    cond do
      is_reference(state.retry_timer) ->
        state

      state.work_queued ->
        state

      map_size(state.tasks) > 0 ->
        %{state | work_requested: true}

      true ->
        send(self(), :work)
        %{state | work_queued: true}
    end
  end

  defp enabled?(%{metadata: %{enabled: enabled}}), do: enabled
  defp enabled?(_state), do: false

  defp source_since(%{
         state: :rebuilding,
         rebuild_start_sequence: start,
         rebuild_catchup_sequence: catchup
       }),
       do: start || catchup || 0

  defp source_since(%{checkpoint_sequence: checkpoint}), do: checkpoint

  defp batch_limit(definition), do: Map.get(definition.options, "batch_documents", 100)

  defp source_batch_limit(definition, identity) do
    source_limit = get_in(identity, [:config, "changes", "max_batch"])
    min(batch_limit(definition), source_limit || batch_limit(definition))
  end

  defp max_concurrent_sources(state),
    do: Map.get(state.definition.options, "max_concurrent_sources", 1)

  defp operation_name(nil), do: nil
  defp operation_name(%{kind: :active}), do: :active
  defp operation_name(%{kind: :begin_rebuild}), do: :rebuilding
  defp operation_name(%{kind: :rebuild}), do: :rebuilding

  defp task_error(%Error{} = error), do: error

  defp task_error(reason),
    do: Error.database_unavailable("derived source task stopped", %{cause: inspect(reason)})
end
