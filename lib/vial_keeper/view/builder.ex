defmodule VialKeeper.View.Builder do
  @moduledoc """
  Durable incremental worker for one declarative view.

  Rebuilds and incremental catch-up run exclusively through maintenance
  admission via `DatabaseCatalog.command_as/4`.
  """
  use GenServer

  alias VialKeeper.Changes
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Observability.Instrumentation.View, as: ViewInstrumentation
  alias VialKeeper.Runtime.{ChangeNotifier, ChildSpec, DatabaseCatalog}
  alias VialKeeper.Storage.Results.ReadChanges
  alias VialKeeper.View.{Definition, Document}

  @default_page_limit 100

  @spec child_spec(keyword()) :: map()
  def child_spec(opts) do
    uuid = Keyword.fetch!(opts, :uuid)
    view_id = Keyword.fetch!(opts, :view_id)

    ChildSpec.worker(
      {:view_builder, uuid, view_id},
      {__MODULE__, :start_link, [opts]},
      :temporary
    )
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    uuid = Keyword.fetch!(opts, :uuid)
    view_id = Keyword.fetch!(opts, :view_id)

    GenServer.start_link(__MODULE__, {uuid, view_id, Keyword.get(opts, :page_size)},
      name: via(uuid, view_id)
    )
  end

  @spec via(binary(), binary()) :: {:via, module(), term()}
  def via(uuid, view_id),
    do: {:via, Registry, {VialKeeper.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}}}

  @spec await_sequence(binary(), binary(), non_neg_integer(), timeout()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def await_sequence(uuid, view_id, target, timeout \\ 5_000)
      when is_binary(uuid) and is_binary(view_id) and is_integer(target) and target >= 0 do
    deadline = System.monotonic_time(:millisecond) + timeout

    await_sequence_loop(uuid, view_id, target, deadline)
  end

  defp await_sequence_loop(uuid, view_id, target, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      {:error,
       VialKeeper.Error.view_not_caught_up("view did not catch up before the wait deadline", %{
         target_sequence: target
       })}
    else
      case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
        [{pid, _}] ->
          try do
            GenServer.call(pid, {:await_sequence, target}, remaining)
          catch
            :exit, {:timeout, _} ->
              {:error,
               VialKeeper.Error.view_not_caught_up(
                 "view did not catch up before the wait deadline",
                 %{
                   target_sequence: target
                 }
               )}

            :exit, {:noproc, _} ->
              Process.sleep(min(remaining, 10))
              await_sequence_loop(uuid, view_id, target, deadline)
          end

        [] ->
          Process.sleep(min(remaining, 10))
          await_sequence_loop(uuid, view_id, target, deadline)
      end
    end
  end

  @spec request_rebuild(binary(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def request_rebuild(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
      [{pid, _}] ->
        GenServer.cast(pid, :request_rebuild)
        :ok

      [] ->
        {:error,
         VialKeeper.Error.view_not_found("view builder is not running", %{view_id: view_id})}
    end
  end

  @spec request_catch_up(binary(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def request_catch_up(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
      [{pid, _}] ->
        GenServer.cast(pid, :catch_up)
        :ok

      [] ->
        {:error,
         VialKeeper.Error.view_not_found("view builder is not running", %{view_id: view_id})}
    end
  end

  @impl true
  def init({uuid, view_id, page_size}) do
    ViewInstrumentation.builder_started(uuid)
    send(self(), :bootstrap)
    {:ok, initial_state(uuid, view_id, page_size)}
  end

  @impl true
  def handle_call({:await_sequence, target}, _from, %{indexed_through: current} = state)
      when current >= target do
    {:reply, :ok, notify_sequence_waiters(state, current)}
  end

  def handle_call({:await_sequence, target}, from, state) do
    waiters = Map.update(state.sequence_waiters, target, [from], &[from | &1])
    {:noreply, %{state | sequence_waiters: waiters}}
  end

  @impl true
  def handle_cast(:request_rebuild, state), do: {:noreply, schedule_rebuild(state)}

  def handle_cast(:catch_up, state), do: {:noreply, schedule_work(state)}

  @impl true
  def handle_info(:bootstrap, state) do
    case load_context(state) do
      {:ok, context} ->
        {:noreply, state |> Map.merge(context) |> ensure_notifier_subscription() |> schedule_work()}

      {:error, %VialKeeper.Error{code: :view_not_found}} ->
        {:stop, :normal, state}

      {:error, _error} ->
        Process.send_after(self(), :bootstrap, 100)
        {:noreply, state}
    end
  end

  def handle_info(:work, %{working: true} = state), do: {:noreply, state}

  def handle_info(:work, state) do
    state = %{state | working: true}

    case run_step(state) do
      {:ok, state} ->
        {:noreply, %{state | working: false}}

      {:schedule, state} ->
        send(self(), :work)
        {:noreply, %{state | working: false}}

      {:wait, state} ->
        {:noreply, %{state | working: false}}
    end
  end

  def handle_info({:database_changed, uuid, _sequence}, %{uuid: uuid} = state) do
    {:noreply, schedule_work(state)}
  end

  def handle_info({:database_maintenance, uuid, %{new_floor: floor}}, %{uuid: uuid} = state)
      when is_integer(floor) do
    state =
      case state do
        %{mode: :rebuilding, rebuild: %{s0: s0}} when is_integer(s0) and floor > s0 ->
          ViewInstrumentation.history_truncated(state.uuid)
          schedule_rebuild(state)

        %{indexed_through: cursor} when floor > cursor ->
          ViewInstrumentation.history_truncated(state.uuid)
          schedule_rebuild(state)

        _ ->
          state
      end

    {:noreply, schedule_work(state)}
  end

  def handle_info({:database_maintenance, uuid, _event}, %{uuid: uuid} = state) do
    {:noreply, schedule_work(state)}
  end

  def handle_info({:database_closed, uuid}, %{uuid: uuid} = state) do
    {:stop, :shutdown, unsubscribe_notifier(state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{notifier_ref: ref} = state) do
    {:noreply, %{state | notifier_ref: nil}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp initial_state(uuid, view_id, page_size) do
    %{
      uuid: uuid,
      view_id: view_id,
      definition: nil,
      mode: :booting,
      indexed_through: 0,
      batch_limit: @default_page_limit,
      page_size: page_size,
      rebuild: nil,
      notifier_ref: nil,
      notifier_since: 0,
      sequence_waiters: %{},
      working: false
    }
  end

  defp schedule_work(state) do
    if state.working,
      do: state,
      else:
        (
          send(self(), :work)
          state
        )
  end

  defp schedule_rebuild(state) do
    %{schedule_work(state) | mode: :rebuilding, rebuild: fresh_rebuild_state()}
  end

  defp fresh_rebuild_state do
    %{
      s0: nil,
      generation: nil,
      after_document_id: nil,
      catch_cursor: nil,
      target_sequence: nil
    }
  end

  defp run_step(%{mode: :booting} = state), do: {:schedule, state}

  defp run_step(%{mode: :rebuilding, rebuild: nil} = state),
    do: {:ok, schedule_rebuild(state)}

  defp run_step(%{mode: :rebuilding, rebuild: %{s0: nil} = rebuild} = state) do
    with {:ok, sequence} <- current_sequence(state.uuid),
         :ok <- probe(state, :rebuild_started, %{sequence: sequence}),
         {:ok, rebuild} <- begin_rebuild(state, rebuild, sequence) do
      {:schedule, %{state | rebuild: rebuild}}
    else
      {:error, %VialKeeper.Error{code: :history_truncated}} ->
        {:schedule, schedule_rebuild(state)}

      {:error, _} ->
        {:wait, state}
    end
  end

  defp run_step(
         %{mode: :rebuilding, rebuild: %{generation: generation, catch_cursor: nil} = rebuild} =
           state
       )
       when not is_nil(generation) do
    case scan_snapshot_page(state, rebuild) do
      {:done, rebuild} ->
        {:schedule, %{state | rebuild: %{rebuild | catch_cursor: rebuild.s0}}}

      {:page, rebuild} ->
        {:schedule, %{state | rebuild: rebuild}}

      {:error, %VialKeeper.Error{code: :history_truncated}} ->
        {:schedule, schedule_rebuild(state)}

      {:error, _} ->
        {:wait, state}
    end
  end

  defp run_step(
         %{mode: :rebuilding, rebuild: %{catch_cursor: cursor, generation: generation} = rebuild} =
           state
       )
       when not is_nil(cursor) and not is_nil(generation) do
    with {:ok, target} <- current_sequence(state.uuid),
         state <- ensure_notifier_subscription(state),
         {:ok, rebuild, status} <- replay_rebuild_changes(state, rebuild, cursor, target),
         {:ok, state} <- maybe_finish_rebuild(state, rebuild, target, status == :done) do
      case status do
        :done -> {:ok, state}
        :wait -> {:wait, state}
        :continue -> {:schedule, state}
      end
    else
      {:error, %VialKeeper.Error{code: :history_truncated}} ->
        ViewInstrumentation.history_truncated(state.uuid)
        {:schedule, schedule_rebuild(state)}

      {:error, _} ->
        {:wait, state}
    end
  end

  defp run_step(%{mode: :incremental} = state) do
    cursor = state.indexed_through
    # Race-free: subscribe before re-read so wakeups cannot be missed (Changes pattern).
    state = ensure_notifier_subscription(state)

    case read_changes_batch(state, cursor) do
      {:ok, []} ->
        {:wait, state}

      {:ok, batch} ->
        apply_incremental_batch(state, cursor, batch)

      {:error, %VialKeeper.Error{code: :history_truncated}} ->
        ViewInstrumentation.history_truncated(state.uuid)
        {:schedule, schedule_rebuild(state)}

      {:error, _} ->
        {:wait, state}
    end
  end

  defp maybe_finish_rebuild(state, _rebuild, _target, false), do: {:ok, state}

  defp maybe_finish_rebuild(state, rebuild, target, true) do
    with {:ok, %{indexed_through: indexed_through}} <-
           maintenance(
             state.uuid,
             {:command, :finish_view_rebuild,
              %{
                "view_id" => state.view_id,
                "generation" => rebuild.generation,
                "indexed_through" => target
              }}
           ) do
      probe(state, :generation_activated, %{sequence: target})
      ViewInstrumentation.rebuild_activated(state.uuid)

      state =
        state
        |> Map.put(:indexed_through, indexed_through)
        |> Map.put(:mode, :incremental)
        |> Map.put(:rebuild, nil)
        |> notify_sequence_waiters(indexed_through)
        |> resubscribe_notifier(indexed_through)

      {:ok, state}
    end
  end

  defp replay_rebuild_changes(_state, rebuild, cursor, target) when cursor >= target do
    {:ok, %{rebuild | target_sequence: target}, :done}
  end

  defp replay_rebuild_changes(state, rebuild, cursor, target) do
    case read_changes_since(state.uuid, cursor, state.batch_limit) do
      {:ok, %ReadChanges{results: [], last_sequence: last_sequence}} ->
        replay_when_empty(cursor, rebuild, target, last_sequence)

      {:ok, %ReadChanges{} = batch} ->
        replay_with_results(state, rebuild, cursor, target, batch)

      {:error, _} = error ->
        error
    end
  end

  defp replay_when_empty(cursor, rebuild, target, last_sequence) do
    next_cursor = max(cursor, last_sequence || cursor)

    if next_cursor >= target do
      {:ok, %{rebuild | catch_cursor: next_cursor, target_sequence: target}, :done}
    else
      {:ok, %{rebuild | catch_cursor: next_cursor}, :wait}
    end
  end

  defp replay_with_results(state, rebuild, cursor, target, %ReadChanges{
         results: results,
         last_sequence: last_sequence,
         has_more: has_more?
       }) do
    with {:ok, rows, removals} <- envelopes_to_rows(state, results),
         {:ok, _} <- append_rebuild_page(state, rebuild.generation, rows, removals) do
      next_cursor = last_sequence || cursor
      status = replay_status(next_cursor, target, has_more?)
      {:ok, %{rebuild | catch_cursor: next_cursor, target_sequence: target}, status}
    end
  end

  defp replay_status(next_cursor, target, has_more?) do
    cond do
      next_cursor >= target and not has_more? -> :done
      has_more? -> :continue
      true -> :wait
    end
  end

  defp append_rebuild_page(state, generation, rows, removals \\ []) do
    maintenance(
      state.uuid,
      {:command, :append_view_rebuild_page,
       %{
         "view_id" => state.view_id,
         "generation" => generation,
         "rows" => rows,
         "removals" => removals
       }}
    )
  end

  defp scan_snapshot_page(state, rebuild) do
    request =
      winning_documents_request(rebuild.after_document_id, @default_page_limit, state.page_size)

    with {:ok, %{documents: documents, next_after: next_after}} <-
           maintenance(state.uuid, {:command, :read_winning_documents_page, request}),
         {:ok, rows} <- documents_to_rows(state, documents),
         {:ok, _} <- append_rebuild_page(state, rebuild.generation, rows) do
      probe(state, :snapshot_page_applied, %{count: length(rows), after: rebuild.after_document_id})

      if next_after do
        {:page, %{rebuild | after_document_id: next_after}}
      else
        {:done, rebuild}
      end
    end
  end

  defp begin_rebuild(state, rebuild, sequence) do
    with {:ok, %{building_generation: generation}} <-
           maintenance(
             state.uuid,
             {:command, :begin_view_rebuild,
              %{
                "view_id" => state.view_id,
                "start_sequence" => sequence
              }}
           ) do
      {:ok, %{rebuild | s0: sequence, generation: generation, after_document_id: nil}}
    end
  end

  defp apply_incremental_batch(state, cursor, batch) do
    through = batch_last_sequence(batch)

    probe(state, :before_incremental_apply, %{expected: cursor, through: through})

    with {:ok, rows, removals} <- envelopes_to_rows(state, batch),
         {:ok, %{indexed_through: indexed_through}} <-
           maintenance(
             state.uuid,
             {:command, :apply_view_batch,
              %{
                "view_id" => state.view_id,
                "expected_indexed_through" => cursor,
                "through_sequence" => through,
                "rows" => rows,
                "removals" => removals
              }}
           ) do
      probe(state, :after_incremental_apply, %{indexed_through: indexed_through})
      ViewInstrumentation.batch_applied(state.uuid)

      state =
        state
        |> Map.put(:indexed_through, indexed_through)
        |> notify_sequence_waiters(indexed_through)
        |> resubscribe_notifier(indexed_through)

      if length(batch) >= state.batch_limit,
        do: {:schedule, state},
        else: {:ok, state}
    else
      {:error, %VialKeeper.Error{code: :revision_conflict}} ->
        case view_state(state) do
          {:ok, %{indexed_through: indexed_through}} ->
            {:schedule, %{state | indexed_through: indexed_through}}

          _ ->
            {:wait, state}
        end

      {:error, _} ->
        {:wait, state}
    end
  end

  defp ensure_notifier_subscription(%{notifier_ref: ref} = state) when is_reference(ref), do: state

  defp ensure_notifier_subscription(state) do
    since = state.indexed_through

    case ChangeNotifier.subscribe(state.uuid, since) do
      {:ok, ref, _} -> %{state | notifier_ref: ref, notifier_since: since}
      {:error, _} -> state
    end
  end

  defp read_changes_batch(state, since) do
    case read_changes_since(state.uuid, since, state.batch_limit) do
      {:ok, %ReadChanges{results: results}} -> {:ok, results}
      {:error, _} = error -> error
    end
  end

  defp read_changes_since(uuid, since, limit) do
    Changes.read(uuid, %{since: since, limit: limit, wait_ms: 0}, admission_class: :maintenance)
  end

  defp envelopes_to_rows(state, changes) do
    with {:ok, envelopes} <- fetch_envelopes(state.uuid, changes) do
      envelopes
      |> latest_envelopes_by_document()
      |> Enum.reduce({:ok, [], []}, fn envelope, acc ->
        map_envelope_row(state, envelope, acc)
      end)
    end
  end

  defp latest_envelopes_by_document(envelopes) do
    envelopes
    |> Map.new(&{&1.id, &1})
    |> Map.values()
    |> Enum.sort_by(& &1.id)
  end

  defp map_envelope_row(_state, %{deleted: true, id: id}, {:ok, rows, removals}),
    do: {:ok, rows, [id | removals]}

  defp map_envelope_row(state, envelope, {:ok, rows, removals}) do
    case Document.map(
           state.definition,
           envelope.id,
           envelope.revision,
           envelope.body || %{}
         ) do
      {:ok, :remove} ->
        {:ok, rows, [envelope.id | removals]}

      {:ok, row} ->
        {:ok, [public_row(row) | rows], removals}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp documents_to_rows(state, documents) do
    Enum.reduce(documents, {:ok, []}, fn document, {:ok, acc} ->
      case Document.map(
             state.definition,
             document["document_id"],
             document["revision_id"],
             document["body"] || %{}
           ) do
        {:ok, :remove} -> {:ok, acc}
        {:ok, row} -> {:ok, [public_row(row) | acc]}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp public_row(row) do
    %{
      "document_id" => row.document_id,
      "revision_id" => row.revision_id,
      "key" => row.key,
      "value" => Map.get(row, :value)
    }
  end

  defp fetch_envelopes(uuid, changes) do
    requests =
      Enum.map(changes, fn change ->
        %{
          document_id: change.document_id,
          revision_id: change.winning_revision
        }
      end)

    case maintenance(uuid, {:command, :get_revisions_batch, %{requests: requests}}) do
      {:ok, envelopes} -> {:ok, envelopes}
      {:error, _} = error -> error
    end
  end

  defp load_context(state) do
    with {:ok, view} <- fetch_view_entry(state),
         {:ok, definition} <- decode_definition(view),
         {:ok, view_state} <- view_state(state),
         {:ok, batch_limit} <- batch_limit_for(state.uuid) do
      mode = initial_mode(view_state)
      rebuild = if mode == :rebuilding, do: fresh_rebuild_state(), else: nil

      {:ok,
       %{
         definition: definition,
         indexed_through: view_state.indexed_through,
         batch_limit: batch_limit,
         mode: mode,
         rebuild: rebuild
       }}
    end
  end

  defp initial_mode(%{status: status, building_generation: building_generation}) do
    cond do
      status == "building" -> :rebuilding
      not is_nil(building_generation) -> :rebuilding
      true -> :incremental
    end
  end

  defp fetch_view_entry(state) do
    with {:ok, views} <- maintenance(state.uuid, {:command, :list_views, %{}}) do
      case Enum.find(views, &(&1["view_id"] == state.view_id)) do
        nil ->
          {:error, VialKeeper.Error.view_not_found("view was not found", %{view_id: state.view_id})}

        view ->
          {:ok, view}
      end
    end
  end

  defp decode_definition(%{"definition" => definition}) when is_map(definition) do
    Definition.normalize(definition)
  end

  defp decode_definition(%{"definition_json" => json}) when is_binary(json) do
    with {:ok, decoded} <- StrictDecoder.decode(json) do
      Definition.normalize(decoded)
    end
  end

  defp decode_definition(view) when is_map(view) do
    case Map.get(view, "definition") || Map.get(view, "definition_json") do
      %{} = definition -> Definition.normalize(definition)
      json when is_binary(json) -> decode_definition(%{"definition_json" => json})
      _ -> {:error, VialKeeper.Error.internal_error("view definition is missing")}
    end
  end

  defp view_state(state) do
    maintenance(state.uuid, {:command, :view_state, state.view_id})
  end

  defp batch_limit_for(uuid) do
    case DatabaseCatalog.command_as(uuid, :maintenance, {:command, :identity, %{}}) do
      {:ok, identity} ->
        {:ok, get_in(identity.config, ["views", "batch_changes"]) || @default_page_limit}

      error ->
        error
    end
  end

  defp current_sequence(uuid) do
    case DatabaseCatalog.command_as(uuid, :maintenance, {:command, :identity, %{}}) do
      {:ok, %{current_sequence: sequence}} when is_integer(sequence) -> {:ok, sequence}
      {:ok, identity} -> {:ok, Map.get(identity, :current_sequence, 0)}
      error -> error
    end
  end

  defp maintenance(uuid, command) do
    DatabaseCatalog.command_as(uuid, :maintenance, command)
  end

  defp winning_documents_request(after_id, limit, page_size) do
    base = %{"after_document_id" => after_id, "limit" => limit}

    case page_size do
      size when is_integer(size) and size > 0 ->
        Map.put(base, "options", %{"page_size" => size})

      _ ->
        base
    end
  end

  defp batch_last_sequence([]), do: 0

  defp batch_last_sequence(batch) do
    batch
    |> List.last()
    |> Map.fetch!(:sequence)
  end

  defp notify_sequence_waiters(state, indexed_through) do
    {ready, pending} =
      Enum.split_with(state.sequence_waiters, fn {target, _waiters} -> indexed_through >= target end)

    Enum.each(ready, fn {_target, waiters} ->
      Enum.each(waiters, &GenServer.reply(&1, :ok))
    end)

    %{state | sequence_waiters: Map.new(pending)}
  end

  defp resubscribe_notifier(state, since) do
    state = unsubscribe_notifier(state)
    ensure_notifier_subscription(%{state | indexed_through: since})
  end

  defp unsubscribe_notifier(%{notifier_ref: ref, uuid: uuid} = state) when is_reference(ref) do
    ChangeNotifier.unsubscribe(uuid, ref)
    Process.demonitor(ref, [:flush])
    %{state | notifier_ref: nil}
  end

  defp unsubscribe_notifier(state), do: state

  defp probe(state, event, metadata) do
    case Application.get_env(:vial_keeper, :view_builder_probe) do
      {pid, ref} when is_pid(pid) ->
        send(pid, {:view_builder_probe, ref, state.uuid, state.view_id, event, metadata})

      _ ->
        :ok
    end

    :ok
  end
end
