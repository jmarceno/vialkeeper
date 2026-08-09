defmodule ElixirDB.Replication do
  @moduledoc "One-shot and continuous replication orchestration."
  alias ElixirDB.Changes.Request
  alias ElixirDB.Domain.BoundaryPage
  alias ElixirDB.JSON.{Canonical, Stringify}
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Replication, as: ReplicationModule
  alias ElixirDB.Replication.{CheckpointReconciler, Id, Wire}
  alias ElixirDB.Replication.LocalEndpoint
  alias ElixirDB.Replication.TransferPipeline
  alias ElixirDB.Retention.SafeReport
  @default_batch 100

  @doc "Ordered worker phase states from Plan §7.7 (excluding terminal idle entry)."
  def phases do
    [
      :idle,
      :handshake,
      :install_boundaries,
      :bootstrap,
      :read_changes,
      :diff,
      :transfer,
      :import,
      :checkpoint_target,
      :checkpoint_source,
      :report_peer,
      :waiting,
      :backoff,
      :completed,
      :failed
    ]
  end

  def one_shot(source_uuid, target_uuid, options \\ %{}) do
    with {:ok, source} <- LocalEndpoint.new(source_uuid),
         {:ok, target} <- LocalEndpoint.new(target_uuid) do
      run(source, target, options)
    end
  end

  def one_shot_endpoints(source, target, options \\ %{}), do: run(source, target, options)

  @doc "Run one captured batch sequence, or keep waiting when mode is continuous."
  def run(source, target, options \\ %{}) do
    session_id = option(options, :session_id, ElixirDB.UUID.v4())
    options = put_option(options, :session_id, session_id)

    with {:ok, context} <- handshake(source, target, options) do
      process_from_handshake(source, target, context, options)
    end
  end

  @doc "Handshake: identities, compatibility, replication id, checkpoint reconciliation."
  def handshake(source, target, options) do
    with :ok <- phase_hook(options, :handshake, %{}),
         {:ok, source_identity} <- endpoint_call(source, :identity, []),
         {:ok, target_identity} <- endpoint_call(target, :identity, []),
         :ok <- compatible(source_identity, target_identity),
         {:ok, replication_id} <-
           Id.calculate(
             source_uuid(source_identity),
             source_uuid(target_identity),
             option(options, :direction, "push"),
             option(options, :mode, "one_shot")
           ),
         {:ok, source_checkpoint} <- endpoint_call(source, :get_checkpoint, [replication_id]),
         {:ok, target_checkpoint} <- endpoint_call(target, :get_checkpoint, [replication_id]),
         {:ok, target_boundary_state} <-
           endpoint_call(target, :get_local_record, [
             "retention_boundary_state",
             source_uuid(source_identity)
           ]),
         {:ok, peer_record} <-
           endpoint_call(source, :get_local_record, [
             "peer_ledger",
             source_uuid(target_identity)
           ]),
         reconcile <-
           CheckpointReconciler.reconcile(
             value(source_checkpoint),
             value(target_checkpoint),
             source_identity
           ),
         terminal <- get(source_identity, :current_sequence) || 0 do
      context = %{
        session_id: option(options, :session_id, ElixirDB.UUID.v4()),
        replication_id: replication_id,
        since: reconcile.since,
        terminal: terminal,
        bootstrap_required:
          reconcile.bootstrap_required or
            peer_history_replacement_required?(peer_record, target_identity),
        reconcile_reason:
          if(peer_history_replacement_required?(peer_record, target_identity),
            do: :peer_history_reset,
            else: reconcile.reason
          ),
        source_identity: source_identity,
        target_identity: target_identity,
        source_checkpoint: source_checkpoint,
        target_checkpoint: target_checkpoint,
        installed_source_compaction_epoch: boundary_state_epoch(target_boundary_state) || 0,
        installed_source_boundary_digest: boundary_state_digest(target_boundary_state),
        boundaries_installed_through: boundary_state_epoch(target_boundary_state) || 0,
        selected: [],
        documents: [],
        chains: [],
        imported: nil,
        bootstrap_cursor: nil,
        bootstrap_applied: false,
        boundary_refresh_required:
          boundary_refresh_required?(source_identity, target_checkpoint, target_boundary_state),
        peer_history_replacement_required:
          peer_history_replacement_required?(peer_record, target_identity)
      }

      with :ok <- phase_hook(options, :after_handshake, context) do
        {:ok, context}
      end
    end
  end

  @doc "Install source retention boundaries on the target before acknowledging epochs."
  def install_boundaries(source, target, context, options) do
    with :ok <- phase_hook(options, :install_boundaries, context),
         {:ok, installed_through} <- install_boundary_pages(source, target, context) do
      context =
        context
        |> Map.put(:boundaries_installed_through, installed_through)
        |> Map.put(:installed_source_compaction_epoch, installed_through)
        |> Map.put(:boundary_refresh_required, false)
        |> Map.put(
          :installed_source_boundary_digest,
          get(context.source_identity, :retention_boundary_digest)
        )

      with :ok <- phase_hook(options, :after_install_boundaries, context) do
        {:ok, context}
      end
    end
  end

  @doc "Paginated snapshot/bootstrap transfer from source to target."
  def bootstrap(source, target, context, options) do
    with :ok <- phase_hook(options, :bootstrap, context),
         {:ok, context} <- bootstrap_pages(source, target, context, options) do
      with :ok <- phase_hook(options, :after_bootstrap, context) do
        {:ok, context}
      end
    end
  end

  @doc "Read a bounded change batch from the source after `context.since`."
  def read_changes(source, context, options) do
    limit = option(options, :batch, @default_batch)
    wait_ms = option(options, :wait_ms_for_read, 0)

    request = %Request{since: context.since, limit: limit, wait_ms: wait_ms}

    with :ok <- phase_hook(options, :read_changes, context),
         changes_result <- endpoint_call(source, :read_changes, [request]),
         {:ok, context} <- apply_read_changes_result(changes_result, context) do
      finalize_read_changes(source, context, options)
    end
  end

  defp finalize_read_changes(_source, %{bootstrap_required: true} = context, options) do
    case phase_hook(options, :after_read_changes, context) do
      :ok -> {:ok, context}
      {:error, error} -> {:error, error}
    end
  end

  defp finalize_read_changes(_source, context, options) do
    with {:ok, selected} <-
           take_batch_bytes(
             terminal_changes(%{results: context.selected}, context.terminal),
             options
           ),
         :ok <- ensure_progress(selected, context.since, context.terminal) do
      after_read_changes(%{context | selected: selected}, options)
    end
  end

  defp after_read_changes(context, options) do
    case phase_hook(options, :after_read_changes, context) do
      :ok -> {:ok, context}
      {:error, error} -> {:error, error}
    end
  end

  @doc "Diff selected change leaves against the target."
  def diff(target, context, options) do
    with :ok <- phase_hook(options, :diff, context),
         {:ok, documents} <- target_diff(target, context) do
      context = %{context | documents: documents}

      with :ok <- phase_hook(options, :after_diff, context) do
        {:ok, context}
      end
    end
  end

  @doc "Fetch revision chains and transfer their missing attachment blobs."
  def transfer(source, target, context, options) do
    with :ok <- phase_hook(options, :transfer, context),
         {:ok, context} <-
           TransferPipeline.run(source, target, context, transfer_config(options)),
         :ok <- phase_hook(options, :after_transfer, context) do
      {:ok, context}
    end
  end

  @doc "Import chains into the target and confirm durable commit (REPL-004)."
  def import_chains(target, context, options) do
    with :ok <- phase_hook(options, :import, context),
         {:ok, imported} <-
           endpoint_call(target, :import_revision_chains, [%{chains: context.chains}]),
         {:ok, _confirmed} <-
           endpoint_call(target, :confirm_durable_commit, [%{imported: imported}]) do
      context = %{context | imported: imported}

      with :ok <- phase_hook(options, :after_import, context) do
        {:ok, context}
      end
    end
  end

  @doc "CAS-write the target checkpoint for the current batch sequence."
  def checkpoint_target(source, target, context, options) do
    with :ok <- phase_hook(options, :checkpoint_target, context),
         {:ok, prepared} <- prepare_checkpoint(source, target, context, options),
         {:ok, target_result} <-
           ReplicationModule.checkpoint_span(
             context.replication_id,
             :target,
             fn ->
               endpoint_call(target, :put_checkpoint, [
                 context.replication_id,
                 prepared.target_request
               ])
             end
           ) do
      context =
        Map.merge(context, %{
          checkpoint_prepared: prepared,
          target_checkpoint_result: target_result,
          source_checkpoint: prepared.source_current,
          target_checkpoint: prepared.target_current
        })

      with :ok <- phase_hook(options, :after_checkpoint_target, context) do
        {:ok, context}
      end
    end
  end

  @doc "CAS-write the source checkpoint after the target checkpoint succeeded."
  def checkpoint_source(source, context, options) do
    prepared = Map.fetch!(context, :checkpoint_prepared)

    with :ok <- phase_hook(options, :checkpoint_source, context),
         {:ok, source_result} <-
           ReplicationModule.checkpoint_span(
             context.replication_id,
             :source,
             fn ->
               endpoint_call(source, :put_checkpoint, [
                 context.replication_id,
                 prepared.source_request
               ])
             end
           ) do
      next = prepared.sequence

      context =
        context
        |> Map.put(:since, next)
        |> Map.put(:safe_source_sequence, prepared.safe_source_sequence)
        |> Map.put(:source_checkpoint_result, source_result)
        |> Map.put(:selected, [])
        |> Map.put(:documents, [])
        |> Map.put(:chains, [])
        |> Map.put(:imported, nil)
        |> Map.delete(:checkpoint_prepared)
        |> Map.delete(:target_checkpoint_result)

      with :ok <- phase_hook(options, :after_checkpoint_source, context) do
        {:ok, context}
      end
    end
  end

  @doc "Report the target safe position and renew the peer lease on the source."
  def report_peer(source, target, context, options) do
    target_uuid = source_uuid(context.target_identity || target)

    with :ok <- phase_hook(options, :report_peer, context),
         {:ok, _result} <- report_peer_position(source, target, context, options),
         :ok <- clear_pending_local_causal(source, target_uuid) do
      with :ok <- phase_hook(options, :after_report_peer, context) do
        {:ok, context}
      end
    end
  end

  @doc "Wait for source changes beyond the current checkpoint (continuous mode)."
  def wait_for_changes(source, context, options) do
    with :ok <- phase_hook(options, :waiting, context),
         {:ok, source_identity} <- endpoint_call(source, :identity, []),
         reconcile <-
           CheckpointReconciler.reconcile(
             value(context.source_checkpoint),
             value(context.target_checkpoint),
             source_identity
           ),
         context <-
           maybe_mark_bootstrap(context, reconcile, source_identity),
         {:ok, changes} <-
           endpoint_call(source, :read_changes, [
             %Request{
               since: context.since,
               limit: option(options, :batch, @default_batch),
               wait_ms: option(options, :wait_ms, 1_000)
             }
           ]) do
      selected = get(changes, :results) || []

      terminal =
        case endpoint_call(source, :identity, []) do
          {:ok, identity} -> get(identity, :current_sequence) || context.since
          _ -> context.since
        end

      context =
        context
        |> Map.put(:terminal, terminal)
        |> Map.put(:selected, selected)
        |> Map.put(:source_identity, source_identity)

      with :ok <- phase_hook(options, :after_waiting, context) do
        {:ok, context}
      end
    end
  end

  @doc "Advance after checkpoint_source: more batches, waiting, or completed."
  def next_after_checkpoint(context, options) do
    continuous? = option(options, :mode, "one_shot") in ["continuous", :continuous]

    cond do
      context.bootstrap_required ->
        {:bootstrap, context}

      context.since < context.terminal ->
        {:continue_batch, context}

      continuous? ->
        {:waiting, context}

      true ->
        {:completed,
         %{
           status: :completed,
           replication_id: context.replication_id,
           source_sequence: context.since
         }}
    end
  end

  @doc "Whether handshake found the source already caught up to the terminal sequence."
  def caught_up?(%{since: since, terminal: terminal, bootstrap_required: false}),
    do: since >= terminal

  def caught_up?(%{bootstrap_required: true}), do: false
  def caught_up?(%{since: since, terminal: terminal}), do: since >= terminal

  defp process_from_handshake(source, target, context, options) do
    cond do
      context.bootstrap_required ->
        finish_bootstrap(source, target, context, options)

      context.boundary_refresh_required ->
        finish_boundary_refresh(source, target, context, options)

      caught_up?(context) ->
        checkpoint_caught_up(source, target, context, options)

      true ->
        process_batches(source, target, context, options)
    end
  end

  defp finish_boundary_refresh(source, target, context, options) do
    with {:ok, context} <- install_boundaries(source, target, context, options) do
      context = %{context | boundary_refresh_required: false}

      if caught_up?(context) do
        checkpoint_caught_up(source, target, context, options)
      else
        process_batches(source, target, context, options)
      end
    end
  end

  defp checkpoint_caught_up(source, target, context, options) do
    with {:ok, context} <- checkpoint_target(source, target, context, options),
         {:ok, context} <- checkpoint_source(source, context, options),
         {:ok, context} <- report_peer(source, target, context, options) do
      continue_after_checkpoint(source, target, context, options)
    end
  end

  defp continue_after_checkpoint(source, target, context, options) do
    handle_next_after_checkpoint(source, target, context, options)
  end

  defp handle_next_after_checkpoint(source, target, context, options) do
    case next_after_checkpoint(context, options) do
      {:completed, result} ->
        {:ok, result}

      {:waiting, context} ->
        wait_loop(source, target, context, options)

      {:continue_batch, context} ->
        process_batches(source, target, context, options)

      {:bootstrap, context} ->
        finish_bootstrap(source, target, context, options)
    end
  end

  defp process_batches(source, target, context, options) do
    with {:ok, context} <- read_changes(source, context, options) do
      if context.bootstrap_required do
        bootstrap_after_read(source, target, context, options)
      else
        continue_batch_after_read(source, target, context, options)
      end
    end
  end

  defp continue_batch_after_read(source, target, context, options) do
    with {:ok, context} <- diff(target, context, options),
         {:ok, context} <- transfer(source, target, context, options),
         {:ok, context} <- import_chains(target, context, options),
         {:ok, context} <- checkpoint_target(source, target, context, options),
         {:ok, context} <- checkpoint_source(source, context, options),
         {:ok, context} <- report_peer(source, target, context, options) do
      handle_next_after_checkpoint(source, target, context, options)
    end
  end

  defp bootstrap_after_read(source, target, context, options) do
    finish_bootstrap(source, target, context, options)
  end

  defp wait_loop(source, target, context, options) do
    case wait_for_changes(source, context, options) do
      {:ok, context} ->
        continue_wait_loop(source, target, context, options)

      {:error, error} ->
        {:error, error}
    end
  end

  defp continue_wait_loop(source, target, %{bootstrap_required: true} = context, options) do
    finish_bootstrap(source, target, context, options)
  end

  defp continue_wait_loop(source, target, %{boundary_refresh_required: true} = context, options) do
    finish_boundary_refresh(source, target, context, options)
  end

  defp continue_wait_loop(source, target, context, options) do
    if context.selected == [] and context.terminal <= context.since do
      wait_loop(source, target, context, options)
    else
      process_batches(source, target, %{context | selected: []}, options)
    end
  end

  defp finish_bootstrap(source, target, context, options) do
    with {:ok, context} <- install_boundaries(source, target, context, options),
         {:ok, context} <- bootstrap(source, target, context, options),
         {:ok, context} <- checkpoint_target(source, target, context, options),
         {:ok, context} <- checkpoint_source(source, context, options),
         {:ok, context} <- report_peer(source, target, context, options) do
      handle_next_after_checkpoint(
        source,
        target,
        %{context | bootstrap_required: false},
        options
      )
    end
  end

  defp bootstrap_pages(source, target, context, options) do
    cursor = context.bootstrap_cursor

    with {:ok, page} <-
           endpoint_call(source, :get_revision_chains, [
             %{bootstrap: true, cursor: cursor, limit: option(options, :batch, @default_batch)}
           ]),
         :ok <- verify_bootstrap_identity(context, page),
         {:ok, chains} <- bootstrap_chains(page),
         {:ok, _context} <-
           transfer(
             source,
             target,
             context
             |> Map.put(:documents, [])
             |> Map.put(:chains, chains)
             |> Map.put(:transfer_preloaded, true),
             options
           ),
         {:ok, imported} <-
           endpoint_call(target, :import_revision_chains, [
             %{
               chains: chains,
               purged_boundaries: get(page, :purged_boundaries) || [],
               source_database_uuid: get(context.source_identity, :database_uuid)
             }
           ]),
         {:ok, _confirmed} <-
           endpoint_call(target, :confirm_durable_commit, [%{imported: imported}]) do
      terminal = get(context.source_identity, :current_sequence) || context.terminal
      next_cursor = get(page, :continuation_cursor)

      context =
        context
        |> Map.put(:bootstrap_cursor, next_cursor)
        |> Map.put(:since, get(page, :retention_floor) || context.since)
        |> Map.put(:terminal, terminal)
        |> Map.put(:imported, imported)
        |> Map.put(
          :installed_source_compaction_epoch,
          get(page, :compaction_epoch) || context.installed_source_compaction_epoch
        )
        |> Map.put(
          :boundaries_installed_through,
          max(
            context.boundaries_installed_through,
            get(page, :compaction_epoch) || 0
          )
        )

      if next_cursor do
        bootstrap_pages(source, target, context, options)
      else
        {:ok,
         %{
           context
           | since: terminal,
             bootstrap_required: false,
             bootstrap_applied: true
         }}
      end
    else
      {:error, %ElixirDB.Error{code: :integrity_violation}} ->
        {:error, ElixirDB.Error.rebase_required("bootstrap cannot merge local mutations safely")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp bootstrap_chains(page) do
    case get(page, :chains) do
      chains when is_list(chains) ->
        {:ok, chains}

      nil ->
        {:ok, []}

      _other ->
        {:error, ElixirDB.Error.invalid_request("bootstrap revision chains must be a list")}
    end
  end

  defp verify_bootstrap_identity(context, page) do
    source_epoch = get(context.source_identity, :history_epoch)
    page_epoch = get(page, :source_history_epoch)

    if is_binary(page_epoch) and page_epoch != source_epoch do
      {:error,
       ElixirDB.Error.source_history_reset("bootstrap page history epoch does not match source")}
    else
      :ok
    end
  end

  defp install_boundary_pages(source, target, context) do
    source_identity = context.source_identity
    cursor = nil
    compaction_epoch = get(source_identity, :compaction_epoch) || 0
    history_epoch = get(source_identity, :history_epoch)
    install_id = "#{context.session_id}-boundaries"

    install_boundary_pages_loop(
      source,
      target,
      history_epoch,
      compaction_epoch,
      cursor,
      0,
      install_id
    )
  end

  defp install_boundary_pages_loop(
         source,
         target,
         history_epoch,
         compaction_epoch,
         cursor,
         installed,
         install_id
       ) do
    request = %{
      source_history_epoch: history_epoch,
      compaction_epoch: compaction_epoch,
      cursor: cursor
    }

    with {:ok, page} <- endpoint_call(source, :read_boundary_pages, [request]),
         install_page <- boundary_install_page(page, source, install_id, is_nil(cursor)),
         {:ok, install_result} <- endpoint_call(target, :install_boundary_pages, [install_page]),
         next_cursor <- get(page, :next_page),
         installed_through <-
           max(installed, MapAccess.get(install_result, :compaction_epoch, compaction_epoch)) do
      if next_cursor do
        install_boundary_pages_loop(
          source,
          target,
          history_epoch,
          compaction_epoch,
          next_cursor,
          installed_through,
          install_id
        )
      else
        {:ok, installed_through}
      end
    end
  end

  defp prepare_checkpoint(source, target, context, options) do
    sequence =
      case context.selected do
        [_ | _] = selected -> get(List.last(selected), :sequence)
        _ -> context.since
      end

    documents = length(context.selected)
    imported = context.imported
    source_identity = context.source_identity || %{}

    with {:ok, source_current} <- endpoint_call(source, :get_checkpoint, [context.replication_id]),
         {:ok, target_current} <- endpoint_call(target, :get_checkpoint, [context.replication_id]),
         source_value <- value(source_current),
         target_value <- value(target_current),
         session_id <- context.session_id,
         entry <-
           checkpoint_entry(source_value, target_value, session_id, sequence, documents, imported),
         history <- checkpoint_history(source_value, target_value, entry),
         source_epoch <- get(source_identity, :history_epoch),
         source_compaction <- get(source_identity, :compaction_epoch) || 0,
         installed_compaction <- context.installed_source_compaction_epoch || source_compaction,
         previous_safe <- int_field(target_value, :safe_source_sequence),
         safe_report <-
           safe_report_decision(
             target,
             source_identity,
             target_value,
             sequence,
             previous_safe,
             installed_compaction,
             context,
             options
           ),
         payload <-
           %{
             "version" => 1,
             "replication_id" => context.replication_id,
             "session_id" => session_id,
             "source_sequence" => sequence,
             "source_history_epoch" => source_epoch,
             "source_compaction_epoch" => source_compaction,
             "safe_source_sequence" => safe_report.safe_source_sequence,
             "installed_source_compaction_epoch" => installed_compaction,
             "history" => history
           },
         target_payload <-
           Map.put(payload, "checkpoint_version", record_version(target_current) + 1),
         source_payload <-
           Map.put(payload, "checkpoint_version", record_version(source_current) + 1) do
      {:ok,
       %{
         sequence: sequence,
         documents: documents,
         revisions: imported_count(imported),
         safe_source_sequence: safe_report.safe_source_sequence,
         source_current: source_current,
         target_current: target_current,
         target_request: checkpoint_request(target_payload, target_current),
         source_request: checkpoint_request(source_payload, source_current)
       }}
    end
  end

  defp safe_report_decision(
         target,
         source_identity,
         target_value,
         sequence,
         previous_safe,
         installed_compaction,
         context,
         options
       ) do
    has_local =
      unacknowledged_local_mutations?(target, get(source_identity, :database_uuid), options)

    SafeReport.decide(%{
      source_history_epoch: get(source_identity, :history_epoch),
      checkpoint_source_history_epoch: epoch_field(target_value),
      source_sequence: sequence,
      previous_safe_source_sequence: previous_safe,
      proposed_safe_source_sequence: sequence,
      source_compaction_epoch: get(source_identity, :compaction_epoch) || 0,
      installed_source_compaction_epoch: installed_compaction,
      boundaries_installed_through: context.boundaries_installed_through,
      position_durably_applied: true,
      has_unacknowledged_local_mutations: has_local,
      checkpoint_only:
        not Map.get(context, :bootstrap_applied, false) and
          imported_count(context.imported) == 0 and context.selected == [],
      bootstrap_applied: Map.get(context, :bootstrap_applied, false)
    })
  end

  defp report_peer_position(source, _target, context, _options) do
    case Map.get(context, :safe_source_sequence) do
      nil -> {:ok, %{skipped: true}}
      safe_sequence -> report_peer_position_value(source, context, safe_sequence)
    end
  end

  defp report_peer_position_value(source, context, safe_sequence) do
    source_identity = context.source_identity || %{}
    target_identity = context.target_identity || %{}
    peer_uuid = source_uuid(target_identity)
    now = DateTime.utc_now()
    expiry_ms = peer_expiry_ms(source_identity)
    lease_expires_at = DateTime.add(now, expiry_ms, :millisecond) |> DateTime.to_iso8601()

    value = %{
      "peer_database_uuid" => peer_uuid,
      "peer_history_epoch" => get(target_identity, :history_epoch),
      "source_database_uuid" => source_uuid(source_identity),
      "source_history_epoch" => get(source_identity, :history_epoch),
      "safe_source_sequence" => safe_sequence,
      "installed_source_compaction_epoch" => context.installed_source_compaction_epoch || 0,
      "last_seen_at" => DateTime.to_iso8601(now),
      "lease_expires_at" => lease_expires_at,
      "status" => "active"
    }

    bootstrap_completed =
      Map.get(context, :bootstrap_applied, false) or
        Map.get(context, :peer_history_replacement_required, false)

    endpoint_call(source, :put_peer_position, [
      %{
        peer_database_uuid: peer_uuid,
        expected_version: peer_record_version(source, peer_uuid),
        bootstrap_completed: bootstrap_completed,
        value: value
      }
    ])
  end

  defp peer_record_version(source, peer_uuid) do
    case endpoint_call(source, :get_local_record, ["peer_ledger", peer_uuid]) do
      {:ok, nil} -> 0
      {:ok, %{version: version}} -> version
      {:ok, %{"version" => version}} -> version
      _ -> 0
    end
  end

  defp boundary_install_page(page, source, install_id, replace) do
    source_uuid = source_uuid(source)

    page =
      case page do
        %BoundaryPage{} = boundary_page ->
          %{boundary_page | source_database_uuid: source_uuid}

        map when is_map(map) ->
          Map.put(map, "source_database_uuid", source_uuid)
      end

    case page do
      %BoundaryPage{} = boundary_page ->
        boundary_page
        |> Wire.boundary_page()
        |> Map.put("install_id", install_id)
        |> Map.put("replace", replace)

      wire when is_map(wire) ->
        wire
        |> Map.put("install_id", install_id)
        |> Map.put("replace", replace)
    end
  end

  defp clear_pending_local_causal(endpoint, peer_database_uuid) do
    case endpoint_call(endpoint, :clear_pending_local_causal, [peer_database_uuid]) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
      _ -> :ok
    end
  end

  defp peer_expiry_ms(identity) do
    get_in(identity, [:config, "retention", "peer_expiry_ms"]) ||
      get_in(identity, ["config", "retention", "peer_expiry_ms"]) || 86_400_000
  end

  defp apply_read_changes_result({:error, %ElixirDB.Error{code: :history_truncated}}, context) do
    {:ok, %{context | bootstrap_required: true, selected: []}}
  end

  defp apply_read_changes_result({:ok, changes}, context) do
    {:ok, %{context | selected: get(changes, :results) || []}}
  end

  defp apply_read_changes_result({:error, error}, _context), do: {:error, error}

  defp maybe_mark_bootstrap(context, reconcile, source_identity) do
    context =
      if reconcile.bootstrap_required do
        %{
          context
          | bootstrap_required: true,
            source_identity: source_identity,
            since: reconcile.since
        }
      else
        %{context | source_identity: source_identity}
      end

    if boundary_refresh_required?(source_identity, context) do
      %{context | boundary_refresh_required: true}
    else
      context
    end
  end

  defp checkpoint_request(payload, current),
    do: Map.put(payload, "expected_checkpoint_version", record_version(current))

  defp checkpoint_history(source, target, entry) do
    (List.wrap(source && MapAccess.get(source, :history)) ++
       List.wrap(target && MapAccess.get(target, :history)) ++ [entry])
    |> Enum.uniq_by(fn item -> {get(item, :session_id), get(item, :source_sequence)} end)
    |> Enum.sort_by(&get(&1, :source_sequence), :desc)
    |> Enum.take(10)
  end

  defp checkpoint_entry(source, target, session_id, sequence, documents, imported) do
    existing =
      (List.wrap(source && MapAccess.get(source, :history)) ++
         List.wrap(target && MapAccess.get(target, :history)))
      |> Enum.find(fn item ->
        get(item, :session_id) == session_id and get(item, :source_sequence) == sequence
      end)

    existing ||
      %{
        "session_id" => session_id,
        "source_sequence" => sequence,
        "documents_read" => documents,
        "revisions_written" => imported_count(imported),
        "completed_at" => DateTime.utc_now() |> DateTime.to_iso8601()
      }
  end

  defp ensure_progress([], since, terminal) when since < terminal,
    do: {:error, ElixirDB.Error.database_unavailable("source changes did not make progress")}

  defp ensure_progress(_selected, _since, _terminal), do: :ok

  defp terminal_changes(changes, terminal) do
    Enum.filter(get(changes, :results) || [], &(get(&1, :sequence) <= terminal))
  end

  defp take_batch_bytes(changes, options) do
    maximum = batch_byte_limit(options)

    Enum.reduce_while(changes, {[], 0}, fn change, {selected, size} ->
      take_batch_change(change, selected, size, maximum)
    end)
    |> case do
      {:error, _} = error -> error
      {:done, selected} -> {:ok, selected}
      {selected, _size} -> {:ok, Enum.reverse(selected)}
    end
  end

  defp batch_byte_limit(options) do
    default = get_in(ElixirDB.Config.defaults(), ["replication", "batch_bytes"]) || 4_194_304
    configured = option(options, :batch_bytes, default)
    configured = normalize_batch_bytes(configured, default)
    min(configured, ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216)
  end

  defp normalize_batch_bytes(value, _default) when is_integer(value) and value > 0, do: value
  defp normalize_batch_bytes(_value, default), do: default

  defp take_batch_change(change, selected, size, maximum) do
    case Canonical.encode(Stringify.keys(change)) do
      {:ok, encoded} ->
        append_batch_change(change, selected, size, byte_size(encoded), maximum)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp append_batch_change(change, selected, size, change_size, maximum) do
    next_size = size + change_size

    cond do
      next_size <= maximum ->
        {:cont, {[change | selected], next_size}}

      selected == [] ->
        {:halt,
         {:error, ElixirDB.Error.payload_too_large("replication batch exceeds the byte limit")}}

      true ->
        {:halt, {:done, Enum.reverse(selected)}}
    end
  end

  defp target_diff(_target, %{selected: []}), do: {:ok, []}

  defp target_diff(target, context) do
    source_uuid =
      context.source_identity
      |> case do
        nil -> nil
        identity -> get(identity, :database_uuid)
      end

    request = %{
      source_database_uuid: source_uuid,
      documents:
        Enum.map(context.selected, fn change ->
          %{
            document_id: get(change, :document_id),
            leaf_revisions:
              Enum.map(get(change, :leaf_revisions) || [], fn leaf ->
                %{
                  revision: get(leaf, :revision),
                  history_id: get(leaf, :history_id)
                }
              end)
          }
        end)
    }

    with {:ok, result} <- endpoint_call(target, :diff_revisions, [request]) do
      {:ok,
       Enum.flat_map(get(result, :documents) || [], fn document ->
         missing = get(document, :missing_revisions) || []

         if missing == [] do
           []
         else
           [
             %{
               document_id: get(document, :document_id),
               leaf_revisions: missing
             }
           ]
         end
       end)}
    end
  end

  defp phase_hook(options, phase, context) do
    case option(options, :phase_hook, nil) do
      nil ->
        :ok

      fun when is_function(fun, 2) ->
        case fun.(phase, context) do
          :ok -> :ok
          {:error, _} = error -> error
          other -> {:error, ElixirDB.Error.internal_error("phase hook returned #{inspect(other)}")}
        end

      _ ->
        :ok
    end
  end

  defp record_version(nil), do: 0
  defp record_version(%{version: version}) when is_integer(version), do: version
  defp record_version(%{"version" => version}) when is_integer(version), do: version
  defp record_version(_), do: 0

  defp imported_count(nil), do: 0
  defp imported_count(imported), do: get(imported, :revisions_inserted) || 0

  defp boundary_state_epoch(state) do
    case value(state) do
      map when is_map(map) -> int_field(map, :compaction_epoch)
      _ -> nil
    end
  end

  defp boundary_state_digest(state) do
    case value(state) do
      map when is_map(map) -> get(map, :boundary_digest)
      _ -> nil
    end
  end

  defp peer_history_replacement_required?(record, target_identity) do
    case value(record) do
      peer when is_map(peer) ->
        peer_epoch = get(peer, :peer_history_epoch)
        target_epoch = get(target_identity, :history_epoch)
        is_binary(peer_epoch) and is_binary(target_epoch) and peer_epoch != target_epoch

      _ ->
        false
    end
  end

  defp boundary_refresh_required?(source_identity, _target_checkpoint, target_state) do
    installed_epoch = boundary_state_epoch(target_state) || 0
    installed_digest = boundary_state_digest(target_state)
    boundary_refresh_needed?(source_identity, installed_epoch, installed_digest)
  end

  defp boundary_refresh_needed?(source_identity, installed_epoch, installed_digest) do
    source_epoch = get(source_identity, :compaction_epoch) || 0
    source_digest = get(source_identity, :retention_boundary_digest)

    source_epoch > installed_epoch or
      (is_binary(source_digest) and is_binary(installed_digest) and
         source_digest != installed_digest)
  end

  defp boundary_refresh_required?(source_identity, context) do
    boundary_refresh_needed?(
      source_identity,
      Map.get(context, :installed_source_compaction_epoch, 0) || 0,
      Map.get(context, :installed_source_boundary_digest)
    )
  end

  defp int_field(map, key) when is_map(map) do
    MapAccess.get(map, key) || MapAccess.get(map, Atom.to_string(key)) || 0
  end

  defp int_field(_map, _key), do: 0

  defp epoch_field(map) when is_map(map) do
    MapAccess.get(map, :source_history_epoch) || MapAccess.get(map, "source_history_epoch")
  end

  defp epoch_field(_), do: nil

  defp compatible(source, target) do
    source_uuid = source_uuid(source)
    target_uuid = source_uuid(target)

    cond do
      source_uuid == target_uuid ->
        {:error,
         ElixirDB.Error.replication_incompatible("source and target database UUIDs must differ")}

      version(source, :replication_protocol_major) != version(target, :replication_protocol_major) ->
        {:error, ElixirDB.Error.replication_incompatible("replication protocol versions differ")}

      version(source, :revision_algorithm_version) != version(target, :revision_algorithm_version) ->
        {:error, ElixirDB.Error.replication_incompatible("revision algorithms differ")}

      version(source, :canonicalization_version) != version(target, :canonicalization_version) ->
        {:error, ElixirDB.Error.replication_incompatible("canonicalization versions differ")}

      true ->
        :ok
    end
  end

  defp endpoint_call(%module{} = endpoint, function, args) when is_atom(module),
    do: apply(module, function, [endpoint | args])

  defp source_uuid(map), do: get(map, :database_uuid)
  defp version(map, key), do: get(map, key)

  defp get(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp value(nil), do: nil
  defp value(%{value: value}), do: value
  defp value(%{"value" => value}), do: value

  defp value(_other), do: nil

  defp unacknowledged_local_mutations?(target, source_database_uuid, options) do
    if explicit_local_mutations_option?(options) do
      option(options, :has_unacknowledged_local_mutations, true)
    else
      case endpoint_call(target, :has_local_origin_changes?, [source_database_uuid]) do
        {:ok, value} when is_boolean(value) -> value
        _ -> true
      end
    end
  end

  defp explicit_local_mutations_option?(options) when is_map(options),
    do: Map.has_key?(options, :has_unacknowledged_local_mutations)

  defp explicit_local_mutations_option?(options) when is_list(options),
    do: Keyword.has_key?(options, :has_unacknowledged_local_mutations)

  defp option(options, key, default) when is_map(options),
    do: MapAccess.get(options, key, default)

  defp option(options, key, default) when is_list(options),
    do: Keyword.get(options, key, default)

  defp put_option(options, key, value) when is_map(options), do: Map.put(options, key, value)
  defp put_option(options, key, value) when is_list(options), do: Keyword.put(options, key, value)

  defp transfer_config(options) do
    defaults = ElixirDB.Config.defaults()["replication"]

    %{
      max_concurrent_chain_fetches:
        option(options, :max_concurrent_chain_fetches, defaults["max_concurrent_chain_fetches"]),
      max_concurrent_blob_transfers:
        option(options, :max_concurrent_blob_transfers, defaults["max_concurrent_blob_transfers"]),
      max_transfer_bytes_in_flight:
        option(options, :max_transfer_bytes_in_flight, defaults["max_transfer_bytes_in_flight"]),
      batch_documents: option(options, :batch_documents, option(options, :batch, @default_batch)),
      phase_hook: option(options, :phase_hook, nil),
      replication_id: option(options, :replication_id, nil)
    }
  end
end
