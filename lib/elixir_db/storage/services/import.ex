defmodule ElixirDB.Storage.Services.Import do
  @moduledoc """
  Shared revision-chain import write workflows.

  Owns chain validation, revision insertion, stale fencing, and
  replication-sourced change finalization against storage ports.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.{RetentionBoundary, Revision}
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Import, as: ImportInstrumentation
  alias ElixirDB.Replication.Profile
  alias ElixirDB.Revisions.{Id, Winner}
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Services.Facts
  alias ElixirDB.Storage.Services.Shadows

  @doc """
  Validates host limits for a replication chain batch.
  """
  @spec validate_chain_batch(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_chain_batch(chains) when is_list(chains) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      length(chains) > max ->
        {:error, ElixirDB.Error.resource_limit("replication chain count exceeds the host limit")}

      not Enum.all?(chains, &is_map/1) ->
        {:error, ElixirDB.Error.invalid_request("replication chains must be objects")}

      not Enum.all?(chains, fn chain ->
        Enum.all?(
          Map.keys(chain),
          &(&1 in [
              :document_id,
              :history_id,
              :leaf_revision,
              :source_update_sequence,
              :revisions,
              :truncated,
              "document_id",
              "history_id",
              "leaf_revision",
              "source_update_sequence",
              "revisions",
              "truncated"
            ])
        )
      end) ->
        {:error, ElixirDB.Error.invalid_request("replication chains contain an unknown field")}

      true ->
        :ok
    end
  end

  def validate_chain_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("replication chains must be an array")}

  @doc "Validates the bounded retired-history manifest carried by bootstrap pages."
  @spec validate_purged_boundaries(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_purged_boundaries(boundaries), do: validate_purged_boundaries(boundaries, nil)

  @spec validate_purged_boundaries(term(), binary() | nil) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_purged_boundaries(boundaries, source_database_uuid) when is_list(boundaries) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      length(boundaries) > max ->
        {:error, ElixirDB.Error.resource_limit("purged boundary count exceeds the host limit")}

      not Enum.all?(boundaries, &is_map/1) ->
        {:error, ElixirDB.Error.invalid_request("purged boundaries must be objects")}

      boundaries != [] and not is_binary(source_database_uuid) ->
        {:error, ElixirDB.Error.invalid_request("purged boundaries require a source database UUID")}

      not Enum.all?(boundaries, &valid_purged_boundary?(&1, source_database_uuid)) ->
        {:error, ElixirDB.Error.invalid_request("purged boundary contains an unknown field")}

      true ->
        :ok
    end
  end

  def validate_purged_boundaries(_, _),
    do: {:error, ElixirDB.Error.invalid_request("purged boundaries must be an array")}

  @doc """
  Ensures every attachment digest referenced by import chains is physically present.

  Runs before the import mutation transaction. Memory adapters skip the check.
  """
  @spec ensure_physical_blobs(BackendContext.t(), term()) :: :ok | {:error, ElixirDB.Error.t()}
  def ensure_physical_blobs(%BackendContext{} = context, chains) when is_list(chains) do
    Facts.verify_physical_digests(context, collect_import_digests(chains))
  end

  def ensure_physical_blobs(_context, _chains),
    do: {:error, ElixirDB.Error.invalid_request("replication chains must be an array")}

  @doc """
  Imports revision chains inside an open transaction.
  """
  @spec import_tx(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def import_tx(%BackendContext{} = context, request) do
    chains = MapAccess.get(request, :chains, [])
    purged_boundaries = MapAccess.get(request, :purged_boundaries, [])
    source_database_uuid = MapAccess.get(request, :source_database_uuid)

    case request_profile(request) do
      :invalid ->
        {:error, ElixirDB.Error.invalid_request("replication profile is invalid")}

      profile ->
        import_tx_with_profile(
          context,
          request,
          profile,
          chains,
          purged_boundaries,
          source_database_uuid
        )
    end
  end

  defp import_tx_with_profile(
         context,
         request,
         profile,
         chains,
         purged_boundaries,
         source_database_uuid
       ) do
    shadow? = Profile.shadow?(profile)

    with :ok <- validate_profile_request(context, request, profile),
         :ok <- validate_purged_boundaries(purged_boundaries, source_database_uuid),
         :ok <- validate_purge_safety(context, purged_boundaries),
         :ok <- Facts.install_imported_boundaries(context, purged_boundaries),
         {:ok, source_origins} <- source_origins(chains, shadow?),
         {:ok, revisions} <- validate_chains(chains),
         {:ok, %{affected: affected, inserted: inserted}} <-
           insert_imported_revisions(context, revisions),
         {:ok, purged_affected} <- purge_imported_histories(context, purged_boundaries),
         :ok <- remove_pending_for_revisions(context, revisions, shadow?),
         :ok <- put_shadow_origins(context, source_origins, shadow?),
         :ok <- put_shadow_watermark(context, request, source_origins, shadow?) do
      finalize_imports(
        context,
        MapSet.union(affected, purged_affected),
        inserted,
        profile,
        source_origins
      )
    end
  end

  defp valid_purged_boundary?(boundary, source_database_uuid) do
    allowed = [
      :source_database_uuid,
      :source_history_epoch,
      :boundary,
      :compaction_epoch,
      "source_database_uuid",
      "source_history_epoch",
      "boundary",
      "compaction_epoch"
    ]

    if Enum.all?(Map.keys(boundary), &(&1 in allowed)) do
      source_uuid = MapAccess.get(boundary, :source_database_uuid)
      source_epoch = MapAccess.get(boundary, :source_history_epoch)
      compaction_epoch = MapAccess.get(boundary, :compaction_epoch)
      raw_boundary = MapAccess.get(boundary, :boundary)

      with true <- is_binary(source_uuid) and source_uuid != "",
           true <- is_nil(source_database_uuid) or source_uuid == source_database_uuid,
           true <- is_binary(source_epoch) and source_epoch != "",
           true <- is_integer(compaction_epoch) and compaction_epoch >= 0,
           {:ok, %RetentionBoundary{retired: true}} <- RetentionBoundary.from_wire(raw_boundary) do
        true
      else
        _ -> false
      end
    else
      false
    end
  end

  defp validate_purge_safety(_context, []), do: :ok

  defp validate_purge_safety(context, _purged_boundaries) do
    case Facts.pending_local_causal?(context) do
      {:ok, false} ->
        :ok

      {:ok, true} ->
        {:error, ElixirDB.Error.rebase_required("bootstrap cannot purge local history")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_chains(chains) do
    Enum.reduce_while(chains, {:ok, []}, fn chain, {:ok, acc} ->
      validate_chain_entry(chain, acc)
    end)
    |> case do
      {:ok, revisions} -> {:ok, revisions}
      error -> error
    end
  end

  defp validate_chain_entry(chain, acc) when is_map(chain) do
    document_id = MapAccess.get(chain, :document_id)
    leaf_revision = MapAccess.get(chain, :leaf_revision)
    revisions = MapAccess.get(chain, :revisions, [])
    truncated = MapAccess.get(chain, :truncated, false)

    if valid_chain_shape?(chain, document_id, leaf_revision, revisions) do
      append_validated_chain(document_id, leaf_revision, revisions, truncated, acc)
    else
      {:halt,
       {:error,
        ElixirDB.Error.invalid_request("revision chain requires document, leaf, and revisions")}}
    end
  end

  defp validate_chain_entry(_chain, _acc),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("revision chains must be objects")}}

  defp append_validated_chain(document_id, leaf_revision, revisions, truncated, acc) do
    case validate_chain_revisions(document_id, revisions, truncated) do
      {:ok, ^leaf_revision, built} ->
        entries =
          built
          |> Enum.reverse()
          |> Enum.with_index()
          |> Enum.map(fn {revision, index} -> {revision, truncated and index == 0} end)

        {:cont, {:ok, acc ++ entries}}

      {:ok, _parent, _built} ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("revision chain leaf does not match payload")}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp valid_chain_shape?(chain, document_id, leaf_revision, revisions) do
    allowed_keys = [
      :document_id,
      :history_id,
      :leaf_revision,
      :source_update_sequence,
      :revisions,
      :truncated,
      "document_id",
      "history_id",
      "leaf_revision",
      "source_update_sequence",
      "revisions",
      "truncated"
    ]

    Enum.all?(Map.keys(chain), &(&1 in allowed_keys)) and is_binary(document_id) and
      document_id != "" and is_binary(leaf_revision) and is_list(revisions) and revisions != []
  end

  defp validate_chain_revisions(document_id, revisions, truncated) do
    Enum.reduce_while(revisions, {:ok, :root, []}, fn raw, {:ok, expected_parent, built} ->
      allow_any_parent = truncated and built == []
      validate_revision_order(document_id, raw, expected_parent, built, allow_any_parent)
    end)
  end

  defp validate_revision_order(document_id, raw, expected_parent, built, allow_any_parent) do
    with {:ok, revision} <- imported_revision(document_id, raw),
         true <-
           allow_any_parent or
             (expected_parent == :root and is_nil(revision.parent_revision)) or
             revision.parent_revision == expected_parent do
      {:cont, {:ok, revision.revision_id, [revision | built]}}
    else
      false ->
        {:halt,
         {:error, ElixirDB.Error.integrity_violation("revision chain is not parent ordered")}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp imported_revision(_document_id, raw) when not is_map(raw),
    do: {:error, ElixirDB.Error.invalid_request("revision chain entries must be objects")}

  defp imported_revision(document_id, raw) do
    allowed_keys = [
      :document_id,
      :revision_id,
      :generation,
      :parent_revision,
      :history_id,
      :deleted,
      :body,
      "document_id",
      "revision_id",
      "generation",
      "parent_revision",
      "history_id",
      "deleted",
      "body",
      :attachments,
      "attachments"
    ]

    if Enum.all?(Map.keys(raw), &(&1 in allowed_keys)) do
      generation_value = MapAccess.get(raw, :generation)
      revision_id = MapAccess.get(raw, :revision_id)
      parent = MapAccess.get(raw, :parent_revision)
      history_id = MapAccess.get(raw, :history_id)
      deleted = MapAccess.get(raw, :deleted, false)
      body = MapAccess.get(raw, :body)

      with :ok <- validate_import_history_id(history_id),
           {:ok, attachments} <- normalize_import_attachments(raw, deleted),
           {:ok, calculated} <-
             Id.calculate(
               document_id,
               history_id,
               parent,
               deleted,
               body,
               attachments
             ),
           true <- calculated == revision_id,
           {:ok, generation} <- Id.generation(revision_id),
           true <- generation_value == generation,
           true <- is_boolean(deleted),
           true <- deleted or is_map(body),
           true <- deleted or body_size_within_limit?(body) do
        {:ok,
         Revision.assemble(
           document_id: document_id,
           history_id: history_id,
           revision_id: revision_id,
           generation: generation,
           parent_revision: parent,
           digest: digest(revision_id),
           deleted: deleted,
           body: body,
           attachments: attachments
         )}
      else
        false -> {:error, ElixirDB.Error.integrity_violation("revision chain validation failed")}
        {:error, error} -> {:error, error}
      end
    else
      {:error, ElixirDB.Error.invalid_request("revision chain entry contains an unknown field")}
    end
  end

  defp validate_import_history_id(history_id)
       when is_binary(history_id) and history_id != "",
       do: :ok

  defp validate_import_history_id(_),
    do: {:error, ElixirDB.Error.invalid_request("revision chain history_id is required")}

  defp body_size_within_limit?(body) do
    case Canonical.encode(body) do
      {:ok, json} ->
        byte_size(json) <= (ElixirDB.Config.host_limits()[:max_document_bytes] || 1_048_576)

      {:error, _} ->
        false
    end
  end

  defp remove_pending_for_revisions(_context, _revisions, true), do: :ok

  defp remove_pending_for_revisions(context, revisions, false) do
    Enum.reduce_while(revisions, :ok, fn {revision, _allow_dangling_parent}, :ok ->
      case Facts.clear_pending_for_manifest(context, revision.attachments) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp insert_imported_revisions(context, revisions) do
    with {:ok, boundaries} <- Facts.list_boundaries(context) do
      import_revision_batch(context, revisions, boundaries)
    end
  end

  defp purge_imported_histories(_context, []), do: {:ok, MapSet.new()}

  defp purge_imported_histories(context, boundaries) do
    Enum.reduce_while(boundaries, {:ok, MapSet.new()}, fn raw, {:ok, affected} ->
      purge_imported_history(context, raw, affected)
    end)
    |> case do
      {:ok, affected} -> {:ok, affected}
      {:error, error} -> {:error, error}
    end
  end

  defp purge_imported_history(context, raw, affected) do
    boundary = MapAccess.get(raw, :boundary, %{})
    document_id = MapAccess.get(boundary, :document_id)
    history_id = MapAccess.get(boundary, :history_id)

    case Facts.find_document(context, document_id) do
      {:ok, nil} ->
        {:cont, {:ok, affected}}

      {:ok, _doc} ->
        delete_imported_history(context, document_id, history_id, affected)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp delete_imported_history(context, document_id, history_id, affected) do
    case Facts.delete_history(context, document_id, history_id) do
      :ok -> {:cont, {:ok, MapSet.put(affected, document_id)}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp import_revision_batch(context, revisions, boundaries) do
    uuid = Map.get(Facts.identity(context), :database_uuid)

    Enum.reduce_while(
      revisions,
      {:ok, MapSet.new(), 0, 0, MapSet.new()},
      fn {revision, allow_dangling_parent}, {:ok, affected, inserted, stale, fenced} ->
        import_revision_entry(
          context,
          revision,
          allow_dangling_parent,
          boundaries,
          affected,
          inserted,
          stale,
          fenced
        )
      end
    )
    |> finalize_import_batch(uuid)
  end

  defp import_revision_entry(
         context,
         revision,
         allow_dangling_parent,
         boundaries,
         affected,
         inserted,
         stale,
         fenced
       ) do
    if compacted_stale?(context, revision, boundaries, fenced) do
      {:cont, {:ok, affected, inserted, stale + 1, MapSet.put(fenced, revision.revision_id)}}
    else
      insert_imported_revision(
        context,
        revision,
        allow_dangling_parent,
        affected,
        inserted,
        stale,
        fenced
      )
    end
  end

  defp finalize_import_batch({:ok, affected, inserted, stale, _fenced}, uuid) do
    ImportInstrumentation.stale_fence_noop(uuid, stale)
    {:ok, %{affected: affected, inserted: inserted}}
  end

  defp finalize_import_batch({:error, _} = error, _uuid), do: error

  defp insert_imported_revision(
         context,
         revision,
         allow_dangling_parent,
         affected,
         inserted,
         stale,
         fenced
       ) do
    case Facts.find_document(context, revision.document_id) do
      {:ok, nil} ->
        insert_new_revision(
          context,
          revision,
          allow_dangling_parent,
          affected,
          inserted,
          stale,
          fenced
        )

      {:error, %ElixirDB.Error{code: :document_not_found}} ->
        insert_new_revision(
          context,
          revision,
          allow_dangling_parent,
          affected,
          inserted,
          stale,
          fenced
        )

      {:ok, doc} ->
        insert_existing_revision(
          context,
          doc,
          revision,
          allow_dangling_parent,
          affected,
          inserted,
          stale,
          fenced
        )

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp insert_new_revision(
         context,
         revision,
         allow_dangling_parent,
         affected,
         inserted,
         stale,
         fenced
       ) do
    document_id = revision.document_id

    with {:ok, _doc} <- Facts.ensure_document(context, document_id),
         :ok <- ensure_import_parent(context, document_id, revision, allow_dangling_parent),
         :ok <- Facts.insert_revision(context, document_id, revision) do
      {:cont, {:ok, MapSet.put(affected, document_id), inserted + 1, stale, fenced}}
    else
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp insert_existing_revision(
         context,
         doc,
         revision,
         allow_dangling_parent,
         affected,
         inserted,
         stale,
         fenced
       ) do
    case Facts.find_revision(context, doc.document_id, revision.revision_id) do
      {:ok, existing} ->
        existing_revision_result(existing, revision, affected, inserted, stale, fenced)

      {:error, %ElixirDB.Error{code: :revision_not_found}} ->
        with :ok <-
               ensure_import_parent(
                 context,
                 doc.document_id,
                 revision,
                 allow_dangling_parent
               ),
             :ok <- Facts.insert_revision(context, doc.document_id, revision) do
          {:cont, {:ok, MapSet.put(affected, revision.document_id), inserted + 1, stale, fenced}}
        else
          {:error, error} -> {:halt, {:error, error}}
        end

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp existing_revision_result(existing, revision, affected, inserted, stale, fenced) do
    if Facts.same_revision?(existing, revision) do
      {:cont, {:ok, affected, inserted, stale, fenced}}
    else
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("existing revision differs from imported revision")}}
    end
  end

  defp ensure_import_parent(_context, _document_id, %Revision{parent_revision: nil}, _allow),
    do: :ok

  defp ensure_import_parent(context, document_id, revision, true) do
    case revision.parent_revision do
      nil -> :ok
      parent -> parent_present_or_allowed(context, document_id, parent)
    end
  end

  defp ensure_import_parent(context, document_id, revision, false),
    do: Facts.ensure_parent(context, document_id, revision.parent_revision)

  defp parent_present_or_allowed(context, document_id, parent) do
    case Facts.find_revision(context, document_id, parent) do
      {:ok, _} -> :ok
      {:error, %ElixirDB.Error{code: :revision_not_found}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp compacted_stale?(context, revision, boundaries, fenced) do
    parent_fenced? =
      is_binary(revision.parent_revision) and MapSet.member?(fenced, revision.parent_revision)

    parent_fenced? or
      Enum.any?(boundaries, fn %{boundary: boundary} ->
        boundary.document_id == revision.document_id and
          boundary.history_id == revision.history_id and
          compacted_by_boundary?(context, boundary, revision)
      end)
  end

  defp compacted_by_boundary?(_context, %{retired: true}, _revision), do: true

  defp compacted_by_boundary?(context, boundary, revision) do
    generation_fenced?(boundary, revision) or
      retired_branch_member?(context, boundary, revision)
  end

  defp generation_fenced?(%{minimum_retained_generation: generation}, revision)
       when is_integer(generation) do
    revision.generation < generation
  end

  defp generation_fenced?(_boundary, _revision), do: false

  defp retired_branch_member?(context, %{retired_branch_roots: roots}, revision)
       when is_list(roots) and roots != [] do
    root_set = MapSet.new(roots)

    MapSet.member?(root_set, revision.revision_id) or
      ancestor_in_retired_roots?(context, revision, root_set)
  end

  defp retired_branch_member?(_context, _boundary, _revision), do: false

  defp ancestor_in_retired_roots?(context, revision, root_set) do
    parent_id = revision.parent_revision

    cond do
      not is_binary(parent_id) or parent_id == "" ->
        false

      MapSet.member?(root_set, parent_id) ->
        true

      true ->
        walk_parent_from_document(context, revision.document_id, parent_id, root_set)
    end
  end

  defp walk_parent_from_document(context, document_id, parent_id, root_set) do
    case Facts.find_document(context, document_id) do
      {:ok, nil} ->
        false

      {:ok, _doc} ->
        walk_retired_ancestors(context, document_id, parent_id, root_set)

      _ ->
        false
    end
  end

  defp walk_retired_ancestors(context, document_id, revision_id, root_set) do
    case Facts.find_revision(context, document_id, revision_id) do
      {:ok, parent} when not is_nil(parent) ->
        continue_retired_walk(context, document_id, parent, root_set)

      _ ->
        false
    end
  end

  defp continue_retired_walk(context, document_id, parent, root_set) do
    next = parent.parent_revision

    cond do
      MapSet.member?(root_set, parent.revision_id) -> true
      not is_binary(next) or next == "" -> false
      MapSet.member?(root_set, next) -> true
      true -> walk_retired_ancestors(context, document_id, next, root_set)
    end
  end

  defp finalize_imports(context, affected, inserted, profile, source_origins) do
    affected = Enum.sort(MapSet.to_list(affected))

    Enum.reduce_while(
      affected,
      {:ok, %{documents_changed: 0, revisions_inserted: inserted, last_sequence: 0}},
      fn document_id, {:ok, acc} ->
        finalize_import_document(context, document_id, acc, profile, source_origins)
      end
    )
    |> case do
      {:ok, result} -> {:ok, result}
      error -> error
    end
  end

  defp finalize_import_document(context, document_id, acc, profile, source_origins) do
    case Facts.list_leaves(context, document_id) do
      {:ok, []} ->
        with :ok <- Facts.empty_document(context, document_id),
             :ok <- Facts.refresh_document(context, document_id, %{body: nil, deleted: true}),
             :ok <- put_empty_shadow_origin(context, profile, source_origins, document_id) do
          {:cont, {:ok, %{acc | documents_changed: acc.documents_changed + 1}}}
        else
          {:error, error} -> {:halt, {:error, error}}
        end

      {:ok, leaves_now} ->
        with {:ok, doc} <- Facts.find_document(context, document_id),
             {:ok, winner} <- Winner.select(leaves_now),
             {:ok, leaf_json} <- Facts.encode_leaf_set(leaves_now),
             :ok <- Facts.update_winning(context, document_id, winner, 0),
             :ok <- Facts.refresh_document(context, document_id, winner),
             {:ok, sequence} <- Facts.allocate_sequence(context),
             {:ok, document_sequence} <-
               imported_document_sequence(context, profile, source_origins, document_id, sequence),
             :ok <- Facts.update_winning(context, document_id, winner, document_sequence),
             :ok <-
               Facts.append_change(
                 context,
                 Facts.change_entry(
                   sequence,
                   document_id,
                   winner,
                   leaf_json,
                   "replication",
                   Facts.backend_meta(doc)
                 )
               ) do
          {:cont,
           {:ok,
            %{
              acc
              | documents_changed: acc.documents_changed + 1,
                last_sequence: max(acc.last_sequence, sequence)
            }}}
        else
          {:error, error} -> {:halt, {:error, error}}
        end

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp imported_document_sequence(
         _context,
         %Profile{kind: :peer},
         _origins,
         _document_id,
         sequence
       ),
       do: {:ok, sequence}

  defp imported_document_sequence(context, %Profile{kind: :shadow}, origins, document_id, _sequence) do
    case Map.fetch(origins, document_id) do
      {:ok, sequence} ->
        {:ok, sequence}

      :error ->
        case Shadows.origin(context, document_id) do
          {:ok, sequence} when is_integer(sequence) ->
            {:ok, sequence}

          {:ok, nil} ->
            {:error,
             ElixirDB.Error.integrity_violation("shadow document has no durable source origin")}

          {:error, error} ->
            {:error, error}
        end
    end
  end

  defp put_empty_shadow_origin(_context, %Profile{kind: :peer}, _origins, _document_id), do: :ok

  defp put_empty_shadow_origin(context, %Profile{kind: :shadow}, origins, document_id) do
    case Map.fetch(origins, document_id) do
      {:ok, sequence} ->
        case Shadows.put_origin(context, document_id, sequence) do
          {:ok, _} -> :ok
          {:error, error} -> {:error, error}
        end

      :error ->
        case Shadows.origin(context, document_id) do
          {:ok, sequence} when is_integer(sequence) ->
            :ok

          {:ok, nil} ->
            {:error,
             ElixirDB.Error.integrity_violation("shadow deleted document has no source origin")}

          {:error, error} ->
            {:error, error}
        end
    end
  end

  defp request_profile(request) do
    case MapAccess.get(request, :profile) do
      %Profile{} = profile ->
        profile

      value when value in [:peer, "peer", nil] ->
        Profile.peer()

      value when value in [:shadow, "shadow"] ->
        Profile.shadow(
          source_database_uuid: MapAccess.get(request, :source_database_uuid),
          target_database_uuid:
            MapAccess.get(request, :shadow_database_uuid) ||
              MapAccess.get(request, :target_database_uuid),
          generation:
            MapAccess.get(request, :shadow_generation) || MapAccess.get(request, :generation),
          operation_id: MapAccess.get(request, :operation_id)
        )

      _ ->
        :invalid
    end
  end

  defp validate_profile_request(_context, request, %Profile{kind: :peer}) do
    if is_nil(MapAccess.get(request, :shadow_database_uuid)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("peer import cannot target a shadow database")}
  end

  defp validate_profile_request(context, request, %Profile{kind: :shadow} = profile) do
    with :ok <- Profile.validate(profile),
         {:ok, identity} <- {:ok, Facts.identity(context)},
         true <- MapAccess.get(identity, :database_kind) in [:shadow, "shadow"],
         true <- MapAccess.get(identity, :database_uuid) == profile.target_database_uuid,
         true <- MapAccess.get(request, :source_database_uuid) == profile.source_database_uuid,
         true <-
           (MapAccess.get(request, :shadow_database_uuid) ||
              MapAccess.get(request, :target_database_uuid)) == profile.target_database_uuid,
         :ok <- validate_shadow_metadata(context, profile),
         :ok <- validate_source_watermark(request) do
      :ok
    else
      false ->
        {:error, ElixirDB.Error.shadow_incompatible("shadow import binding does not match target")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_shadow_metadata(context, profile) do
    case Shadows.metadata(context) do
      {:ok, metadata} when is_map(metadata) ->
        if metadata_field(metadata, :source_database_uuid) == profile.source_database_uuid and
             metadata_field(metadata, :shadow_database_uuid) == profile.target_database_uuid and
             metadata_field(metadata, :generation) == profile.generation and
             metadata_field(metadata, :operation_id) == profile.operation_id do
          :ok
        else
          {:error,
           ElixirDB.Error.shadow_identity_conflict("shadow metadata binding does not match")}
        end

      {:ok, nil} ->
        {:error, ElixirDB.Error.shadow_identity_conflict("shadow metadata is missing")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_source_watermark(request) do
    case MapAccess.get(request, :source_watermark) do
      value when is_integer(value) and value >= 0 -> :ok
      nil -> :ok
      _ -> {:error, ElixirDB.Error.invalid_request("shadow source watermark is invalid")}
    end
  end

  defp source_origins(_chains, false), do: {:ok, %{}}

  defp source_origins(chains, true) when is_list(chains) do
    Enum.reduce_while(chains, {:ok, %{}}, fn chain, {:ok, origins} ->
      document_id = MapAccess.get(chain, :document_id)
      sequence = MapAccess.get(chain, :source_update_sequence)

      cond do
        not is_binary(document_id) or document_id == "" ->
          {:halt, {:error, ElixirDB.Error.invalid_request("shadow chain document_id is invalid")}}

        not is_integer(sequence) or sequence < 0 ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("shadow chain source_update_sequence is required")}}

        Map.get(origins, document_id) in [nil, sequence] ->
          {:cont, {:ok, Map.put(origins, document_id, sequence)}}

        true ->
          {:halt,
           {:error,
            ElixirDB.Error.integrity_violation("shadow chains disagree on source_update_sequence")}}
      end
    end)
  end

  defp source_origins(_chains, true),
    do: {:error, ElixirDB.Error.invalid_request("shadow chains must be an array")}

  defp put_shadow_origins(_context, _origins, false), do: :ok

  defp put_shadow_origins(context, origins, true) do
    Enum.reduce_while(Enum.sort(origins), :ok, fn {document_id, sequence}, :ok ->
      case Shadows.put_origin(context, document_id, sequence) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp put_shadow_watermark(_context, _request, _origins, false), do: :ok

  defp put_shadow_watermark(context, request, origins, true) do
    watermark = MapAccess.get(request, :source_watermark) || max_origin(origins)

    case Shadows.put_watermark(context, watermark) do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp max_origin(origins) when map_size(origins) == 0, do: 0

  defp max_origin(origins),
    do: origins |> Enum.max_by(fn {_id, sequence} -> sequence end) |> elem(1)

  defp metadata_field(metadata, key),
    do: Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()

  defp normalize_import_attachments(raw, _deleted) do
    case MapAccess.get(raw, :attachments) || MapAccess.get(raw, "attachments") || %{} do
      attachments when is_map(attachments) -> Manifest.normalize(attachments)
      _ -> {:error, ElixirDB.Error.invalid_request("attachments must be an object")}
    end
  end

  defp collect_import_digests(chains) do
    chains
    |> Enum.flat_map(&chain_attachment_digests/1)
    |> Enum.reduce(%{}, fn {digest, logical_size}, acc ->
      Map.put_new(acc, digest, logical_size)
    end)
    |> Map.to_list()
  end

  defp chain_attachment_digests(chain) do
    revisions = MapAccess.get(chain, :revisions) || []
    Enum.flat_map(revisions, &revision_attachment_digests/1)
  end

  defp revision_attachment_digests(revision) do
    case MapAccess.get(revision, :attachments) || %{} do
      attachments when is_map(attachments) ->
        attachments
        |> Map.values()
        |> Enum.map(fn entry ->
          {MapAccess.get(entry, :digest) || MapAccess.get(entry, "digest"),
           MapAccess.get_first(entry, [:length])}
        end)
        |> Enum.filter(fn {digest, logical_size} ->
          is_binary(digest) and is_integer(logical_size) and logical_size >= 0
        end)

      _ ->
        []
    end
  end
end
