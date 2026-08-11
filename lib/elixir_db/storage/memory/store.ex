defmodule ElixirDB.Storage.Memory.Store do
  @moduledoc """
  In-memory fact store for the Memory storage backend.

  Holds normalized documents, revisions, changes, and retention markers in an
  Agent. Persists an Erlang-term snapshot under the bundle root so close/reopen
  semantic tests can reload state without SQL.
  """

  use Agent

  alias ElixirDB.Domain.{BoundaryPage, PeerPosition, Revision}
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Compare

  @snapshot_file "memory.backend"

  @type peer_entry :: %{version: non_neg_integer(), value: map()}
  @type staging_entry :: %{state: map(), boundaries: %{optional({binary(), binary()}) => term()}}

  @type state :: %{
          root: binary(),
          identity: map(),
          documents: %{optional(binary()) => map()},
          revisions: %{optional(binary()) => %{optional(binary()) => map()}},
          changes: [map()],
          boundaries: [map()],
          peers: %{optional(binary()) => peer_entry()},
          boundary_install_state: %{optional(binary()) => map()},
          boundary_staging: %{optional(binary()) => staging_entry()},
          maintenance_counter: non_neg_integer(),
          compaction_result: map() | nil,
          checkpoints: %{optional(binary()) => %{version: non_neg_integer(), value: map()}},
          pending_local_causal: boolean(),
          pending_blobs: %{optional(binary()) => map()},
          closed?: boolean()
        }

  @doc "Starts an empty in-memory store for `root`."
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts) do
    root = Keyword.fetch!(opts, :root)
    identity = Keyword.fetch!(opts, :identity)
    Agent.start_link(fn -> empty_state(root, identity) end)
  end

  @doc "Opens a store from a previously persisted snapshot."
  @spec open(binary()) :: {:ok, pid()} | {:error, ElixirDB.Error.t()}
  def open(root) when is_binary(root) do
    path = snapshot_path(root)

    with {:ok, binary} <- File.read(path),
         {:ok, state} <- decode_snapshot(binary),
         {:ok, pid} <- Agent.start_link(fn -> %{state | root: root, closed?: false} end) do
      {:ok, pid}
    else
      {:error, :enoent} ->
        {:error, ElixirDB.Error.invalid_request("memory backend snapshot is missing")}

      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error, ElixirDB.Error.internal_error("cannot open memory store: #{inspect(reason)}")}
    end
  end

  @doc "Persists and stops the store."
  @spec close(pid()) :: :ok | {:error, ElixirDB.Error.t()}
  def close(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      state = Agent.get(pid, & &1)

      case persist(state) do
        :ok ->
          Agent.stop(pid)
          :ok

        {:error, _} = error ->
          _ = Agent.stop(pid)
          error
      end
    else
      :ok
    end
  end

  @doc "Returns the current identity map, including current_sequence."
  @spec identity(pid()) :: map()
  def identity(pid), do: Agent.get(pid, &refresh_identity/1)

  @doc "Returns the full store state for port implementations."
  @spec get(pid()) :: state()
  def get(pid), do: Agent.get(pid, & &1)

  @doc "Updates store state with `fun`."
  @spec update(pid(), (state() -> {:ok, state(), term()} | {:error, ElixirDB.Error.t()})) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def update(pid, fun) when is_function(fun, 1) do
    Agent.get_and_update(pid, fn state ->
      case fun.(state) do
        {:ok, new_state, value} -> {{:ok, value}, new_state}
        {:error, error} -> {{:error, error}, state}
      end
    end)
  end

  @doc "Finds one document fact."
  @spec find_document(state(), binary()) :: map() | nil
  def find_document(state, document_id) do
    case Map.get(state.documents, document_id) do
      nil -> nil
      doc -> shape_document(document_id, doc)
    end
  end

  @doc "Ensures a document placeholder exists."
  @spec ensure_document(state(), binary()) :: {:ok, state(), map()}
  def ensure_document(state, document_id) do
    case Map.get(state.documents, document_id) do
      nil ->
        documents =
          Map.put(state.documents, document_id, %{
            winning_revision: nil,
            winning_deleted: true,
            update_sequence: 0,
            body: nil
          })

        new_state = %{state | documents: documents}
        {:ok, new_state, shape_document(document_id, documents[document_id])}

      doc ->
        {:ok, state, shape_document(document_id, doc)}
    end
  end

  @doc "Inserts a revision and updates leaf markers."
  @spec insert_revision(state(), binary(), Revision.t()) ::
          {:ok, state()} | {:error, ElixirDB.Error.t()}
  def insert_revision(state, document_id, %Revision{} = revision) do
    by_doc = Map.get(state.revisions, document_id, %{})

    if Map.has_key?(by_doc, revision.revision_id) do
      {:error, ElixirDB.Error.integrity_violation("revision already exists")}
    else
      by_doc =
        by_doc
        |> clear_parent_leaf(revision.parent_revision)
        |> Map.put(revision.revision_id, %{revision: revision, is_leaf: true})

      {:ok, %{state | revisions: Map.put(state.revisions, document_id, by_doc)}}
    end
  end

  @doc "Finds one revision or returns a revision_not_found error."
  @spec find_revision(state(), binary(), binary()) ::
          {:ok, Revision.t()} | {:error, ElixirDB.Error.t()}
  def find_revision(state, document_id, revision_id) do
    case get_in(state.revisions, [document_id, revision_id]) do
      %{revision: revision} ->
        {:ok, revision}

      nil ->
        {:error, ElixirDB.Error.revision_not_found("revision not found", %{revision: revision_id})}
    end
  end

  @doc "Lists leaf revisions ordered by revision id."
  @spec list_leaves(state(), binary()) :: [Revision.t()]
  def list_leaves(state, document_id) do
    state.revisions
    |> Map.get(document_id, %{})
    |> Map.values()
    |> Enum.filter(& &1.is_leaf)
    |> Enum.map(& &1.revision)
    |> Enum.sort_by(& &1.revision_id)
  end

  @doc "Updates the winning projection."
  @spec update_winning(state(), binary(), Revision.t(), non_neg_integer()) ::
          {:ok, state()} | {:error, ElixirDB.Error.t()}
  def update_winning(state, document_id, %Revision{} = winner, sequence) do
    case Map.fetch(state.documents, document_id) do
      :error ->
        {:error, ElixirDB.Error.document_not_found("document not found")}

      {:ok, doc} ->
        documents =
          Map.put(state.documents, document_id, %{
            doc
            | winning_revision: winner.revision_id,
              winning_deleted: winner.deleted,
              update_sequence: sequence,
              body: if(winner.deleted, do: nil, else: winner.body)
          })

        {:ok, %{state | documents: documents}}
    end
  end

  @doc "Empties a document after history purge."
  @spec empty_document(state(), binary()) :: {:ok, state()} | {:error, ElixirDB.Error.t()}
  def empty_document(state, document_id) do
    case Map.fetch(state.documents, document_id) do
      :error ->
        {:error, ElixirDB.Error.document_not_found("document not found")}

      {:ok, _doc} ->
        documents =
          Map.put(state.documents, document_id, %{
            winning_revision: nil,
            winning_deleted: true,
            update_sequence: 0,
            body: nil
          })

        {:ok, %{state | documents: documents}}
    end
  end

  @doc "Deletes all revisions for a history id."
  @spec delete_history(state(), binary(), binary()) :: {:ok, state()}
  def delete_history(state, document_id, history_id) do
    by_doc =
      state.revisions
      |> Map.get(document_id, %{})
      |> Enum.reject(fn {_id, %{revision: revision}} -> revision.history_id == history_id end)
      |> Map.new()
      |> recompute_leaves()

    {:ok, %{state | revisions: Map.put(state.revisions, document_id, by_doc)}}
  end

  @doc """
  Lists documents with `update_sequence <= through` and their full revision lists.
  """
  @spec list_compaction_documents(state(), non_neg_integer()) :: [map()]
  def list_compaction_documents(state, through)
      when is_integer(through) and through >= 0 do
    state.documents
    |> Enum.filter(fn {_id, doc} -> doc.update_sequence <= through end)
    |> Enum.map(fn {document_id, doc} ->
      revisions =
        state.revisions
        |> Map.get(document_id, %{})
        |> Map.values()
        |> Enum.map(& &1.revision)

      %{
        document_id: document_id,
        latest_change_sequence: doc.update_sequence,
        winning_revision: doc.winning_revision,
        revisions: revisions
      }
    end)
  end

  @doc "Deletes named revisions and recomputes leaf markers for the document."
  @spec delete_revisions(state(), binary(), [binary()]) :: {:ok, state()}
  def delete_revisions(state, document_id, revision_ids)
      when is_binary(document_id) and is_list(revision_ids) do
    drop = MapSet.new(revision_ids)

    by_doc =
      state.revisions
      |> Map.get(document_id, %{})
      |> Enum.reject(fn {id, _} -> MapSet.member?(drop, id) end)
      |> Map.new()
      |> recompute_leaves()

    {:ok, %{state | revisions: Map.put(state.revisions, document_id, by_doc)}}
  end

  @doc "Applies a shared compaction effect map to in-memory state."
  @spec apply_compaction_effect(state(), map()) :: {:ok, state()} | {:error, ElixirDB.Error.t()}
  def apply_compaction_effect(state, effect) when is_map(effect) do
    source_uuid = Map.fetch!(effect, :source_database_uuid)
    source_epoch = Map.fetch!(effect, :source_history_epoch)
    new_epoch = Map.fetch!(effect, :new_compaction_epoch)

    with {:ok, state} <- persist_expired_peers(state, Map.get(effect, :peers_to_expire, [])),
         {:ok, state} <-
           upsert_boundaries(
             state,
             Map.get(effect, :boundaries_to_upsert, []),
             source_uuid,
             source_epoch,
             new_epoch
           ),
         {:ok, state} <-
           remove_boundaries(state, Map.get(effect, :boundaries_to_remove, []), source_uuid),
         {:ok, state} <- delete_revision_removals(state, Map.get(effect, :removals, %{})),
         {:ok, state} <- empty_documents(state, Map.get(effect, :documents_to_empty, [])),
         {:ok, state} <- delete_changes(state, Map.get(effect, :delete_changes_through, 0)) do
      digest =
        Map.get(effect, :boundary_digest) ||
          digest_for_source(state, source_uuid)

      identity =
        state.identity
        |> Map.put(:retention_floor_sequence, Map.fetch!(effect, :new_floor))
        |> Map.put(:compaction_epoch, new_epoch)
        |> Map.put(:retention_boundary_digest, digest)

      state = %{state | identity: identity}

      state =
        if Map.get(effect, :increment_maintenance?, false),
          do: %{state | maintenance_counter: state.maintenance_counter + 1},
          else: state

      {:ok, %{state | compaction_result: Map.get(effect, :result_stats, %{})}}
    end
  rescue
    KeyError ->
      {:error, ElixirDB.Error.invalid_request("compaction effect is incomplete")}
  end

  @doc "Compare-and-swaps a peer ledger wire value."
  @spec put_peer_cas(state(), binary(), non_neg_integer(), map()) ::
          {:ok, state(), map()} | {:error, ElixirDB.Error.t()}
  def put_peer_cas(state, peer_uuid, expected, value)
      when is_binary(peer_uuid) and is_integer(expected) and expected >= 0 and is_map(value) do
    current = Map.get(state.peers, peer_uuid)
    observed = if is_nil(current), do: 0, else: current.version

    with {:ok, json} <- Canonical.encode(value),
         :ok <- validate_peer_cas(expected, observed, current, json),
         {:ok, next_version, replayed} <- next_peer_version(expected, observed, current, json) do
      peers =
        Map.put(state.peers, peer_uuid, %{version: next_version, value: value})

      {:ok, %{state | peers: peers}, %{version: next_version, value: value, replayed: replayed}}
    else
      {:error, _} = error -> error
    end
  end

  def put_peer_cas(_state, _peer_uuid, _expected, _value),
    do: {:error, ElixirDB.Error.invalid_request("peer position CAS fields are invalid")}

  @doc "Upserts a peer wire without bumping the record version."
  @spec update_peer_wire(state(), binary(), map()) :: {:ok, state()}
  def update_peer_wire(state, peer_uuid, wire)
      when is_binary(peer_uuid) and is_map(wire) do
    peers =
      case Map.fetch(state.peers, peer_uuid) do
        {:ok, entry} ->
          Map.put(state.peers, peer_uuid, %{entry | value: wire})

        :error ->
          Map.put(state.peers, peer_uuid, %{version: 1, value: wire})
      end

    {:ok, %{state | peers: peers}}
  end

  @doc "Allocates contiguous change sequences."
  @spec allocate_sequences(state(), non_neg_integer()) :: {:ok, state(), [integer()]}
  def allocate_sequences(state, 0), do: {:ok, state, []}

  def allocate_sequences(state, count) when count > 0 do
    current = state.identity.current_sequence
    sequences = Enum.to_list((current + 1)..(current + count))
    identity = %{state.identity | current_sequence: current + count}
    {:ok, %{state | identity: identity}, sequences}
  end

  @doc "Appends a change-log entry."
  @spec append_change(state(), map()) :: {:ok, state()} | {:error, ElixirDB.Error.t()}
  def append_change(state, entry) when is_map(entry) do
    winner = Map.fetch!(entry, :winner)

    change = %{
      sequence: Map.fetch!(entry, :sequence),
      document_id: Map.fetch!(entry, :document_id),
      winning_revision: winner.revision_id,
      winning_deleted: winner.deleted,
      leaf_set_json: Map.fetch!(entry, :leaf_json),
      origin: Map.get(entry, :origin, "local")
    }

    {:ok, %{state | changes: state.changes ++ [change]}}
  end

  defp empty_state(root, identity) do
    %{
      root: root,
      identity: identity,
      documents: %{},
      revisions: %{},
      changes: [],
      boundaries: [],
      peers: %{},
      boundary_install_state: %{},
      boundary_staging: %{},
      maintenance_counter: 0,
      compaction_result: nil,
      checkpoints: %{},
      pending_local_causal: false,
      pending_blobs: %{},
      closed?: false
    }
  end

  defp refresh_identity(state), do: state.identity

  defp persist_expired_peers(state, []), do: {:ok, state}

  defp persist_expired_peers(state, peers) do
    Enum.reduce_while(peers, {:ok, state}, fn peer, {:ok, acc} ->
      wire = peer_wire(peer)

      case update_peer_wire(acc, peer.peer_database_uuid, wire) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
      end
    end)
  end

  defp peer_wire(%PeerPosition{} = peer) do
    %{
      "peer_database_uuid" => peer.peer_database_uuid,
      "peer_history_epoch" => peer.peer_history_epoch,
      "source_database_uuid" => peer.source_database_uuid,
      "source_history_epoch" => peer.source_history_epoch,
      "safe_source_sequence" => peer.safe_source_sequence,
      "installed_source_compaction_epoch" => peer.installed_source_compaction_epoch,
      "last_seen_at" => peer.last_seen_at,
      "lease_expires_at" => peer.lease_expires_at,
      "status" => Atom.to_string(peer.status)
    }
  end

  defp peer_wire(peer) when is_map(peer) do
    %{
      "peer_database_uuid" => MapAccess.get(peer, :peer_database_uuid),
      "peer_history_epoch" => MapAccess.get(peer, :peer_history_epoch),
      "source_database_uuid" => MapAccess.get(peer, :source_database_uuid),
      "source_history_epoch" => MapAccess.get(peer, :source_history_epoch),
      "safe_source_sequence" => MapAccess.get(peer, :safe_source_sequence),
      "installed_source_compaction_epoch" =>
        MapAccess.get(peer, :installed_source_compaction_epoch),
      "last_seen_at" => MapAccess.get(peer, :last_seen_at),
      "lease_expires_at" => MapAccess.get(peer, :lease_expires_at),
      "status" =>
        case MapAccess.get(peer, :status) do
          status when is_atom(status) -> Atom.to_string(status)
          status -> status
        end
    }
  end

  defp upsert_boundaries(state, [], _source_uuid, _source_epoch, _epoch), do: {:ok, state}

  defp upsert_boundaries(state, boundaries, source_uuid, source_epoch, epoch) do
    Enum.reduce_while(boundaries, {:ok, state}, fn boundary, {:ok, acc} ->
      stored = %{
        source_database_uuid: source_uuid,
        source_history_epoch: source_epoch,
        compaction_epoch: epoch,
        boundary: boundary
      }

      {:cont, {:ok, %{acc | boundaries: upsert_boundary_list(acc.boundaries, stored)}}}
    end)
  end

  defp remove_boundaries(state, [], _source_uuid), do: {:ok, state}

  defp remove_boundaries(state, boundaries, source_uuid) do
    keys =
      MapSet.new(boundaries, fn boundary ->
        {source_uuid, boundary.document_id, boundary.history_id}
      end)

    filtered =
      Enum.reject(state.boundaries, fn stored ->
        MapSet.member?(
          keys,
          {stored.source_database_uuid, stored.boundary.document_id, stored.boundary.history_id}
        )
      end)

    {:ok, %{state | boundaries: filtered}}
  end

  defp delete_revision_removals(state, removals) when map_size(removals) == 0, do: {:ok, state}

  defp delete_revision_removals(state, removals) do
    Enum.reduce_while(removals, {:ok, state}, fn {document_id, revision_ids}, {:ok, acc} ->
      case delete_revisions(acc, document_id, revision_ids) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
      end
    end)
  end

  defp empty_documents(state, []), do: {:ok, state}

  defp empty_documents(state, document_ids) do
    Enum.reduce_while(document_ids, {:ok, state}, fn document_id, {:ok, acc} ->
      case empty_document(acc, document_id) do
        {:ok, new_state} -> {:cont, {:ok, new_state}}
        {:error, %ElixirDB.Error{code: :document_not_found}} -> {:cont, {:ok, acc}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp delete_changes(state, 0), do: {:ok, state}

  defp delete_changes(state, through) when is_integer(through) and through > 0 do
    {:ok, %{state | changes: Enum.reject(state.changes, &(&1.sequence <= through))}}
  end

  defp digest_for_source(state, source_uuid) do
    state.boundaries
    |> Enum.filter(&(&1.source_database_uuid == source_uuid))
    |> Enum.map(& &1.boundary)
    |> BoundaryPage.digest_for()
  end

  defp upsert_boundary_list(list, stored) do
    key = {stored.source_database_uuid, stored.boundary.document_id, stored.boundary.history_id}

    list
    |> Enum.reject(fn existing ->
      {existing.source_database_uuid, existing.boundary.document_id, existing.boundary.history_id} ==
        key
    end)
    |> Kernel.++([stored])
  end

  defp validate_peer_cas(expected, observed, current, json)
       when observed != expected and not is_nil(current) do
    if Canonical.encode!(current.value) == json do
      :ok
    else
      {:error,
       ElixirDB.Error.checkpoint_conflict("local record version is stale", %{
         expected_version: expected,
         observed_version: observed
       })}
    end
  end

  defp validate_peer_cas(expected, observed, nil, _json) when observed != expected do
    {:error,
     ElixirDB.Error.checkpoint_conflict("local record version is stale", %{
       expected_version: expected,
       observed_version: observed
     })}
  end

  defp validate_peer_cas(_expected, _observed, _current, _json), do: :ok

  defp next_peer_version(expected, observed, current, json)
       when observed != expected and not is_nil(current) do
    if Canonical.encode!(current.value) == json,
      do: {:ok, observed, true},
      else: {:error, ElixirDB.Error.checkpoint_conflict("local record version is stale")}
  end

  defp next_peer_version(expected, observed, _current, _json) when expected == observed,
    do: {:ok, expected + 1, false}

  defp next_peer_version(_expected, _observed, _current, _json),
    do: {:error, ElixirDB.Error.checkpoint_conflict("local record version is stale")}

  defp shape_document(document_id, doc) do
    %{
      document_id: document_id,
      winning_revision: doc.winning_revision,
      winning_deleted: doc.winning_deleted,
      update_sequence: doc.update_sequence,
      body: doc.body,
      backend_meta: %{doc_key: document_id}
    }
  end

  defp clear_parent_leaf(by_doc, nil), do: by_doc

  defp clear_parent_leaf(by_doc, parent) do
    case Map.get(by_doc, parent) do
      nil -> by_doc
      entry -> Map.put(by_doc, parent, %{entry | is_leaf: false})
    end
  end

  defp recompute_leaves(by_doc) do
    parents =
      by_doc
      |> Map.values()
      |> Enum.map(& &1.revision.parent_revision)
      |> Enum.filter(&is_binary/1)
      |> MapSet.new()

    Map.new(by_doc, fn {id, entry} ->
      {id, %{entry | is_leaf: not MapSet.member?(parents, id)}}
    end)
  end

  defp snapshot_path(root), do: Path.join(root, @snapshot_file)

  defp persist(state) do
    path = snapshot_path(state.root)
    File.mkdir_p!(state.root)

    case File.write(path, :erlang.term_to_binary(%{state | closed?: true})) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, ElixirDB.Error.internal_error("cannot persist memory store: #{inspect(reason)}")}
    end
  end

  defp decode_snapshot(binary) do
    state = :erlang.binary_to_term(binary, [:safe])

    if is_map(state) and Map.has_key?(state, :identity) do
      {:ok, migrate_state(state)}
    else
      {:error, ElixirDB.Error.invalid_request("memory backend snapshot is invalid")}
    end
  rescue
    _ -> {:error, ElixirDB.Error.invalid_request("memory backend snapshot is invalid")}
  end

  defp migrate_state(state) do
    defaults = empty_state(Map.get(state, :root, ""), Map.fetch!(state, :identity))
    Map.merge(defaults, state)
  end

  @doc false
  def same?(a, b), do: Compare.same?(a, b)
end
