defmodule ElixirDB.Integrity.Rules do
  @moduledoc """
  Shared logical integrity rules over a normalized storage snapshot.

  Engine-specific probes and external blob checks remain backend-owned and are
  reported separately as backend details.
  """

  alias ElixirDB.Domain.{BoundaryPage, Checkpoint, PeerPosition, RetentionBoundary}
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Id

  @type snapshot :: %{
          required(:meta) => map(),
          optional(:boundaries) => [map()],
          optional(:peers) => [PeerPosition.t()],
          optional(:maintenance_counter) => non_neg_integer() | nil,
          optional(:documents) => [map()],
          optional(:revisions) => [map()],
          optional(:changes) => [map()],
          optional(:pending_blobs) => [map()],
          optional(:checkpoints) => [map()],
          optional(:revision_attachments) => [map()],
          optional(:indexes) => [map()]
        }

  @doc """
  Validates logical consistency of a normalized integrity snapshot.

  Returns `:ok` when all shared rules pass, otherwise an integrity violation
  with the same error identity used by the product integrity path.
  """
  @spec validate(map()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate(snapshot) when is_map(snapshot) do
    meta = Map.fetch!(snapshot, :meta)
    boundaries = Map.get(snapshot, :boundaries, [])
    peers = Map.get(snapshot, :peers, [])
    documents = Map.get(snapshot, :documents, [])
    revisions = Map.get(snapshot, :revisions, [])
    changes = Map.get(snapshot, :changes, [])
    pending_blobs = Map.get(snapshot, :pending_blobs, [])
    checkpoints = Map.get(snapshot, :checkpoints, [])

    with :ok <- validate_db_meta(meta),
         :ok <- validate_boundary_records(boundaries, meta),
         :ok <- validate_peer_records(peers, meta),
         :ok <- validate_retention_maintenance(snapshot),
         :ok <- validate_revision_rows(revisions, boundaries),
         :ok <- validate_revision_attachments(snapshot, revisions),
         :ok <- validate_pending_blobs(pending_blobs),
         :ok <- validate_document_rows(documents, revisions),
         :ok <- validate_change_rows(changes, revisions, meta) do
      validate_checkpoints(checkpoints, meta)
    end
  end

  defp validate_db_meta(meta) when is_map(meta) do
    validators = [
      fn -> validate_uuid(Map.get(meta, :database_uuid)) end,
      fn -> validate_history_epoch(Map.get(meta, :history_epoch)) end,
      fn -> validate_non_negative(Map.get(meta, :current_sequence), "current_sequence") end,
      fn ->
        validate_non_negative(Map.get(meta, :retention_floor_sequence), "retention_floor_sequence")
      end,
      fn -> validate_non_negative(Map.get(meta, :compaction_epoch), "compaction_epoch") end,
      fn -> validate_floor_within_sequence(meta) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_uuid(uuid) when is_binary(uuid) and uuid != "", do: :ok

  defp validate_uuid(_),
    do: {:error, ElixirDB.Error.integrity_violation("database UUID is invalid")}

  defp validate_history_epoch(epoch) when is_binary(epoch) and epoch != "", do: :ok

  defp validate_history_epoch(_),
    do: {:error, ElixirDB.Error.integrity_violation("history epoch is invalid")}

  defp validate_non_negative(value, _field) when is_integer(value) and value >= 0, do: :ok

  defp validate_non_negative(_, field),
    do: {:error, ElixirDB.Error.integrity_violation("metadata field is invalid", %{field: field})}

  defp validate_floor_within_sequence(meta) do
    sequence = Map.get(meta, :current_sequence)
    floor = Map.get(meta, :retention_floor_sequence)

    if is_integer(floor) and is_integer(sequence) and floor <= sequence,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.integrity_violation("retention floor exceeds current sequence", %{
           floor: floor,
           current_sequence: sequence
         })}
  end

  defp validate_boundary_records(boundaries, meta) when is_list(boundaries) do
    computed =
      boundaries
      |> Enum.map(& &1.boundary)
      |> BoundaryPage.digest_for()

    stored = Map.get(meta, :retention_boundary_digest)

    with :ok <- validate_boundary_digest(stored, computed, boundaries) do
      validate_boundary_entries(boundaries)
    end
  end

  defp validate_boundary_digest(stored, computed, boundaries) do
    cond do
      boundaries == [] and stored in [nil, ""] ->
        :ok

      is_binary(stored) and stored == computed ->
        :ok

      true ->
        {:error, ElixirDB.Error.integrity_violation("retention boundary digest mismatch")}
    end
  end

  defp validate_boundary_entries(boundaries) do
    Enum.reduce_while(boundaries, :ok, fn entry, :ok ->
      boundary = entry.boundary
      epoch = Map.get(entry, :compaction_epoch)

      with true <- is_integer(epoch) and epoch >= 0,
           true <- is_binary(boundary.history_id) and boundary.history_id != "",
           true <- is_list(boundary.retired_branch_roots),
           :ok <- validate_retired_branch_roots(boundary) do
        {:cont, :ok}
      else
        false ->
          {:halt,
           {:error, ElixirDB.Error.integrity_violation("retention boundary record is invalid")}}

        {:error, error} ->
          {:halt, {:error, error}}
      end
    end)
  end

  defp validate_retired_branch_roots(%RetentionBoundary{retired_branch_roots: roots}) do
    if Enum.all?(roots, &(is_binary(&1) and &1 != "")),
      do: :ok,
      else: {:error, ElixirDB.Error.integrity_violation("retention boundary roots are invalid")}
  end

  defp validate_peer_records(peers, meta) when is_list(peers) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      case validate_peer_record(peer, meta) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_peer_record(%PeerPosition{} = peer, meta) do
    cond do
      peer.source_database_uuid != Map.get(meta, :database_uuid) ->
        {:error, ElixirDB.Error.integrity_violation("peer source database UUID mismatch")}

      peer.source_history_epoch != Map.get(meta, :history_epoch) ->
        {:error, ElixirDB.Error.integrity_violation("peer source history epoch mismatch")}

      peer.safe_source_sequence > Map.get(meta, :current_sequence) ->
        {:error, ElixirDB.Error.integrity_violation("peer safe sequence exceeds source sequence")}

      peer.installed_source_compaction_epoch > Map.get(meta, :compaction_epoch) ->
        {:error, ElixirDB.Error.integrity_violation("peer installed compaction epoch is invalid")}

      peer.safe_source_sequence < Map.get(meta, :retention_floor_sequence) and
          peer.status == :active ->
        {:error,
         ElixirDB.Error.integrity_violation("active peer safe sequence is below retention floor")}

      true ->
        :ok
    end
  end

  defp validate_retention_maintenance(snapshot) do
    case Map.fetch(snapshot, :maintenance_counter) do
      :error ->
        :ok

      {:ok, nil} ->
        :ok

      {:ok, counter} when is_integer(counter) and counter >= 0 ->
        :ok

      {:ok, _} ->
        {:error, ElixirDB.Error.integrity_violation("retention maintenance counter is invalid")}
    end
  end

  defp validate_revision_rows(revisions, boundaries) when is_list(revisions) do
    by_key = revision_index(revisions)
    children = child_parents(revisions)

    Enum.reduce_while(revisions, :ok, fn row, :ok ->
      case validate_revision_row(row, boundaries, by_key, children) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp revision_index(revisions) do
    Map.new(revisions, fn row -> {{row.document_id, row.revision_id}, row} end)
  end

  defp child_parents(revisions) do
    Enum.reduce(revisions, MapSet.new(), fn
      %{parent: parent}, acc when is_binary(parent) and parent != "" -> MapSet.put(acc, parent)
      _, acc -> acc
    end)
  end

  defp validate_revision_row(row, boundaries, by_key, children) do
    document_id = row.document_id
    revision_id = row.revision_id
    generation = row.generation
    parent = Map.get(row, :parent)
    history_id = row.history_id
    digest = Map.get(row, :digest)
    deleted = truthy?(Map.get(row, :deleted))
    body = Map.get(row, :body)
    attachments = Map.get(row, :attachments) || %{}
    is_leaf = truthy?(Map.get(row, :is_leaf))

    with true <- is_binary(history_id) and history_id != "",
         :ok <- validate_revision_body_shape(deleted, body),
         :ok <- validate_tombstone_attachments(deleted, attachments, revision_id),
         {:ok, calculated} <-
           Id.calculate(document_id, history_id, parent, deleted, body, attachments),
         true <- calculated == revision_id,
         true <- digest == revision_digest_part(revision_id),
         {:ok, expected_generation} <- Id.generation(revision_id),
         true <- expected_generation == generation,
         :ok <-
           validate_parent_row(
             by_key,
             boundaries,
             document_id,
             revision_id,
             generation,
             history_id,
             parent
           ),
         :ok <- validate_leaf_row(revision_id, is_leaf, children) do
      :ok
    else
      false ->
        {:error,
         ElixirDB.Error.integrity_violation("revision identity or generation is invalid", %{
           revision: revision_id
         })}

      {:error, error} ->
        {:error, error}
    end
  end

  defp validate_revision_body_shape(true, nil), do: :ok

  defp validate_revision_body_shape(true, _body),
    do: {:error, ElixirDB.Error.integrity_violation("deleted revision has a body term")}

  defp validate_revision_body_shape(false, body) when is_map(body), do: :ok

  defp validate_revision_body_shape(false, _body),
    do:
      {:error,
       ElixirDB.Error.integrity_violation("revision identity or generation is invalid", %{})}

  defp validate_tombstone_attachments(true, attachments, revision_id)
       when attachments != %{} and not is_nil(attachments) do
    {:error,
     ElixirDB.Error.integrity_violation(
       "tombstone revisions must have an empty attachment manifest",
       %{revision: revision_id}
     )}
  end

  defp validate_tombstone_attachments(_deleted, _attachments, _revision_id), do: :ok

  defp revision_digest_part(revision_id) do
    case String.split(revision_id, "-", parts: 2) do
      [_generation, digest] when is_binary(digest) and digest != "" -> digest
      _ -> nil
    end
  end

  defp validate_parent_row(
         _by_key,
         _boundaries,
         _document_id,
         _revision_id,
         _generation,
         _history_id,
         nil
       ),
       do: :ok

  defp validate_parent_row(
         by_key,
         boundaries,
         document_id,
         revision_id,
         generation,
         history_id,
         parent
       ) do
    case Map.fetch(by_key, {document_id, parent}) do
      {:ok, _} ->
        :ok

      :error ->
        if truncated_parent_allowed?(boundaries, document_id, history_id, generation),
          do: :ok,
          else:
            {:error,
             ElixirDB.Error.integrity_violation("revision has a dangling parent", %{
               revision: revision_id,
               parent_revision: parent
             })}
    end
  end

  defp truncated_parent_allowed?(boundaries, document_id, history_id, generation) do
    Enum.any?(boundaries, fn %{boundary: boundary} ->
      boundary.document_id == document_id and boundary.history_id == history_id and
        (boundary.retired or
           (is_integer(boundary.minimum_retained_generation) and
              generation >= boundary.minimum_retained_generation))
    end)
  end

  defp validate_leaf_row(revision_id, is_leaf, children) do
    expected_leaf = not MapSet.member?(children, revision_id)

    if is_leaf == expected_leaf,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.integrity_violation("revision leaf marker is stale", %{
           revision: revision_id
         })}
  end

  @doc """
  Flattens revision attachment maps into integrity snapshot rows.

  Each row carries document/revision identity plus digest, logical size, and
  content type from the manifest entry.
  """
  @spec flatten_revision_attachments([map()]) :: [map()]
  def flatten_revision_attachments(revisions) when is_list(revisions) do
    Enum.flat_map(revisions, fn row ->
      (Map.get(row, :attachments) || %{})
      |> Enum.map(fn {name, entry} ->
        %{
          document_id: row.document_id,
          revision_id: row.revision_id,
          name: name,
          digest: MapAccess.get(entry, :digest),
          logical_size: MapAccess.get(entry, :length) || MapAccess.get(entry, :logical_size),
          content_type: MapAccess.get(entry, :content_type)
        }
      end)
    end)
  end

  @doc """
  Ensures each attachment digest reports a single logical size across rows.

  Accepts maps with `:digest` / `:logical_size` or `{digest, size}` tuples.
  """
  @spec validate_digest_size_consistency([map() | {term(), term()}]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def validate_digest_size_consistency(rows) when is_list(rows) do
    rows
    |> Enum.map(&digest_size_pair/1)
    |> Enum.group_by(fn {digest, _} -> digest end, fn {_, size} -> size end)
    |> Enum.reduce_while(:ok, fn {digest, sizes}, :ok ->
      case Enum.uniq(sizes) do
        [_] ->
          {:cont, :ok}

        uniq ->
          {:halt,
           {:error,
            ElixirDB.Error.integrity_violation(
              "attachment digest has inconsistent logical sizes across retained manifests",
              %{digest: digest, sizes: uniq}
            )}}
      end
    end)
  end

  defp validate_revision_attachments(snapshot, revisions) do
    flat =
      case Map.get(snapshot, :revision_attachments) do
        list when is_list(list) -> list
        _ -> flatten_revision_attachments(revisions)
      end

    revision_keys = MapSet.new(revisions, fn row -> {row.document_id, row.revision_id} end)

    with :ok <- validate_attachment_orphans(flat, revision_keys),
         :ok <- validate_attachment_fields(flat),
         :ok <- validate_digest_size_consistency(flat) do
      validate_attachment_digests(flat)
    end
  end

  defp digest_size_pair({digest, size}), do: {digest, size}

  defp digest_size_pair(row) when is_map(row),
    do: {Map.get(row, :digest), Map.get(row, :logical_size)}

  defp validate_attachment_orphans(rows, revision_keys) do
    case Enum.find(rows, fn row ->
           document_id = Map.get(row, :document_id)
           revision_id = Map.get(row, :revision_id)

           not (is_binary(document_id) and is_binary(revision_id) and
                  MapSet.member?(revision_keys, {document_id, revision_id}))
         end) do
      nil ->
        :ok

      _ ->
        {:error,
         ElixirDB.Error.integrity_violation(
           "revision_attachments row references a missing revision"
         )}
    end
  end

  defp validate_attachment_fields(rows) do
    invalid? =
      Enum.any?(rows, fn row ->
        digest = Map.get(row, :digest)
        size = Map.get(row, :logical_size)
        content_type = Map.get(row, :content_type)

        not (is_integer(size) and size >= 0 and is_binary(digest) and byte_size(digest) == 64 and
               is_binary(content_type) and content_type != "")
      end)

    if invalid?,
      do:
        {:error, ElixirDB.Error.integrity_violation("revision_attachments row fields are invalid")},
      else: :ok
  end

  defp validate_attachment_digests(rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      digest = Map.get(row, :digest)

      if valid_digest?(digest) do
        {:cont, :ok}
      else
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("revision_attachments digest is invalid", %{
            digest: digest
          })}}
      end
    end)
  end

  defp validate_pending_blobs(rows) when is_list(rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      digest = Map.get(row, :digest)
      size = Map.get(row, :logical_size)
      expires_at = Map.get(row, :expires_at)
      updated_at = Map.get(row, :updated_at)

      if valid_digest?(digest) and is_integer(size) and size >= 0 and valid_iso8601?(expires_at) and
           valid_iso8601?(updated_at) do
        {:cont, :ok}
      else
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("pending_blobs row fields are invalid", %{
            digest: digest
          })}}
      end
    end)
  end

  defp valid_digest?(digest) when is_binary(digest),
    do: Regex.match?(~r/^[0-9a-f]{64}$/, digest)

  defp valid_digest?(_), do: false

  defp valid_iso8601?(value) when is_binary(value) do
    match?({:ok, _, _}, DateTime.from_iso8601(value))
  end

  defp valid_iso8601?(_), do: false

  defp validate_document_rows(documents, revisions) when is_list(documents) do
    by_key = revision_index(revisions)

    Enum.reduce_while(documents, :ok, fn doc, :ok ->
      case validate_document_row(doc, by_key) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_document_row(doc, by_key) do
    winning = Map.get(doc, :winning_revision)
    deleted = truthy?(Map.get(doc, :winning_deleted))
    sequence = Map.get(doc, :update_sequence)
    body = Map.get(doc, :body)
    document_id = Map.get(doc, :document_id)

    cond do
      empty_document_row?(winning, deleted, sequence, body) ->
        :ok

      is_nil(winning) ->
        {:error,
         ElixirDB.Error.integrity_violation("document has no winning revision", %{
           document_id: document_id
         })}

      true ->
        validate_winner_row(by_key, document_id, winning, body, deleted)
    end
  end

  defp empty_document_row?(nil, true, 0, nil), do: true
  defp empty_document_row?(_winning, _deleted, _sequence, _body), do: false

  defp validate_winner_row(by_key, document_id, winning, body, deleted) do
    case Map.fetch(by_key, {document_id, winning}) do
      {:ok, revision} ->
        if winner_matches?(revision, body, deleted) do
          :ok
        else
          {:error,
           ElixirDB.Error.integrity_violation("materialized winner is inconsistent", %{
             document_id: document_id
           })}
        end

      :error ->
        {:error,
         ElixirDB.Error.integrity_violation("document winner is missing", %{
           document_id: document_id
         })}
    end
  end

  defp winner_matches?(revision, body, deleted) do
    revision_deleted = truthy?(Map.get(revision, :deleted))
    revision_body = Map.get(revision, :body)

    if revision_deleted do
      is_nil(body) and deleted
    else
      not deleted and bodies_equal?(revision_body, body)
    end
  end

  defp bodies_equal?(left, right) when is_map(left) and is_map(right) do
    Canonical.encode!(left) == Canonical.encode!(right)
  end

  defp bodies_equal?(nil, nil), do: true
  defp bodies_equal?(_, _), do: false

  defp validate_change_rows(changes, revisions, meta) when is_list(changes) do
    floor = Map.get(meta, :retention_floor_sequence)
    by_key = revision_index(revisions)

    changes
    |> Enum.sort_by(&Map.get(&1, :sequence))
    |> Enum.reduce_while({:ok, nil}, fn change, {:ok, previous} ->
      sequence = Map.get(change, :sequence)
      document_id = Map.get(change, :document_id)
      winning = Map.get(change, :winning_revision)
      leaves = Map.get(change, :leaf_revisions) || []

      with :ok <- validate_change_sequence(sequence, previous, floor),
           :ok <- validate_change_leaves(by_key, document_id, winning, leaves) do
        {:cont, {:ok, sequence}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, _} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp validate_change_sequence(sequence, _previous, floor) when sequence <= floor,
    do:
      {:error,
       ElixirDB.Error.integrity_violation("change row is at or below the retention floor", %{
         sequence: sequence,
         floor: floor
       })}

  defp validate_change_sequence(sequence, previous, _floor)
       when not is_nil(previous) and sequence <= previous,
       do:
         {:error,
          ElixirDB.Error.integrity_violation("change sequences are not strictly increasing", %{
            sequence: sequence,
            previous: previous
          })}

  defp validate_change_sequence(_sequence, _previous, _floor), do: :ok

  defp validate_change_leaves(by_key, document_id, winning, leaves) when is_list(leaves) do
    Enum.reduce_while(leaves, :ok, fn leaf, :ok ->
      validate_change_leaf(by_key, document_id, leaf)
    end)
    |> case do
      :ok ->
        case Map.fetch(by_key, {document_id, winning}) do
          {:ok, _} ->
            :ok

          :error ->
            {:error,
             ElixirDB.Error.integrity_violation("change winner is missing", %{revision: winning})}
        end

      error ->
        error
    end
  end

  defp validate_change_leaves(_by_key, _document_id, _winning, _leaves),
    do: {:error, ElixirDB.Error.integrity_violation("change JSON and term differ")}

  defp validate_change_leaf(_by_key, document_id, leaf) when not is_map(leaf),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("change leaf entry is not an object", %{
          document_id: document_id
        })}}

  defp validate_change_leaf(by_key, document_id, leaf) do
    revision = MapAccess.get(leaf, :revision)

    if is_binary(revision) and revision != "",
      do: validate_change_leaf_revision(by_key, document_id, leaf, revision),
      else:
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change leaf revision is invalid", %{
            document_id: document_id
          })}}
  end

  defp validate_change_leaf_revision(by_key, document_id, leaf, revision) do
    case Map.fetch(by_key, {document_id, revision}) do
      {:ok, row} ->
        supplied_deleted = MapAccess.get(leaf, :deleted)
        supplied_history_id = MapAccess.get(leaf, :history_id)
        deleted = truthy?(Map.get(row, :deleted))
        history_id = Map.get(row, :history_id)

        validate_change_leaf_marker(
          supplied_deleted == deleted and supplied_history_id == history_id,
          supplied_deleted,
          revision
        )

      :error ->
        {:halt,
         {:error,
          ElixirDB.Error.integrity_violation("change references a missing revision", %{
            revision: revision
          })}}
    end
  end

  defp validate_change_leaf_marker(true, supplied_deleted, _revision)
       when is_boolean(supplied_deleted),
       do: {:cont, :ok}

  defp validate_change_leaf_marker(_matches, _supplied_deleted, revision),
    do:
      {:halt,
       {:error,
        ElixirDB.Error.integrity_violation("change leaf deletion marker is stale", %{
          revision: revision
        })}}

  defp validate_checkpoints(rows, meta) when is_list(rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      value = Map.get(row, :value, row)

      case validate_checkpoint_value(value, meta) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp validate_checkpoint_value(value, meta) when is_map(value) do
    with {:ok, checkpoint} <- Checkpoint.from_wire(value) do
      validate_checkpoint_fields(checkpoint, meta)
    end
  end

  defp validate_checkpoint_value(_value, _meta),
    do: {:error, ElixirDB.Error.integrity_violation("checkpoint record is invalid")}

  defp validate_checkpoint_fields(%Checkpoint{} = checkpoint, _meta) do
    validators = [
      fn -> validate_checkpoint_safe_sequence(checkpoint) end,
      fn -> validate_checkpoint_installed_epoch(checkpoint) end,
      fn -> validate_checkpoint_history(checkpoint) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_checkpoint_safe_sequence(checkpoint) do
    if checkpoint.safe_source_sequence > checkpoint.source_sequence,
      do:
        {:error,
         ElixirDB.Error.integrity_violation("checkpoint safe sequence exceeds source sequence")}
  end

  defp validate_checkpoint_installed_epoch(checkpoint) do
    if checkpoint.installed_source_compaction_epoch > checkpoint.source_compaction_epoch,
      do:
        {:error,
         ElixirDB.Error.integrity_violation("checkpoint installed compaction epoch regressed")}
  end

  defp validate_checkpoint_history(checkpoint) do
    if checkpoint_history_monotonic?(checkpoint.history, checkpoint.source_sequence),
      do: nil,
      else: {:error, ElixirDB.Error.integrity_violation("checkpoint history regressed")}
  end

  defp checkpoint_history_monotonic?(history, source_sequence) when is_list(history) do
    history
    |> Enum.map(&checkpoint_history_sequence/1)
    |> descending_through?(source_sequence)
  end

  defp descending_through?(sequences, source_sequence) do
    Enum.reduce_while(sequences, nil, fn
      seq, _previous when not is_integer(seq) or seq < 0 or seq > source_sequence ->
        {:halt, :invalid}

      seq, nil ->
        {:cont, seq}

      seq, previous when seq <= previous ->
        {:cont, seq}

      _seq, _previous ->
        {:halt, :invalid}
    end) != :invalid
  end

  defp checkpoint_history_sequence(entry) when is_map(entry) do
    MapAccess.get(entry, :source_sequence)
  end

  defp checkpoint_history_sequence(_), do: nil

  defp truthy?(true), do: true
  defp truthy?(1), do: true
  defp truthy?(false), do: false
  defp truthy?(0), do: false
  defp truthy?(_), do: false
end
