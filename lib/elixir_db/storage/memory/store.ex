defmodule ElixirDB.Storage.Memory.Store do
  @moduledoc """
  In-memory fact store for the Memory storage backend.

  Holds normalized documents, revisions, changes, and retention markers in an
  Agent. Persists an Erlang-term snapshot under the bundle root so close/reopen
  semantic tests can reload state without SQL.
  """

  use Agent

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Revisions.Compare

  @snapshot_file "memory.backend"

  @type state :: %{
          root: binary(),
          identity: map(),
          documents: %{optional(binary()) => map()},
          revisions: %{optional(binary()) => %{optional(binary()) => Revision.t()}},
          changes: [map()],
          boundaries: [map()],
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
      pending_local_causal: false,
      pending_blobs: %{},
      closed?: false
    }
  end

  defp refresh_identity(state), do: state.identity

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
      {:ok, state}
    else
      {:error, ElixirDB.Error.invalid_request("memory backend snapshot is invalid")}
    end
  rescue
    _ -> {:error, ElixirDB.Error.invalid_request("memory backend snapshot is invalid")}
  end

  @doc false
  def same?(a, b), do: Compare.same?(a, b)
end
