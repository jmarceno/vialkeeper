defmodule ElixirDB.RevisionHistoryModel do
  @moduledoc """
  Pure in-memory revision history applicator for Phase 2 adapter↔model tests.

  Mirrors SQLite adapter semantics for local put/delete, sibling chain import,
  conflict resolution (surviving-body and delete-all), and exact replay using
  `Winner`, `ConflictResolution`, `Tree`, and `Id`.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Revisions.{ConflictResolution, Id, Tree, Winner}

  @type state :: %{
          document_id: binary() | nil,
          revisions: %{optional(binary()) => Revision.t()},
          last_result: term(),
          last_concrete_op: map() | nil
        }

  @doc """
  Empty pure-model document state.
  """
  @spec new() :: state()
  def new, do: %{document_id: nil, revisions: %{}, last_result: nil, last_concrete_op: nil}

  @doc """
  Applies one generated or concrete operation to pure-model state.
  """
  @spec apply_operation(state(), map()) :: state()
  def apply_operation(state, %{op: :replay_last}) do
    case state.last_concrete_op do
      nil -> %{state | last_result: {:error, :invalid_request}}
      op -> apply_operation(state, op)
    end
  end

  def apply_operation(state, %{op: :put} = op) do
    document_id = op.document_id
    body = op.body
    parent = resolve_parent(state, op[:if_revision])
    concrete = %{op: :put, document_id: document_id, body: body, if_revision: parent}

    with {:ok, revision} <- build_revision(document_id, parent, false, body) do
      cond do
        match_mutation?(state, parent, revision) ->
          state
          |> insert_revision(document_id, revision, replayed: false)
          |> Map.put(:last_concrete_op, concrete)

        identical_present?(state, revision) and winner_id(state) == revision.revision_id ->
          result = %{revision: revision.revision_id, replayed: true, conflicts: conflicts_of(state)}
          %{state | last_result: {:ok, result}, last_concrete_op: concrete}

        identical_present?(state, revision) ->
          %{state | last_result: {:error, :revision_conflict}, last_concrete_op: concrete}

        true ->
          %{state | last_result: {:error, :revision_conflict}, last_concrete_op: concrete}
      end
    else
      {:error, _} ->
        %{state | last_result: {:error, :invalid_request}}
    end
  end

  def apply_operation(state, %{op: :delete} = op) do
    document_id = op.document_id
    parent = resolve_parent(state, op[:if_revision])
    concrete = %{op: :delete, document_id: document_id, if_revision: parent}

    with true <- is_binary(parent),
         {:ok, revision} <- build_revision(document_id, parent, true, nil) do
      cond do
        match_mutation?(state, parent, revision) ->
          state
          |> insert_revision(document_id, revision, replayed: false)
          |> Map.put(:last_concrete_op, concrete)

        identical_present?(state, revision) and winner_id(state) == revision.revision_id ->
          result = %{revision: revision.revision_id, replayed: true, conflicts: conflicts_of(state)}
          %{state | last_result: {:ok, result}, last_concrete_op: concrete}

        true ->
          %{state | last_result: {:error, :revision_conflict}, last_concrete_op: concrete}
      end
    else
      false ->
        %{state | last_result: {:error, :revision_conflict}, last_concrete_op: concrete}

      {:error, _} ->
        %{state | last_result: {:error, :invalid_request}}
    end
  end

  def apply_operation(state, %{op: :import_siblings} = op) do
    document_id = op.document_id
    root_body = op.root_body
    left_body = op.left_body
    right_body = op.right_body

    {:ok, root_id} = Id.calculate(document_id, nil, false, root_body)
    {:ok, left_id} = Id.calculate(document_id, root_id, false, left_body)
    {:ok, right_id} = Id.calculate(document_id, root_id, false, right_body)

    root = revision!(document_id, root_id, nil, false, root_body)
    left = revision!(document_id, left_id, root_id, false, left_body)
    right = revision!(document_id, right_id, root_id, false, right_body)

    state =
      state
      |> accept_import(document_id, root)
      |> accept_import(document_id, left)
      |> accept_import(document_id, right)

    %{
      state
      | last_result: {:ok, %{imported: [root_id, left_id, right_id], replayed: false}},
        last_concrete_op: op
    }
  end

  def apply_operation(state, %{op: :resolve} = op) do
    document_id = op.document_id
    leaves = Tree.leaves(Map.values(state.revisions))
    live = Winner.live_leaves(leaves)
    expected = expected_live_ids(live, op[:expected])
    concrete =
      op
      |> Map.put(:expected, expected)
      |> Map.put(:chosen_parent_revision, choose_parent(live, Map.get(op, :chosen_side, :winner), op))

    case ConflictResolution.validate_leaf_set(leaves, expected) do
      {:error, %ElixirDB.Error{code: code}} ->
        %{state | last_result: {:error, code}, last_concrete_op: concrete}

      :ok ->
        case build_resolution_revisions(document_id, live, op) do
          {:error, code} ->
            %{state | last_result: {:error, code}, last_concrete_op: concrete}

          {:ok, new_revisions} ->
            statuses = Enum.map(new_revisions, &revision_status(state, &1))

            cond do
              Enum.any?(statuses, &(&1 == :different)) ->
                %{state | last_result: {:error, :integrity_violation}, last_concrete_op: concrete}

              statuses != [] and Enum.all?(statuses, &(&1 == :existing)) ->
                {:ok, winner} = projection_winner(state)

                %{
                  state
                  | last_result:
                      {:ok,
                       %{
                         revision: winner.revision_id,
                         replayed: true,
                         conflicts: conflicts_of(state)
                       }},
                    last_concrete_op: concrete
                }

              Enum.any?(statuses, &(&1 == :existing)) ->
                %{state | last_result: {:error, :revision_conflict}, last_concrete_op: concrete}

              true ->
                next =
                  Enum.reduce(new_revisions, state, fn rev, acc ->
                    put_in(acc, [:revisions, rev.revision_id], rev)
                  end)

                {:ok, winner} = projection_winner(next)

                %{
                  next
                  | document_id: document_id,
                    last_result:
                      {:ok,
                       %{
                         revision: winner.revision_id,
                         replayed: false,
                         conflicts: conflicts_of(next)
                       }},
                    last_concrete_op: concrete
                }
            end
        end
    end
  end

  @doc """
  Projects comparable snapshot fields from pure-model state.
  """
  @spec snapshot(state()) :: map()
  def snapshot(%{revisions: revisions} = state) when map_size(revisions) == 0 do
    %{
      document_id: state.document_id,
      tree: [],
      leaf_set: [],
      live_leaf_set: [],
      winner: nil,
      winner_deleted: nil,
      conflicts: [],
      tombstones: [],
      last_result: normalize_result(state.last_result)
    }
  end

  def snapshot(state) do
    revision_list = Map.values(state.revisions)
    leaves = Tree.leaves(revision_list)
    {:ok, winner} = Winner.select(leaves)
    live = Winner.live_leaves(leaves)

    %{
      document_id: state.document_id,
      tree: encode_tree(revision_list),
      leaf_set: encode_leaf_set(leaves),
      live_leaf_set: live |> Enum.map(& &1.revision_id) |> Enum.sort(),
      winner: winner.revision_id,
      winner_deleted: winner.deleted,
      conflicts: Winner.conflicts(leaves, winner),
      tombstones:
        revision_list
        |> Enum.filter(& &1.deleted)
        |> Enum.map(& &1.revision_id)
        |> Enum.sort(),
      last_result: normalize_result(state.last_result)
    }
  end

  defp match_mutation?(state, parent, revision) do
    winner =
      case projection_winner(state) do
        {:ok, w} -> w
        _ -> nil
      end

    cond do
      is_nil(winner) and is_nil(parent) ->
        not Map.has_key?(state.revisions, revision.revision_id)

      not is_nil(winner) and parent == winner.revision_id ->
        not Map.has_key?(state.revisions, revision.revision_id)

      true ->
        false
    end
  end

  defp insert_revision(state, document_id, revision, replayed: replayed) do
    next = %{
      state
      | document_id: document_id,
        revisions: Map.put(state.revisions, revision.revision_id, revision)
    }

    {:ok, winner} = projection_winner(next)

    %{
      next
      | last_result:
          {:ok, %{revision: winner.revision_id, replayed: replayed, conflicts: conflicts_of(next)}}
    }
  end

  defp accept_import(state, document_id, revision) do
    case Map.fetch(state.revisions, revision.revision_id) do
      :error ->
        %{
          state
          | document_id: document_id,
            revisions: Map.put(state.revisions, revision.revision_id, revision)
        }

      {:ok, existing} ->
        if same_revision?(existing, revision),
          do: %{state | document_id: document_id},
          else: raise("import content mismatch for #{revision.revision_id}")
    end
  end

  defp build_resolution_revisions(document_id, live, op) do
    case op.mode do
      :delete_all ->
        live
        |> Enum.map(fn leaf -> build_revision(document_id, leaf.revision_id, true, nil) end)
        |> collect_ok()

      :surviving_body ->
        chosen = choose_parent(live, Map.get(op, :chosen_side, :winner), op)

        with true <- chosen in Enum.map(live, & &1.revision_id),
             {:ok, survivor} <- build_revision(document_id, chosen, false, op.body),
             {:ok, tombs} <-
               live
               |> Enum.reject(&(&1.revision_id == chosen))
               |> Enum.map(&build_revision(document_id, &1.revision_id, true, nil))
               |> collect_ok() do
          {:ok, [survivor | tombs]}
        else
          false -> {:error, :revision_conflict}
          {:error, _} -> {:error, :invalid_request}
        end
    end
  end

  defp choose_parent(_live, _side, %{chosen_parent_revision: chosen}) when is_binary(chosen),
    do: chosen

  defp choose_parent(live, :left, _op), do: live |> Enum.map(& &1.revision_id) |> Enum.min()
  defp choose_parent(live, :right, _op), do: live |> Enum.map(& &1.revision_id) |> Enum.max()

  defp choose_parent(live, :winner, _op) do
    {:ok, winner} = Winner.select(live)
    winner.revision_id
  end

  defp expected_live_ids(live, :current_live), do: Enum.map(live, & &1.revision_id)
  defp expected_live_ids(live, nil), do: Enum.map(live, & &1.revision_id)
  defp expected_live_ids(_live, ids) when is_list(ids), do: ids

  defp resolve_parent(_state, nil), do: nil

  defp resolve_parent(state, :winner) do
    case projection_winner(state) do
      {:ok, winner} -> winner.revision_id
      _ -> nil
    end
  end

  defp resolve_parent(_state, parent) when is_binary(parent), do: parent

  defp projection_winner(%{revisions: revisions}) when map_size(revisions) == 0, do: :error

  defp projection_winner(state) do
    leaves = Tree.leaves(Map.values(state.revisions))
    Winner.select(leaves)
  end

  defp winner_id(state) do
    case projection_winner(state) do
      {:ok, winner} -> winner.revision_id
      _ -> nil
    end
  end

  defp conflicts_of(state) do
    case projection_winner(state) do
      {:ok, winner} ->
        Winner.conflicts(Tree.leaves(Map.values(state.revisions)), winner)

      _ ->
        []
    end
  end

  defp revision_status(state, revision) do
    case Map.fetch(state.revisions, revision.revision_id) do
      {:ok, existing} -> if(same_revision?(existing, revision), do: :existing, else: :different)
      :error -> :missing
    end
  end

  defp identical_present?(state, revision) do
    case Map.fetch(state.revisions, revision.revision_id) do
      {:ok, existing} -> same_revision?(existing, revision)
      :error -> false
    end
  end

  defp same_revision?(a, b),
    do:
      a.revision_id == b.revision_id and a.parent_revision == b.parent_revision and
        a.deleted == b.deleted and a.body == b.body and a.generation == b.generation

  defp build_revision(document_id, parent, deleted, body) do
    with {:ok, revision_id} <- Id.calculate(document_id, parent, deleted, body),
         {:ok, generation} <- Id.generation(revision_id) do
      Revision.new(%{
        document_id: document_id,
        revision_id: revision_id,
        generation: generation,
        parent_revision: parent,
        deleted: deleted,
        body: body
      })
    end
  end

  defp revision!(document_id, revision_id, parent, deleted, body) do
    {:ok, revision} = build_revision(document_id, parent, deleted, body)
    true = revision.revision_id == revision_id
    revision
  end

  defp collect_ok(values) do
    Enum.reduce_while(values, {:ok, []}, fn
      {:ok, value}, {:ok, acc} -> {:cont, {:ok, [value | acc]}}
      {:error, _}, _ -> {:halt, {:error, :invalid_request}}
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end

  defp encode_tree(revisions) do
    revisions
    |> Enum.map(fn rev ->
      %{
        revision_id: rev.revision_id,
        parent_revision: rev.parent_revision,
        deleted: rev.deleted,
        body: rev.body,
        generation: rev.generation
      }
    end)
    |> Enum.sort_by(& &1.revision_id)
  end

  defp encode_leaf_set(leaves) do
    leaves
    |> Enum.map(fn leaf -> %{revision: leaf.revision_id, deleted: leaf.deleted} end)
    |> Enum.sort_by(& &1.revision)
  end

  defp normalize_result(nil), do: nil

  defp normalize_result({:ok, result}),
    do: {:ok, Map.take(result, [:revision, :replayed, :conflicts, :imported])}

  defp normalize_result({:error, code}) when is_atom(code), do: {:error, code}
  defp normalize_result({:error, %ElixirDB.Error{code: code}}), do: {:error, code}
end
