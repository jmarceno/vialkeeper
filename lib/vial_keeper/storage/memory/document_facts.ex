defmodule VialKeeper.Storage.Memory.DocumentFacts do
  @moduledoc "Memory document/revision fact port."
  @behaviour VialKeeper.Storage.Ports.DocumentFacts

  alias VialKeeper.Domain.Revision
  alias VialKeeper.Revisions.Compare
  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Memory.{Context, Store}
  alias VialKeeper.Storage.Ports.Errors

  @impl true
  def find_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.find_document(Store.get(adapter.store), document_id)}
    end
  end

  @impl true
  def find_documents(%BackendContext{} = context, document_ids) when is_list(document_ids) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)
      {:ok, Map.new(document_ids, &{&1, Store.find_document(state, &1)})}
    end
  end

  @impl true
  def find_revision(%BackendContext{} = context, document_id, revision_id)
      when is_binary(document_id) and is_binary(revision_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      lookup_revision(Store.get(adapter.store), document_id, revision_id)
    end
  end

  @impl true
  def find_revision_batch(%BackendContext{} = context, requests) when is_list(requests) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      {:ok,
       Enum.map(requests, fn %{document_id: document_id, revision_id: revision_id} ->
         document = Store.find_document(state, document_id)

         revision =
           if document, do: lookup_revision_value(state, document_id, revision_id), else: nil

         %{
           document_id: document_id,
           revision_id: revision_id,
           document: document,
           revision: revision
         }
       end)}
    end
  end

  @impl true
  def list_leaves(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      case Store.find_document(state, document_id) do
        nil -> {:error, VialKeeper.Error.document_not_found("document not found")}
        _ -> {:ok, Store.list_leaves(state, document_id)}
      end
    end
  end

  @impl true
  def list_ancestors(%BackendContext{} = context, document_id, revision_id)
      when is_binary(document_id) and is_binary(revision_id) do
    with {:ok, adapter} <- Context.unwrap(context),
         state = Store.get(adapter.store),
         {:ok, revision} <- Store.find_revision(state, document_id, revision_id) do
      walk_ancestors(state, document_id, revision.parent_revision, %{}, [])
    else
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def list_document_page(%BackendContext{} = context, cursor, limit)
      when (is_nil(cursor) or is_binary(cursor)) and is_integer(limit) and limit > 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, page_documents(Store.get(adapter.store), cursor, limit)}
    end
  end

  @impl true
  def ensure_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        {:ok, new_state, doc} = Store.ensure_document(state, document_id)
        {:ok, new_state, doc}
      end)
    end
  end

  @impl true
  def insert_document_with_revision(
        %BackendContext{} = context,
        document_id,
        %Revision{} = revision,
        sequence,
        _body_json
      )
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        Store.insert_document_with_revision(state, document_id, revision, sequence)
      end)
    end
  end

  @impl true
  def ensure_parent(%BackendContext{}, document_id, nil)
      when is_binary(document_id),
      do: :ok

  def ensure_parent(%BackendContext{} = context, document_id, parent)
      when is_binary(document_id) and is_binary(parent) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Store.find_revision(Store.get(adapter.store), document_id, parent) do
        {:ok, _} ->
          :ok

        {:error, _} ->
          {:error,
           VialKeeper.Error.integrity_violation("revision parent is missing", %{
             parent_revision: parent
           })}
      end
    end
  end

  @impl true
  def insert_revision(%BackendContext{} = context, document_id, %Revision{} = revision)
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      apply_store_ok(adapter, &Store.insert_revision(&1, document_id, revision))
    end
  end

  @impl true
  def insert_revision_with_body(%BackendContext{} = context, document_id, revision, _body_json),
    do: insert_revision(context, document_id, revision)

  @impl true
  def insert_revision_for_document(
        %BackendContext{} = context,
        %{document_id: document_id},
        %Revision{} = revision
      )
      when is_binary(document_id) do
    insert_revision(context, document_id, revision)
  end

  @impl true
  def insert_or_accept_revision(%BackendContext{} = context, document_id, %Revision{} = revision)
      when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      accept_or_insert(context, adapter, document_id, revision)
    end
  end

  @impl true
  def update_winning(%BackendContext{} = context, document_id, %Revision{} = winner, sequence)
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      apply_store_ok(adapter, &Store.update_winning(&1, document_id, winner, sequence))
    end
  end

  @impl true
  def update_winning_with_body(
        %BackendContext{} = context,
        document_id,
        winner,
        sequence,
        _body_json
      ),
      do: update_winning(context, document_id, winner, sequence)

  @impl true
  def update_winning_for_document(
        %BackendContext{} = context,
        %{document_id: document_id},
        %Revision{} = winner,
        sequence
      )
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    update_winning(context, document_id, winner, sequence)
  end

  @impl true
  def empty_document(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      apply_store_ok(adapter, &Store.empty_document(&1, document_id))
    end
  end

  @impl true
  def delete_history(%BackendContext{} = context, document_id, history_id)
      when is_binary(document_id) and is_binary(history_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        {:ok, new_state} = Store.delete_history(state, document_id, history_id)
        {:ok, new_state, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def list_compaction_documents(%BackendContext{} = context, candidate_floor)
      when is_integer(candidate_floor) and candidate_floor >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.list_compaction_documents(Store.get(adapter.store), candidate_floor)}
    end
  end

  @impl true
  def delete_revisions(%BackendContext{} = context, document_id, revision_ids)
      when is_binary(document_id) and is_list(revision_ids) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        {:ok, new_state} = Store.delete_revisions(state, document_id, revision_ids)
        {:ok, new_state, :ok}
      end)
      |> normalize_ok()
    end
  end

  defp lookup_revision(state, document_id, revision_id) do
    case Store.find_document(state, document_id) do
      nil ->
        {:ok, nil}

      _doc ->
        case Store.find_revision(state, document_id, revision_id) do
          {:ok, revision} -> {:ok, revision}
          {:error, reason} -> {:error, Errors.normalize(reason)}
        end
    end
  end

  defp lookup_revision_value(state, document_id, revision_id) do
    case Store.find_revision(state, document_id, revision_id) do
      {:ok, revision} -> revision
      {:error, _} -> nil
    end
  end

  defp page_documents(state, cursor, limit) do
    sorted = state.documents |> Map.keys() |> Enum.sort()
    ids = if is_binary(cursor), do: Enum.drop_while(sorted, &(&1 <= cursor)), else: sorted
    %{document_ids: Enum.take(ids, limit), next_cursor: Enum.at(ids, limit)}
  end

  defp accept_or_insert(context, adapter, document_id, revision) do
    case Store.find_revision(Store.get(adapter.store), document_id, revision.revision_id) do
      {:ok, existing} ->
        if Compare.same?(existing, revision) do
          :ok
        else
          {:error,
           VialKeeper.Error.integrity_violation("existing revision differs from imported revision")}
        end

      {:error, %VialKeeper.Error{code: :revision_not_found}} ->
        insert_revision(context, document_id, revision)

      {:error, error} ->
        {:error, error}
    end
  end

  defp apply_store_ok(adapter, fun) do
    adapter.store
    |> Store.update(fn state ->
      case fun.(state) do
        {:ok, new_state} -> {:ok, new_state, :ok}
        {:error, error} -> {:error, error}
      end
    end)
    |> normalize_ok()
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:error, reason}), do: {:error, Errors.normalize(reason)}

  @spec walk_ancestors(
          Store.state(),
          binary(),
          binary() | nil,
          %{optional(binary()) => true},
          [Revision.t()]
        ) :: {:ok, [Revision.t()]} | {:error, VialKeeper.Error.t()}
  defp walk_ancestors(_state, _document_id, nil, _seen, acc), do: {:ok, Enum.reverse(acc)}

  defp walk_ancestors(state, document_id, revision_id, seen, acc) do
    if Map.has_key?(seen, revision_id) do
      {:error, VialKeeper.Error.integrity_violation("revision ancestry cycle detected")}
    else
      case Store.find_revision(state, document_id, revision_id) do
        {:ok, revision} ->
          walk_ancestors(
            state,
            document_id,
            revision.parent_revision,
            Map.put(seen, revision_id, true),
            [revision | acc]
          )

        {:error, reason} ->
          {:error, Errors.normalize(reason)}
      end
    end
  end
end
