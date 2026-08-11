defmodule ElixirDB.Storage.Memory.ChangeLog do
  @moduledoc "Memory change-log fact port."
  @behaviour ElixirDB.Storage.Ports.ChangeLog

  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def allocate_sequences(%BackendContext{} = context, count)
      when is_integer(count) and count >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        {:ok, new_state, sequences} = Store.allocate_sequences(state, count)
        {:ok, new_state, sequences}
      end)
    end
  end

  @impl true
  def append_change(%BackendContext{} = context, entry) when is_map(entry) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        {:ok, new_state} = Store.append_change(state, entry)
        {:ok, new_state, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def read_page(%BackendContext{} = context, since, limit)
      when is_integer(since) and since >= 0 and is_integer(limit) and limit > 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, page_from_state(Store.get(adapter.store), since, limit)}
    end
  end

  @impl true
  def has_local_origin_changes?(%BackendContext{} = context) do
    has_local_origin_changes?(context, nil)
  end

  @impl true
  def has_local_origin_changes?(%BackendContext{} = context, _peer) do
    with {:ok, adapter} <- Context.unwrap(context) do
      changes = Store.get(adapter.store).changes
      {:ok, Enum.any?(changes, &(&1.origin == "local"))}
    end
  end

  @impl true
  def clear_pending_local_causal(%BackendContext{} = context) do
    clear_pending_local_causal(context, nil)
  end

  @impl true
  def clear_pending_local_causal(%BackendContext{} = context, _peer) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, fn state ->
        {:ok, %{state | pending_local_causal: false}, :cleared}
      end)
    end
  end

  @impl true
  def delete_through_boundary(%BackendContext{} = context, through)
      when is_integer(through) and through >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        changes = Enum.reject(state.changes, &(&1.sequence <= through))
        {:ok, %{state | changes: changes}, :ok}
      end)
      |> normalize_ok()
    end
  end

  defp page_from_state(state, since, limit) do
    changes =
      state.changes
      |> Enum.filter(&(&1.sequence > since))
      |> Enum.take(limit + 1)

    page = Enum.take(changes, limit)
    last = List.last(page, %{sequence: since}).sequence

    %{
      results: Enum.map(page, &change_row/1),
      last_sequence: last,
      has_more: length(changes) > limit
    }
  end

  defp change_row(change) do
    leaves =
      case StrictDecoder.decode(change.leaf_set_json) do
        {:ok, value} -> value
        _ -> []
      end

    %{
      sequence: change.sequence,
      document_id: change.document_id,
      winning_revision: change.winning_revision,
      deleted: change.winning_deleted,
      leaf_revisions: leaves,
      origin: change.origin
    }
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:error, reason}), do: {:error, Errors.normalize(reason)}
end
