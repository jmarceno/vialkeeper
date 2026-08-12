defmodule ElixirDB.Storage.Memory.IndexCandidates do
  @moduledoc """
  Memory index and candidate port.

  Provides unordered bounded-scan candidates with no structured or full-text
  indexes. Shared `ElixirDB.Query.Executor` owns filtering and product order.
  """
  @behaviour ElixirDB.Storage.Ports.IndexCandidates

  alias ElixirDB.Query.Projection
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}

  @impl true
  def list_indexes(%BackendContext{}), do: {:ok, []}

  @impl true
  def create_index(%BackendContext{}, _definition),
    do: {:error, ElixirDB.Error.invalid_request("memory backend indexes are unsupported")}

  @impl true
  def delete_index(%BackendContext{}, _index_id), do: :ok

  @impl true
  def rebuild_index(%BackendContext{}, _index_id), do: {:ok, %{}}

  @impl true
  def winning_document_count(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      count =
        state.documents
        |> Map.values()
        |> Enum.count(&(not &1.winning_deleted))

      {:ok, count}
    end
  end

  @impl true
  def lookup_candidates(%BackendContext{} = context, %{kind: :bounded_scan}) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)
      {:ok, bounded_scan_candidates(state.documents)}
    end
  end

  def lookup_candidates(%BackendContext{}, _request), do: {:ok, []}

  @impl true
  def range_scan_candidates(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_index_hint("memory backend has no structured indexes", %{})}

  @impl true
  def full_text_candidates(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_index_hint("memory backend has no full-text indexes", %{})}

  @impl true
  def ready_definitions(%BackendContext{}), do: {:ok, []}

  @impl true
  def refresh_document(%BackendContext{}, _document_id, _winner, _ready), do: :ok

  defp bounded_scan_candidates(documents) do
    # Deliberately disordered: product order comes from Query.Executor.
    documents
    |> Enum.reduce([], fn {document_id, doc}, acc ->
      append_live_candidate(acc, document_id, doc)
    end)
    |> Enum.shuffle()
  end

  defp append_live_candidate(acc, _document_id, %{winning_deleted: true}), do: acc

  defp append_live_candidate(acc, document_id, doc) do
    [
      Projection.document(document_id, doc.winning_revision, doc.body)
      | acc
    ]
  end
end
