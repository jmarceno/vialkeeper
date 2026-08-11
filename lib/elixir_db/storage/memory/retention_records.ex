defmodule ElixirDB.Storage.Memory.RetentionRecords do
  @moduledoc "Memory retention-records fact port used by mutation and import."
  @behaviour ElixirDB.Storage.Ports.RetentionRecords

  alias ElixirDB.Domain.RetentionBoundary
  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def retention_state(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      identity = Store.identity(adapter.store)

      {:ok,
       %{
         retention_floor_sequence: Map.get(identity, :retention_floor_sequence, 0),
         compaction_epoch: Map.get(identity, :compaction_epoch, 0),
         retention_boundary_digest: Map.get(identity, :retention_boundary_digest)
       }}
    end
  end

  @impl true
  def list_peer_positions(%BackendContext{}), do: {:ok, []}

  @impl true
  def put_peer_position_cas(%BackendContext{}, _request),
    do: {:error, ElixirDB.Error.invalid_request("memory backend peer positions are unsupported")}

  @impl true
  def read_boundary_pages(%BackendContext{}, _request),
    do: {:ok, %{pages: [], continuation_cursor: nil}}

  @impl true
  def install_boundary_pages(%BackendContext{}, _request), do: {:ok, %{installed: 0}}

  @impl true
  def get_compaction_result(%BackendContext{}), do: {:ok, nil}

  @impl true
  def put_compaction_result(%BackendContext{}, _result), do: :ok

  @impl true
  def list_boundaries(%BackendContext{} = context), do: list_boundaries(context, [])

  @impl true
  def list_boundaries(%BackendContext{} = context, opts) when is_list(opts) do
    with {:ok, adapter} <- Context.unwrap(context) do
      source = Keyword.get(opts, :source_database_uuid)

      boundaries =
        adapter.store
        |> Store.get()
        |> Map.fetch!(:boundaries)
        |> Enum.filter(fn stored ->
          is_nil(source) or stored.source_database_uuid == source
        end)

      {:ok, boundaries}
    end
  end

  @impl true
  def install_imported_boundaries(%BackendContext{} = context, boundaries)
      when is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context) do
      merge_imported_boundaries(adapter, boundaries)
    end
  end

  @impl true
  def mark_pending_local_causal(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        {:ok, %{state | pending_local_causal: true}, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def pending_local_causal?(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.get(adapter.store).pending_local_causal}
    end
  end

  @impl true
  def encode_stored_boundary(stored) when is_map(stored) do
    boundary = Map.fetch!(stored, :boundary)

    %{
      "source_database_uuid" => stored.source_database_uuid,
      "source_history_epoch" => stored.source_history_epoch,
      "compaction_epoch" => stored.compaction_epoch,
      "boundary" => %{
        "document_id" => boundary.document_id,
        "history_id" => boundary.history_id,
        "minimum_retained_generation" => boundary.minimum_retained_generation,
        "retired" => boundary.retired,
        "retired_branch_roots" => boundary.retired_branch_roots
      }
    }
  end

  defp normalize_all(boundaries, existing) do
    Enum.reduce_while(boundaries, {:ok, existing}, fn raw, {:ok, acc} ->
      case normalize_one(raw) do
        {:ok, stored} -> {:cont, {:ok, upsert(acc, stored)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp merge_imported_boundaries(adapter, boundaries) do
    adapter.store
    |> Store.update(fn state ->
      case normalize_all(boundaries, state.boundaries) do
        {:ok, merged} -> {:ok, %{state | boundaries: merged}, :ok}
        {:error, error} -> {:error, error}
      end
    end)
    |> normalize_ok()
  end

  defp normalize_one(raw) do
    with {:ok, boundary} <-
           RetentionBoundary.from_wire(
             MapAccess.get(raw, :boundary) || MapAccess.get(raw, "boundary")
           ),
         source_uuid when is_binary(source_uuid) <-
           MapAccess.get(raw, :source_database_uuid) || MapAccess.get(raw, "source_database_uuid"),
         source_epoch when is_binary(source_epoch) <-
           MapAccess.get(raw, :source_history_epoch) || MapAccess.get(raw, "source_history_epoch"),
         compaction_epoch when is_integer(compaction_epoch) <-
           MapAccess.get(raw, :compaction_epoch) || MapAccess.get(raw, "compaction_epoch") || 0 do
      {:ok,
       %{
         source_database_uuid: source_uuid,
         source_history_epoch: source_epoch,
         compaction_epoch: compaction_epoch,
         boundary: boundary
       }}
    else
      _ -> {:error, ElixirDB.Error.invalid_request("purged boundary is invalid")}
    end
  end

  defp upsert(list, stored) do
    key = {stored.source_database_uuid, stored.boundary.document_id, stored.boundary.history_id}

    list
    |> Enum.reject(fn existing ->
      {existing.source_database_uuid, existing.boundary.document_id, existing.boundary.history_id} ==
        key
    end)
    |> Kernel.++([stored])
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:error, reason}), do: {:error, Errors.normalize(reason)}
end
