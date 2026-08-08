defmodule ElixirDB.Storage.FaultAdapter do
  @moduledoc """
  Storage adapter wrapper that injects failures at named points.
  """

  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.FaultAdapter, as: Fault

  defstruct [:inner, :fault]

  @type t :: %__MODULE__{inner: term(), fault: Fault.t()}

  @spec wrap(term()) :: t()
  def wrap(inner), do: %__MODULE__{inner: inner, fault: Fault.wrap(inner)}

  @spec inject(t(), atom(), Fault.fault()) :: t()
  def inject(%__MODULE__{} = adapter, point, fault),
    do: %{adapter | fault: Fault.inject(adapter.fault, point, fault)}

  @impl true
  def create(path, options \\ %{}), do: ElixirDB.Storage.SQLite.Adapter.create(path, options)

  @impl true
  def open(path, options \\ %{}), do: ElixirDB.Storage.SQLite.Adapter.open(path, options)

  @impl true
  def close(%__MODULE__{inner: inner}), do: inner_module(inner).close(inner)

  @impl true
  def identity(%__MODULE__{inner: inner}), do: inner_module(inner).identity(inner)

  @impl true
  def update_config(%__MODULE__{inner: inner}, config),
    do: inner_module(inner).update_config(inner, config)

  @impl true
  def integrity_check(%__MODULE__{inner: inner}, options),
    do: inner_module(inner).integrity_check(inner, options)

  @impl true
  def get_document(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).get_document(inner, request)

  @impl true
  def get_revision(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).get_revision(inner, request)

  @impl true
  def apply_local_mutation(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).apply_local_mutation(inner, request)

  @impl true
  def apply_bulk_mutation(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).apply_bulk_mutation(inner, request)

  @impl true
  def resolve_conflict(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).resolve_conflict(inner, request)

  @impl true
  def read_changes(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).read_changes(inner, request)

  @impl true
  def has_local_origin_changes?(%__MODULE__{inner: inner}),
    do: inner_module(inner).has_local_origin_changes?(inner)

  @impl true
  def has_local_origin_changes?(%__MODULE__{inner: inner}, peer_database_uuid),
    do: inner_module(inner).has_local_origin_changes?(inner, peer_database_uuid)

  @impl true
  def clear_pending_local_causal(%__MODULE__{inner: inner}),
    do: inner_module(inner).clear_pending_local_causal(inner)

  @impl true
  def clear_pending_local_causal(%__MODULE__{inner: inner}, peer_database_uuid),
    do: inner_module(inner).clear_pending_local_causal(inner, peer_database_uuid)

  @impl true
  def diff_revisions(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).diff_revisions(inner, request)

  @impl true
  def get_revision_chains(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).get_revision_chains(inner, request)

  @impl true
  def import_revision_chains(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).import_revision_chains(inner, request)

  @impl true
  def get_local_record(%__MODULE__{inner: inner}, namespace, key),
    do: inner_module(inner).get_local_record(inner, namespace, key)

  @impl true
  def put_local_record_cas(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).put_local_record_cas(inner, request)

  @impl true
  def retention_state(%__MODULE__{inner: inner}),
    do: inner_module(inner).retention_state(inner)

  @impl true
  def list_peer_positions(%__MODULE__{inner: inner}),
    do: inner_module(inner).list_peer_positions(inner)

  @impl true
  def put_peer_position_cas(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).put_peer_position_cas(inner, request)

  @impl true
  def read_boundary_pages(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).read_boundary_pages(inner, request)

  @impl true
  def install_boundary_pages(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).install_boundary_pages(inner, request)

  @impl true
  def compact_retention(%__MODULE__{inner: inner, fault: fault} = adapter, request) do
    case Fault.maybe_fail(fault, :before_compact_retention) do
      {:ok, fault} -> run_compact_retention(adapter, inner, fault, request)
      {:error, error, _} -> {:error, error}
    end
  end

  defp run_compact_retention(adapter, inner, fault, request) do
    inner_with_fault = %{inner | retention_fault: retention_fault_fn(fault)}
    result = inner_module(inner).compact_retention(inner_with_fault, request)
    fail_after_compact(%{adapter | fault: fault}, result)
  end

  defp retention_fault_fn(fault) do
    fn point ->
      case Fault.maybe_fail(fault, point) do
        {:ok, _} -> :ok
        {:error, error, _} -> {:error, error}
      end
    end
  end

  @impl true
  def list_replication_jobs(%__MODULE__{inner: inner}),
    do: inner_module(inner).list_replication_jobs(inner)

  @impl true
  def put_replication_job(%__MODULE__{inner: inner}, job),
    do: inner_module(inner).put_replication_job(inner, job)

  @impl true
  def delete_replication_job(%__MODULE__{inner: inner}, job_id),
    do: inner_module(inner).delete_replication_job(inner, job_id)

  @impl true
  def create_index(%__MODULE__{inner: inner}, definition),
    do: inner_module(inner).create_index(inner, definition)

  @impl true
  def delete_index(%__MODULE__{inner: inner}, index_id),
    do: inner_module(inner).delete_index(inner, index_id)

  @impl true
  def rebuild_index(%__MODULE__{inner: inner}, index_id),
    do: inner_module(inner).rebuild_index(inner, index_id)

  @impl true
  def list_indexes(%__MODULE__{inner: inner}), do: inner_module(inner).list_indexes(inner)

  @impl true
  def execute_query(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).execute_query(inner, request)

  @impl true
  def explain_query(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).explain_query(inner, request)

  @impl true
  def resolve_attachment_ticket(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).resolve_attachment_ticket(inner, request)

  @impl true
  def resolve_blob_metadata(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).resolve_blob_metadata(inner, request)

  @impl true
  def protect_pending_blob(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).protect_pending_blob(inner, request)

  @impl true
  def remove_pending_blob_protection(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).remove_pending_blob_protection(inner, request)

  @impl true
  def list_live_attachment_digests(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).list_live_attachment_digests(inner, request)

  @impl true
  def cleanup_expired_pending_blobs(%__MODULE__{inner: inner}, request),
    do: inner_module(inner).cleanup_expired_pending_blobs(inner, request)

  defp fail_after_compact(%__MODULE__{fault: fault}, result) do
    case Fault.maybe_fail(fault, :after_compact_retention) do
      {:ok, _} -> result
      {:error, error, _} -> {:error, error}
    end
  end

  defp inner_module(inner), do: inner.__struct__
end
