defmodule ElixirDB.Storage.Adapter do
  @moduledoc "Engine-neutral persistence boundary."

  @callback create(binary(), map()) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  @callback open(binary(), map()) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  @callback close(term()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback identity(term()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback update_config(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback integrity_check(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback get_document(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback get_revision(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback apply_local_mutation(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback apply_bulk_mutation(term(), map()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  @callback resolve_conflict(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback read_changes(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback has_local_origin_changes?(term()) ::
              {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  @callback diff_revisions(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback get_revision_chains(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback import_revision_chains(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback get_local_record(term(), binary(), binary()) ::
              {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  @callback put_local_record_cas(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback retention_state(term()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback list_peer_positions(term()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  @callback put_peer_position_cas(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback read_boundary_pages(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback install_boundary_pages(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback compact_retention(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback list_replication_jobs(term()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  @callback put_replication_job(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback delete_replication_job(term(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback create_index(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback delete_index(term(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback rebuild_index(term(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback list_indexes(term()) :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  @callback execute_query(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback explain_query(term(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
end
