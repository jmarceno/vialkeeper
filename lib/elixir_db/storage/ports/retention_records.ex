defmodule ElixirDB.Storage.Ports.RetentionRecords do
  @moduledoc """
  Retention record port: peer positions, boundary pages, retention state, and
  compaction result metadata.

  Compaction policy and safe-report semantics remain in shared retention code.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback retention_state(BackendContext.t()) :: result(map())
  @callback list_peer_positions(BackendContext.t()) :: result([map()])
  @callback put_peer_position_cas(BackendContext.t(), map()) :: result(map())
  @callback read_boundary_pages(BackendContext.t(), map()) :: result(map())
  @callback install_boundary_pages(BackendContext.t(), map()) :: result(map())
  @callback get_compaction_result(BackendContext.t()) :: result(map() | nil)
  @callback put_compaction_result(BackendContext.t(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback list_boundaries(BackendContext.t()) :: result([map()])
  @callback list_boundaries(BackendContext.t(), keyword()) :: result([map()])
  @callback install_imported_boundaries(BackendContext.t(), [map()]) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback mark_pending_local_causal(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback pending_local_causal?(BackendContext.t()) :: result(boolean())
  @callback encode_stored_boundary(map()) :: map()
  @callback apply_compaction_effect(BackendContext.t(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback boundary_install_state(BackendContext.t(), binary()) :: result(map() | nil)
  @callback begin_boundary_install(BackendContext.t(), binary(), map()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback stage_boundary_page(BackendContext.t(), binary(), map()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback complete_boundary_install(BackendContext.t(), binary()) :: result(map())
  @callback replace_boundary_set(BackendContext.t(), map(), [term()]) :: result(map())
  @callback maintenance_counter(BackendContext.t()) :: result(non_neg_integer())
  @callback update_peer_wire(BackendContext.t(), binary(), map()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback put_peer_position_record(BackendContext.t(), map()) :: result(map())
end
