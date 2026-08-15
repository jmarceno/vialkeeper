defmodule VialKeeper.Storage.SQLite.Retention do
  @moduledoc """
  Compatibility wrappers that route SQLite adapter handles into shared retention
  services. Prefer `VialKeeper.Storage.Services` with a `BackendContext`.
  """

  alias VialKeeper.Storage.Services.Retention
  alias VialKeeper.Storage.SQLite.{Adapter, RetentionRecords}

  @doc "Clears pending local causal markers."
  @spec clear_pending_local_causal(term()) :: :ok | {:error, VialKeeper.Error.t()}
  def clear_pending_local_causal(conn), do: clear_pending_local_causal(conn, nil)

  @spec clear_pending_local_causal(term(), binary() | nil) ::
          :ok | {:error, VialKeeper.Error.t()}
  def clear_pending_local_causal(conn, peer_database_uuid),
    do: RetentionRecords.clear_pending_local_causal(conn, peer_database_uuid)

  @doc "Compacts retention inside an already-open transaction."
  @spec compact(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def compact(adapter, request \\ %{}) when is_map(request),
    do: Retention.compact_tx(Adapter.to_context(adapter), request)

  @doc "Builds retention state from an open adapter."
  @spec retention_state(map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def retention_state(adapter) when is_map(adapter),
    do: Retention.retention_state(Adapter.to_context(adapter))

  @doc "Lists peer ledger positions."
  @spec list_peer_positions(term()) :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  def list_peer_positions(conn), do: RetentionRecords.list_peers(conn)

  @doc "Compare-and-swaps a peer ledger position inside an open transaction."
  @spec put_peer_position_cas(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def put_peer_position_cas(adapter, request) when is_map(request),
    do: Retention.put_peer_position_cas(Adapter.to_context(adapter), request)

  @doc "Reads one boundary page."
  @spec read_boundary_pages(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def read_boundary_pages(adapter, request) when is_map(adapter) and is_map(request),
    do: Retention.read_boundary_pages(Adapter.to_context(adapter), request)

  @doc "Installs one boundary page inside an open transaction."
  @spec install_boundary_pages(map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def install_boundary_pages(adapter, request) when is_map(adapter) and is_map(request),
    do: Retention.install_boundary_pages(Adapter.to_context(adapter), request)
end
