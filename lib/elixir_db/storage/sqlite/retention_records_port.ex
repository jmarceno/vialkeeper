defmodule ElixirDB.Storage.SQLite.RetentionRecordsPort do
  @moduledoc """
  SQLite retention-records fact port.
  """
  @behaviour ElixirDB.Storage.Ports.RetentionRecords

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Context, LocalRecords, RetentionRecords}

  @impl true
  def retention_state(%BackendContext{} = _context) do
    {:error, ElixirDB.Error.invalid_request("call ElixirDB.Storage.Services.retention_state/1")}
  end

  @impl true
  def list_peer_positions(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.list_peers(adapter.conn))
    end
  end

  @impl true
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    put_peer_position_record(context, %{
      peer_database_uuid: MapAccess.get(request, :peer_database_uuid),
      expected_version: MapAccess.get(request, :expected_version, 0),
      value: MapAccess.get(request, :value)
    })
  end

  @impl true
  def read_boundary_pages(%BackendContext{} = _context, request) when is_map(request) do
    _ = request
    {:error, ElixirDB.Error.invalid_request("call ElixirDB.Storage.Services.read_boundary_pages/2")}
  end

  @impl true
  def install_boundary_pages(%BackendContext{} = _context, request) when is_map(request) do
    _ = request

    {:error,
     ElixirDB.Error.invalid_request("call ElixirDB.Storage.Services.install_boundary_pages/2")}
  end

  @impl true
  def get_compaction_result(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.get_last_result(adapter.conn))
    end
  end

  @impl true
  def put_compaction_result(%BackendContext{} = context, result) when is_map(result) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.put_last_result(adapter.conn, result))
    end
  end

  @impl true
  def list_boundaries(%BackendContext{} = context) do
    list_boundaries(context, [])
  end

  @impl true
  def list_boundaries(%BackendContext{} = context, opts) when is_list(opts) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.list_boundaries(adapter.conn, opts))
    end
  end

  @impl true
  def install_imported_boundaries(%BackendContext{} = context, boundaries)
      when is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.install_imported_boundaries(adapter.conn, boundaries))
    end
  end

  @impl true
  def mark_pending_local_causal(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.mark_pending_local_causal(adapter.conn))
    end
  end

  @impl true
  def pending_local_causal?(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.pending_local_causal?(adapter.conn))
    end
  end

  @impl true
  def encode_stored_boundary(stored) when is_map(stored) do
    RetentionRecords.encode_stored_boundary(stored)
  end

  @impl true
  def apply_compaction_effect(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.apply_compaction_effect(adapter.conn, effect))
    end
  end

  @impl true
  def boundary_install_state(%BackendContext{} = context, source_uuid)
      when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.boundary_install_state(adapter.conn, source_uuid))
    end
  end

  @impl true
  def begin_boundary_install(%BackendContext{} = context, install_id, state)
      when is_binary(install_id) and is_map(state) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.begin_boundary_install(adapter.conn, install_id, state))
    end
  end

  @impl true
  def stage_boundary_page(%BackendContext{} = context, install_id, page)
      when is_binary(install_id) and is_map(page) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.stage_boundary_page(adapter.conn, install_id, page))
    end
  end

  @impl true
  def complete_boundary_install(%BackendContext{} = context, install_id)
      when is_binary(install_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.complete_boundary_install(adapter.conn, install_id))
    end
  end

  @impl true
  def replace_boundary_set(%BackendContext{} = context, state, boundaries)
      when is_map(state) and is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.replace_boundary_set(adapter.conn, state, boundaries))
    end
  end

  @impl true
  def maintenance_counter(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.maintenance_counter(adapter.conn))
    end
  end

  @impl true
  def update_peer_wire(%BackendContext{} = context, peer_uuid, wire)
      when is_binary(peer_uuid) and is_map(wire) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(RetentionRecords.update_peer_wire(adapter.conn, peer_uuid, wire))
    end
  end

  @impl true
  def put_peer_position_record(%BackendContext{} = context, request) when is_map(request) do
    peer_uuid = MapAccess.get(request, :peer_database_uuid)
    expected = MapAccess.get(request, :expected_version, 0)
    value = MapAccess.get(request, :value)

    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(
        LocalRecords.put_cas_tx(adapter, %{
          namespace: RetentionRecords.peer_ledger_namespace(),
          key: peer_uuid,
          expected_version: expected,
          value: value
        })
      )
    end
  end
end
