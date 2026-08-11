defmodule ElixirDB.Storage.Memory.IndexCandidates do
  @moduledoc "Memory index port with no-op refresh for mutation semantic tests."
  @behaviour ElixirDB.Storage.Ports.IndexCandidates

  alias ElixirDB.Storage.BackendContext

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
  def lookup_candidates(%BackendContext{}, _request), do: {:ok, []}

  @impl true
  def range_scan_candidates(%BackendContext{}, _request), do: {:ok, []}

  @impl true
  def full_text_candidates(%BackendContext{}, _request), do: {:ok, []}

  @impl true
  def ready_definitions(%BackendContext{}), do: {:ok, []}

  @impl true
  def refresh_document(%BackendContext{}, _document_id, _winner, _ready), do: :ok
end
