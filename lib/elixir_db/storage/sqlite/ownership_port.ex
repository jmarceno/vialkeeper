defmodule ElixirDB.Storage.SQLite.OwnershipPort do
  @moduledoc """
  SQLite ownership port. Runtime starts ownership through the selected backend,
  never by constructing lease paths or issuing transaction text.
  """
  @behaviour ElixirDB.Storage.Ports.Ownership

  alias ElixirDB.Storage.SQLite.Adapter

  @impl true
  def start_ownership(bundle_root) when is_binary(bundle_root) do
    Adapter.start_ownership(bundle_root)
  end
end
