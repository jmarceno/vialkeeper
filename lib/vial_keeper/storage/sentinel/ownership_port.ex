defmodule VialKeeper.Storage.Sentinel.OwnershipPort do
  @moduledoc """
  Sentinel ownership port without SQL.
  """
  @behaviour VialKeeper.Storage.Ports.Ownership

  alias VialKeeper.Storage.Sentinel.Adapter

  @impl true
  def start_ownership(bundle_root) when is_binary(bundle_root) do
    Adapter.start_ownership(bundle_root)
  end
end
