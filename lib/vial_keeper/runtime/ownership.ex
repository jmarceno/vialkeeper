defmodule VialKeeper.Runtime.Ownership do
  @moduledoc """
  Runtime ownership admission for a database bundle.

  Delegates acquire/release to the configured storage backend. Callers never
  construct engine-specific lease paths or transaction modes.
  """

  alias VialKeeper.Storage.Registry, as: StorageRegistry

  @doc false
  def child_spec(bundle_root) when is_binary(bundle_root) do
    backend = StorageRegistry.backend()

    %{
      id: __MODULE__,
      start: {backend, :start_ownership, [bundle_root]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @doc "Starts backend ownership for `bundle_root`."
  @spec start_link(binary()) :: GenServer.on_start()
  def start_link(bundle_root) when is_binary(bundle_root) do
    StorageRegistry.backend().start_ownership(bundle_root)
  end
end
