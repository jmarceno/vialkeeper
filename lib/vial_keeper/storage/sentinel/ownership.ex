defmodule VialKeeper.Storage.Sentinel.Ownership do
  @moduledoc """
  Exclusive ownership for the sentinel backend.

  Registers an ownership key in the database registry so two runtimes cannot
  open the same sentinel bundle concurrently. This proof does not use SQL.
  """
  use GenServer

  def start_link(bundle_root), do: GenServer.start_link(__MODULE__, bundle_root)

  @impl true
  def init(bundle_root) do
    key = {:ownership, Path.expand(bundle_root)}

    case Registry.register(VialKeeper.Runtime.DatabaseRegistry, key, :sentinel) do
      {:ok, _owner} ->
        {:ok, %{key: key}}

      {:error, {:already_registered, _pid}} ->
        unavailable(:busy)
    end
  end

  defp unavailable(reason) do
    {:stop,
     VialKeeper.Error.database_in_use("database ownership lease is unavailable", %{
       cause: inspect(reason)
     })}
  end
end
