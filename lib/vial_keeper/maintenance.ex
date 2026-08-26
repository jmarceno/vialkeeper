defmodule VialKeeper.Maintenance do
  @moduledoc "Internal maintenance service used by HTTP and administration routes; not a client API."

  alias VialKeeper.Attachments
  alias VialKeeper.Error
  alias VialKeeper.Replication.Wire
  alias VialKeeper.Runtime.DatabaseCatalog

  @doc "Runs an integrity check for a database through its owner."
  @spec integrity_check(binary()) :: {:ok, map()} | {:error, Error.t()}
  def integrity_check(uuid) when is_binary(uuid) do
    DatabaseCatalog.command(uuid, {:command, :integrity_check, %{}})
  end

  @doc "Compacts retention and returns the public compact statistics shape."
  @spec compact(binary(), map()) :: {:ok, map()} | {:error, Error.t()}
  def compact(uuid, request \\ %{})

  def compact(uuid, request) when is_binary(uuid) and is_map(request) do
    case DatabaseCatalog.command(uuid, {:command, :compact_retention, request}) do
      {:ok, stats} -> {:ok, Wire.compact_stats(stats)}
      error -> error
    end
  end

  def compact(_uuid, _request),
    do: {:error, Error.invalid_request("compact retention request must be an object")}

  @doc "Runs attachment garbage collection for a database."
  @spec attachment_gc(binary()) :: {:ok, map()} | {:error, Error.t()}
  def attachment_gc(uuid) when is_binary(uuid), do: Attachments.gc(uuid)
end
