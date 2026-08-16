defmodule VialKeeper.NodeIdentity do
  @moduledoc "Durable identity for a host participating in shadow control operations."

  alias VialKeeper.AtomicWrite

  @filename "node_identity"
  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @spec path(Path.t()) :: Path.t()
  def path(root), do: Path.join(root, @filename)

  @spec ensure(Path.t()) :: {:ok, String.t()} | {:error, VialKeeper.Error.t()}
  def ensure(root) when is_binary(root) do
    path = path(root)

    case File.read(path) do
      {:ok, value} ->
        validate(value)

      {:error, :enoent} ->
        create(path)

      {:error, reason} ->
        {:error,
         VialKeeper.Error.database_unavailable("node identity cannot be read", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec get() :: {:ok, String.t()} | {:error, VialKeeper.Error.t()}
  def get, do: ensure(VialKeeper.Config.database_root())

  @spec ensure!() :: String.t()
  def ensure! do
    case get() do
      {:ok, identity} -> identity
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  defp create(path) do
    identity = VialKeeper.UUID.v4()

    case AtomicWrite.write(path, identity <> "\n") do
      :ok ->
        {:ok, identity}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.database_unavailable("node identity cannot be persisted", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate(value) do
    identity = String.trim(value)

    if Regex.match?(@uuid_pattern, identity),
      do: {:ok, String.downcase(identity)},
      else: {:error, VialKeeper.Error.integrity_violation("node identity is invalid")}
  end
end
