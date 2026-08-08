defmodule ElixirDB.Runtime.RegistrationManifest do
  @moduledoc "Atomic, routing-only registration manifest."

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.PathSafety
  alias ElixirDB.Runtime.AtomicWrite

  @spec path() :: binary()
  def path do
    Application.get_env(:elixir_db, :registration_manifest) ||
      Path.join(ElixirDB.Config.database_root(), "registrations.json")
  end

  @spec read() :: {:ok, list()} | {:error, ElixirDB.Error.t()}
  def read do
    case File.read(path()) do
      {:error, :enoent} ->
        {:ok, []}

      {:ok, body} ->
        with {:ok, map} <- StrictDecoder.decode(body), do: validate(map)

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("registration manifest cannot be read", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec write(list()) :: :ok | {:error, ElixirDB.Error.t()}
  def write(entries) do
    with {:ok, normalized} <- normalize_entries(entries),
         {:ok, json} <- Canonical.encode(%{"version" => 1, "databases" => normalized}),
         :ok <- AtomicWrite.write(path(), json) do
      :ok
    else
      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("registration manifest could not be prepared", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate(%{"version" => 1, "databases" => databases}) when is_list(databases) do
    root = ElixirDB.Config.database_root()

    with true <-
           Enum.all?(databases, fn entry ->
             is_map(entry) and Map.keys(entry) in [["path", "uuid"], ["uuid", "path"]]
           end),
         {:ok, values} <- normalize_entries(databases),
         :ok <- ensure_unique(values) do
      {:ok,
       Enum.map(values, fn %{"uuid" => uuid, "path" => relative} ->
         bundle_root = Path.join(root, relative)

         %{
           uuid: uuid,
           path: relative,
           bundle_root: bundle_root,
           sqlite_path: Path.join(bundle_root, "database.sqlite3"),
           status: :unknown
         }
       end)}
    else
      false ->
        {:error, ElixirDB.Error.invalid_request("registration manifest contains an invalid entry")}

      {:error, _} = error ->
        error
    end
  end

  defp validate(_),
    do: {:error, ElixirDB.Error.invalid_request("registration manifest version is unsupported")}

  defp safe_entry?(relative) do
    is_binary(relative) and Path.type(relative) != :absolute and relative != ".." and
      not Enum.any?(Path.split(relative), &(&1 == "..")) and not String.contains?(relative, "\\") and
      relative != "" and
      PathSafety.no_symlink_components?(Path.join(ElixirDB.Config.database_root(), relative))
  end

  defp normalize_entries(entries) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      normalize_entry(entry, acc)
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_entries(_),
    do: {:error, ElixirDB.Error.invalid_request("registration manifest databases must be an array")}

  defp normalize_entry(entry, acc) when is_map(entry) do
    uuid = Map.get(entry, "uuid", Map.get(entry, :uuid))
    relative = Map.get(entry, "path", Map.get(entry, :path))

    if is_binary(uuid) and is_binary(relative) and valid_uuid?(uuid) and safe_entry?(relative),
      do: {:cont, {:ok, [%{"uuid" => uuid, "path" => relative} | acc]}},
      else: invalid_entry(acc)
  end

  defp normalize_entry(_entry, acc), do: invalid_entry(acc)

  defp invalid_entry(_acc),
    do:
      {:halt,
       {:error, ElixirDB.Error.invalid_request("registration manifest contains an invalid entry")}}

  defp ensure_unique(entries) do
    uuids = Enum.map(entries, & &1["uuid"])
    paths = Enum.map(entries, & &1["path"])

    if length(uuids) == length(Enum.uniq(uuids)) and length(paths) == length(Enum.uniq(paths)),
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.duplicate_database_uuid("registration manifest contains duplicates")}
  end

  defp valid_uuid?(uuid),
    do:
      Regex.match?(
        ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        uuid
      )
end
