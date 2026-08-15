defmodule VialKeeper.Runtime.RegistrationManifest do
  @moduledoc "Atomic, routing-only registration manifest."

  alias VialKeeper.JSON.Canonical
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.PathSafety
  alias VialKeeper.Runtime.AtomicWrite

  @spec path() :: binary()
  def path do
    Application.get_env(:vial_keeper, :registration_manifest) ||
      Path.join(VialKeeper.Config.database_root(), "registrations.json")
  end

  @spec read() :: {:ok, list()} | {:error, VialKeeper.Error.t()}
  def read do
    case File.read(path()) do
      {:error, :enoent} ->
        {:ok, []}

      {:ok, body} ->
        with {:ok, map} <- StrictDecoder.decode(body), do: validate(map)

      {:error, reason} ->
        {:error,
         VialKeeper.Error.database_unavailable("registration manifest cannot be read", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec write(list()) :: :ok | {:error, VialKeeper.Error.t()}
  def write(entries) do
    with {:ok, normalized} <- normalize_entries(entries),
         {:ok, json} <- Canonical.encode(%{"version" => 1, "databases" => normalized}),
         :ok <- AtomicWrite.write(path(), json) do
      :ok
    else
      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.database_unavailable("registration manifest could not be prepared", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate(%{"version" => 1, "databases" => databases}) when is_list(databases) do
    root = VialKeeper.Config.database_root()

    with true <-
           Enum.all?(databases, fn entry ->
             is_map(entry) and
               Enum.sort(Map.keys(entry)) in [
                 ["path", "uuid"],
                 ["database_kind", "path", "uuid"]
               ]
           end),
         {:ok, values} <- normalize_entries(databases),
         :ok <- ensure_unique(values) do
      {:ok,
       Enum.map(values, fn %{
                             "uuid" => uuid,
                             "path" => relative,
                             "database_kind" => stored_kind
                           } ->
         {:ok, kind} = VialKeeper.DatabaseKind.from_storage(stored_kind)
         bundle_root = Path.join(root, relative)

         %{
           uuid: uuid,
           path: relative,
           database_kind: kind,
           bundle_root: bundle_root,
           status: :unknown
         }
       end)}
    else
      false ->
        {:error,
         VialKeeper.Error.invalid_request("registration manifest contains an invalid entry")}

      {:error, _} = error ->
        error
    end
  end

  defp validate(_),
    do: {:error, VialKeeper.Error.invalid_request("registration manifest version is unsupported")}

  defp safe_entry?(relative) do
    is_binary(relative) and Path.type(relative) != :absolute and relative != ".." and
      not Enum.any?(Path.split(relative), &(&1 == "..")) and not String.contains?(relative, "\\") and
      relative != "" and
      PathSafety.no_symlink_components?(Path.join(VialKeeper.Config.database_root(), relative))
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
    do:
      {:error, VialKeeper.Error.invalid_request("registration manifest databases must be an array")}

  defp normalize_entry(entry, acc) when is_map(entry) do
    uuid = Map.get(entry, "uuid", Map.get(entry, :uuid))
    relative = Map.get(entry, "path", Map.get(entry, :path))
    kind = Map.get(entry, "database_kind", Map.get(entry, :database_kind, "ordinary"))

    with true <-
           is_binary(uuid) and is_binary(relative) and valid_uuid?(uuid) and safe_entry?(relative),
         {:ok, normalized_kind} <- VialKeeper.DatabaseKind.normalize(kind) do
      {:cont,
       {:ok,
        [
          %{
            "uuid" => uuid,
            "path" => relative,
            "database_kind" => VialKeeper.DatabaseKind.storage(normalized_kind)
          }
          | acc
        ]}}
    else
      _ -> invalid_entry(acc)
    end
  end

  defp normalize_entry(_entry, acc), do: invalid_entry(acc)

  defp invalid_entry(_acc),
    do:
      {:halt,
       {:error, VialKeeper.Error.invalid_request("registration manifest contains an invalid entry")}}

  defp ensure_unique(entries) do
    uuids = Enum.map(entries, & &1["uuid"])
    paths = Enum.map(entries, & &1["path"])

    if length(uuids) == length(Enum.uniq(uuids)) and length(paths) == length(Enum.uniq(paths)),
      do: :ok,
      else:
        {:error,
         VialKeeper.Error.duplicate_database_uuid("registration manifest contains duplicates")}
  end

  defp valid_uuid?(uuid),
    do:
      Regex.match?(
        ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        uuid
      )
end
