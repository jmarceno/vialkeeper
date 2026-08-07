defmodule ElixirDB.Runtime.RegistrationManifest do
  @moduledoc "Atomic, routing-only registration manifest."

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
        with {:ok, map} <- ElixirDB.JSON.StrictDecoder.decode(body),
             {:ok, entries} <- validate(map),
             do: {:ok, entries}

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
         {:ok, json} <- ElixirDB.JSON.Canonical.encode(%{"version" => 1, "databases" => normalized}),
         :ok <- ElixirDB.Runtime.AtomicWrite.write(path(), json) do
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
         %{uuid: uuid, path: relative, absolute_path: Path.join(root, relative), status: :unknown}
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
      no_symlink_components?(Path.join(ElixirDB.Config.database_root(), relative))
  end

  defp normalize_entries(entries) when is_list(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      if is_map(entry) do
        uuid = Map.get(entry, "uuid", Map.get(entry, :uuid))
        relative = Map.get(entry, "path", Map.get(entry, :path))

        if is_binary(uuid) and is_binary(relative) and valid_uuid?(uuid) and safe_entry?(relative),
          do: {:cont, {:ok, [%{"uuid" => uuid, "path" => relative} | acc]}},
          else:
            {:halt,
             {:error,
              ElixirDB.Error.invalid_request("registration manifest contains an invalid entry")}}
      else
        {:halt,
         {:error, ElixirDB.Error.invalid_request("registration manifest contains an invalid entry")}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_entries(_),
    do: {:error, ElixirDB.Error.invalid_request("registration manifest databases must be an array")}

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

  defp no_symlink_components?(path) do
    path
    |> Path.split()
    |> Enum.reduce_while("", fn component, current ->
      next = Path.join(current, component)

      case File.lstat(next) do
        {:ok, %File.Stat{type: :symlink}} -> {:halt, false}
        {:ok, _} -> {:cont, next}
        {:error, :enoent} -> {:halt, true}
        {:error, _} -> {:halt, false}
      end
    end)
    |> case do
      false -> false
      _ -> true
    end
  end
end
