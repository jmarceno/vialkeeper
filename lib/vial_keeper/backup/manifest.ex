defmodule VialKeeper.Backup.Manifest do
  @moduledoc """
  Verified backup generation manifest for a closed `.vialkeeper` bundle.

  A manifest is a sidecar JSON document stored **next to** a closed bundle
  copy, never inside the bundle. It records the generation identity, creation
  time, source UUID and path, release diagnostics, storage versions, byte
  sizes, content hashes, and the post-close integrity result. A generation
  that has not been restored is unverified; the manifest makes the generation
  auditable and reproducible.

  The manifest format is versioned. Version 1 is the only version shipped in
  V1. The file name for a bundle `notes.vialkeeper` is
  `notes.vialkeeper.manifest.json` placed beside the bundle directory. When a
  generation directory contains one bundle, the same file may be named
  `manifest.json` inside the generation directory as long as it is not placed
  under the bundle's `blobs/` or `tmp/` subdirectories.

  No product backup API is required. Operators create generations with ordinary
  OS copy after `POST /v1/databases/:uuid/close`, then use
  `VialKeeper.Storage.SQLite.BackupManifest` to run the offline integrity check
  and write a manifest that follows this JSON contract.

  See `Operations.md`, `Offline copy, move, and restore`, for the operator
  procedure and the integrity requirements for a verified generation.
  """

  alias VialKeeper.Diagnostics
  alias VialKeeper.Error
  alias VialKeeper.UUID

  @manifest_version 1
  @manifest_filename_suffix ".manifest.json"
  @storage_version_keys [
    "file_format_version",
    "logical_schema_version",
    "revision_algorithm_version",
    "canonicalization_version",
    "replication_protocol_major"
  ]
  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i
  @uuid_v4_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @type t :: %{
          required(String.t()) => term()
        }

  @doc "Current manifest version."
  @spec manifest_version() :: pos_integer()
  def manifest_version, do: @manifest_version

  @doc "Suffix for a bundle sidecar manifest file."
  @spec manifest_suffix() :: String.t()
  def manifest_suffix, do: @manifest_filename_suffix

  @doc """
  Returns the expected sidecar manifest path for a bundle path.

  For a bundle at `notes.vialkeeper`, the manifest is
  `notes.vialkeeper.manifest.json` in the same parent directory.
  """
  @spec manifest_path_for_bundle(String.t()) :: String.t()
  def manifest_path_for_bundle(bundle_path) when is_binary(bundle_path) do
    bundle_path <> @manifest_filename_suffix
  end

  @doc """
  Builds a manifest map from explicit fields.

  Required keys:

  - `:generation_id` - UUID v4 for this generation
  - `:database_uuid` - source database UUID
  - `:source_path` - relative bundle path as registered (e.g. `"notes.vialkeeper"`)
  - `:bundle_path` - absolute or relative path to the closed bundle copy on disk
  - `:created_at` - `DateTime` in UTC or ISO8601 string
  - `:diagnostics` - `Diagnostics.runtime/0` map (or a map with at least `app_version`)
  - `:storage` - map with `file_format_version` and `logical_schema_version`
  - `:integrity` - result map from `POST /v1/databases/:uuid/integrity-check` (`%{ok: true}`)

  Optional keys:

  - `:bundle_bytes` - total bytes of the bundle directory (computed if not given)
  - `:sqlite_bytes` / `:sqlite_sha256` - computed from `bundle_path/database.sqlite3` if not given
  - `:blobs` - computed by inventorying `bundle_path/blobs` if not given

  Returns `{:ok, manifest}` or `{:error, reason}`.
  """
  @spec build(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def build(attrs) when is_map(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, generation_id} <- fetch_generation_id(attrs),
         {:ok, database_uuid} <- fetch_database_uuid(attrs),
         {:ok, source_path} <- fetch_source_path(attrs),
         {:ok, bundle_path} <- fetch_bundle_path(attrs),
         {:ok, created_at} <- fetch_created_at(attrs),
         {:ok, diagnostics} <- fetch_diagnostics(attrs),
         {:ok, storage} <- fetch_storage(attrs),
         {:ok, integrity} <- fetch_integrity(attrs),
         {:ok, bundle_bytes} <- fetch_bundle_bytes(attrs, bundle_path),
         {:ok, sqlite} <- fetch_sqlite(attrs, bundle_path),
         {:ok, blobs} <- fetch_blobs(attrs, bundle_path) do
      manifest = %{
        "manifest_version" => @manifest_version,
        "generation_id" => generation_id,
        "created_at" => created_at,
        "database_uuid" => database_uuid,
        "source_path" => source_path,
        "bundle_bytes" => bundle_bytes,
        "diagnostics" => diagnostics,
        "storage" => storage,
        "artifacts" => %{
          "sqlite" => sqlite,
          "blobs" => blobs
        },
        "integrity" => integrity
      }

      case validate(manifest) do
        :ok -> {:ok, manifest}
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Builds and writes a manifest for a closed bundle.

  The bundle at `bundle_path` must be a closed `.vialkeeper` directory
  (no `-wal`/`-shm` hot journal, no `.lease`). The manifest is written
  atomically to `manifest_path`, which defaults to
  `manifest_path_for_bundle(bundle_path)` when not given.

  Required options mirror `build/1`:

  - `:generation_id` - UUID, auto-generated if not given
  - `:database_uuid` - required
  - `:source_path` - required
  - `:created_at` - defaults to now in UTC
  - `:diagnostics` - defaults to `Diagnostics.runtime/0`
  - `:storage` - required
  - `:integrity` - **required**; must be the post-close integrity result

  `VialKeeper.Storage.SQLite.BackupManifest.write/2` derives `database_uuid`,
  `storage`, and `integrity` from the closed bundle instead of trusting caller
  metadata. The Mix task uses that backend-owned entry point.

  Returns `{:ok, manifest}` or `{:error, reason}`.
  """
  @spec write(String.t(), map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def write(bundle_path, attrs \\ %{}) when is_binary(bundle_path) do
    with {:ok, attrs} <- normalize_attrs(attrs) do
      manifest_path = Map.get(attrs, "manifest_path") || manifest_path_for_bundle(bundle_path)

      with :ok <- ensure_manifest_sidecar_path(bundle_path, manifest_path),
           :ok <- ensure_no_lease(bundle_path),
           :ok <- ensure_no_hot_journal(bundle_path),
           {:ok, normalized} <- normalize_write_attrs(bundle_path, attrs),
           {:ok, manifest} <- build(normalized),
           {:ok, json} <- encode_json(manifest),
           :ok <- atomic_write(manifest_path, json) do
        {:ok, manifest}
      end
    end
  end

  @doc """
  Verifies a manifest against the bundle on disk.

  Checks:

  - JSON structure and required fields
  - `sqlite` bytes and SHA-256 match the file
  - `blobs` count, bytes, tree hash, and per-entry hashes match the `blobs/` tree
  - no `.lease` and no hot journal sidecars remain

  `database_uuid` and `storage` versions are validated for presence and type;
  a full logical re-check of the bundle is `POST /v1/databases/:uuid/integrity-check`.
  `integrity.ok` must be `true`.

  Returns `:ok` or `{:error, reason}`.
  """
  @spec verify(String.t(), t() | String.t()) :: :ok | {:error, VialKeeper.Error.t()}
  def verify(bundle_path, manifest_or_path) when is_binary(bundle_path) do
    with {:ok, manifest} <- load_manifest(manifest_or_path),
         :ok <- validate(manifest),
         :ok <- ensure_no_lease(bundle_path),
         :ok <- ensure_no_hot_journal(bundle_path),
         :ok <- verify_bundle_path(manifest, bundle_path),
         :ok <- verify_sqlite(manifest, bundle_path) do
      verify_blobs(manifest, bundle_path)
    end
  end

  @doc """
  Validates a manifest map's required structure.

  Does not verify hashes against disk; use `verify/2` for that.
  """
  @spec validate(t()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate(manifest) when is_map(manifest) do
    with :ok <- validate_version(manifest),
         :ok <- validate_generation_id(manifest),
         :ok <- validate_created_at(manifest),
         :ok <- validate_database_uuid(manifest),
         :ok <- validate_source_path(manifest),
         :ok <- validate_bundle_bytes(manifest),
         :ok <- validate_diagnostics(manifest),
         :ok <- validate_storage(manifest),
         :ok <- validate_artifacts(manifest) do
      validate_integrity(manifest)
    end
  end

  def validate(_), do: {:error, VialKeeper.Error.invalid_request("manifest must be a map")}

  @doc "Encodes a manifest to canonical JSON."
  @spec encode_json(t()) :: {:ok, String.t()} | {:error, VialKeeper.Error.t()}
  def encode_json(manifest) when is_map(manifest) do
    {:ok, JSON.encode!(manifest)}
  rescue
    exception in [ArgumentError, Protocol.UndefinedError] ->
      {:error,
       VialKeeper.Error.internal_error("manifest JSON encoding failed", %{cause: inspect(exception)})}
  end

  @doc "Decodes a manifest from JSON."
  @spec decode_json(String.t()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def decode_json(json) when is_binary(json) do
    case JSON.decode(json) do
      {:ok, manifest} when is_map(manifest) ->
        {:ok, manifest}

      {:ok, _} ->
        {:error, VialKeeper.Error.invalid_request("manifest JSON must be an object")}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.invalid_request("manifest JSON is invalid", %{cause: inspect(reason)})}
    end
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  defp normalize_attrs(attrs) when is_map(attrs) do
    case VialKeeper.MapAccess.string_keys(attrs) do
      {:ok, normalized} ->
        normalize_map_values(normalized)

      :key_collision ->
        {:error, VialKeeper.Error.invalid_request("map has atom/string key collision")}
    end
  end

  defp normalize_write_attrs(bundle_path, attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs) do
      generation_id = Map.get(attrs, "generation_id") || UUID.v4()
      created_at = Map.get(attrs, "created_at") || DateTime.utc_now()
      diagnostics = Map.get(attrs, "diagnostics") || Diagnostics.runtime()
      source_path = Map.get(attrs, "source_path")
      database_uuid = Map.get(attrs, "database_uuid")
      storage = Map.get(attrs, "storage")

      {:ok,
       %{
         generation_id: generation_id,
         database_uuid: database_uuid,
         source_path: source_path,
         bundle_path: bundle_path,
         created_at: created_at,
         diagnostics: diagnostics,
         storage: storage,
         integrity: Map.get(attrs, "integrity"),
         bundle_bytes: Map.get(attrs, "bundle_bytes"),
         sqlite: Map.get(attrs, "sqlite"),
         blobs: Map.get(attrs, "blobs")
       }}
    end
  end

  defp fetch_generation_id(attrs) do
    case Map.get(attrs, "generation_id") do
      nil -> {:error, VialKeeper.Error.invalid_request("generation_id is required")}
      id when is_binary(id) -> with :ok <- validate_uuid_v4(id, "generation_id"), do: {:ok, id}
      _ -> {:error, VialKeeper.Error.invalid_request("generation_id must be a UUID string")}
    end
  end

  defp fetch_database_uuid(attrs) do
    case Map.get(attrs, "database_uuid") do
      nil -> {:error, VialKeeper.Error.invalid_request("database_uuid is required")}
      id when is_binary(id) -> with :ok <- validate_uuid(id, "database_uuid"), do: {:ok, id}
      _ -> {:error, VialKeeper.Error.invalid_request("database_uuid must be a UUID string")}
    end
  end

  defp fetch_source_path(attrs) do
    case Map.get(attrs, "source_path") do
      nil -> {:error, VialKeeper.Error.invalid_request("source_path is required")}
      path when is_binary(path) -> with :ok <- validate_relative_source_path(path), do: {:ok, path}
      _ -> {:error, VialKeeper.Error.invalid_request("source_path must be a relative path")}
    end
  end

  defp fetch_bundle_path(attrs) do
    case Map.get(attrs, "bundle_path") do
      nil -> {:error, VialKeeper.Error.invalid_request("bundle_path is required")}
      path when is_binary(path) and byte_size(path) > 0 -> {:ok, path}
      _ -> {:error, VialKeeper.Error.invalid_request("bundle_path must be a non-empty string")}
    end
  end

  defp fetch_created_at(attrs) do
    case Map.get(attrs, "created_at") do
      nil ->
        {:ok, DateTime.utc_now() |> DateTime.to_iso8601()}

      %DateTime{} = dt ->
        {:ok, dt |> DateTime.shift_zone!("Etc/UTC") |> DateTime.to_iso8601()}

      bin when is_binary(bin) ->
        case DateTime.from_iso8601(bin) do
          {:ok, dt, _} -> {:ok, dt |> DateTime.shift_zone!("Etc/UTC") |> DateTime.to_iso8601()}
          _ -> {:error, VialKeeper.Error.invalid_request("created_at must be ISO8601 UTC")}
        end

      _ ->
        {:error, VialKeeper.Error.invalid_request("created_at must be DateTime or ISO8601 string")}
    end
  end

  defp fetch_diagnostics(attrs) do
    case Map.get(attrs, "diagnostics") do
      nil -> {:error, VialKeeper.Error.invalid_request("diagnostics is required")}
      diag when is_map(diag) -> {:ok, diag}
      _ -> {:error, VialKeeper.Error.invalid_request("diagnostics must be a map")}
    end
  end

  defp fetch_storage(attrs) do
    case Map.get(attrs, "storage") do
      nil -> {:error, VialKeeper.Error.invalid_request("storage is required")}
      storage when is_map(storage) -> {:ok, storage}
      _ -> {:error, VialKeeper.Error.invalid_request("storage must be a map")}
    end
  end

  defp fetch_integrity(attrs) do
    case Map.get(attrs, "integrity") do
      nil ->
        {:error,
         VialKeeper.Error.invalid_request(
           "integrity is required (post-close integrity-check result)"
         )}

      integrity when is_map(integrity) ->
        {:ok, integrity}

      _ ->
        {:error, VialKeeper.Error.invalid_request("integrity must be a map")}
    end
  end

  defp fetch_bundle_bytes(attrs, bundle_path) do
    case Map.get(attrs, "bundle_bytes") do
      nil -> compute_bundle_bytes(bundle_path)
      bytes when is_integer(bytes) and bytes >= 0 -> {:ok, bytes}
      _ -> {:error, VialKeeper.Error.invalid_request("bundle_bytes must be a non-negative integer")}
    end
  end

  defp fetch_sqlite(attrs, bundle_path) do
    case Map.get(attrs, "sqlite") do
      nil -> compute_sqlite_artifact(bundle_path)
      artifact when is_map(artifact) -> {:ok, artifact}
      _ -> {:error, VialKeeper.Error.invalid_request("sqlite artifact must be a map")}
    end
  end

  defp fetch_blobs(attrs, bundle_path) do
    case Map.get(attrs, "blobs") do
      nil -> compute_blobs_artifact(bundle_path)
      artifact when is_map(artifact) -> {:ok, artifact}
      _ -> {:error, VialKeeper.Error.invalid_request("blobs artifact must be a map")}
    end
  end

  defp compute_bundle_bytes(bundle_path) do
    case bundle_size(bundle_path) do
      {:ok, bytes} ->
        {:ok, bytes}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("cannot compute bundle_bytes", %{cause: inspect(reason)})}
    end
  end

  defp compute_sqlite_artifact(bundle_path) do
    sqlite_path = Path.join(bundle_path, "database.sqlite3")

    with {:ok, bytes} <- file_bytes(sqlite_path),
         {:ok, sha256} <- hash_file(sqlite_path) do
      {:ok,
       %{
         "relative_path" => "database.sqlite3",
         "bytes" => bytes,
         "sha256" => sha256
       }}
    else
      {:error, %VialKeeper.Error{} = error} -> {:error, error}
    end
  end

  defp compute_blobs_artifact(bundle_path) do
    blobs_path = Path.join(bundle_path, "blobs")

    case inventory_blobs(blobs_path) do
      {:ok, entries} ->
        total_bytes = Enum.reduce(entries, 0, fn entry, acc -> acc + entry.bytes end)
        tree_sha = blobs_tree_sha256(entries)

        {:ok,
         %{
           "count" => length(entries),
           "bytes" => total_bytes,
           "sha256" => tree_sha,
           "entries" =>
             Enum.map(entries, fn entry ->
               %{
                 "digest" => entry.digest,
                 "bytes" => entry.bytes,
                 "sha256" => entry.sha256,
                 "relative_path" => entry.relative_path
               }
             end)
         }}

      {:error, _} = error ->
        error
    end
  end

  defp inventory_blobs(blobs_path) do
    if File.dir?(blobs_path), do: list_blob_prefixes(blobs_path), else: {:ok, []}
  end

  defp list_blob_prefixes(blobs_path) do
    case File.ls(blobs_path) do
      {:ok, prefixes} ->
        collect_blob_prefixes(blobs_path, prefixes)

      {:error, reason} ->
        {:error, Error.internal_error("cannot list blobs directory", %{cause: inspect(reason)})}
    end
  end

  defp collect_blob_prefixes(blobs_path, prefixes) do
    Enum.reduce_while(prefixes, {:ok, []}, fn prefix, {:ok, acc} ->
      case inventory_blob_prefix(blobs_path, prefix) do
        {:ok, entries} -> {:cont, {:ok, [entries | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, acc |> Enum.reverse() |> List.flatten()}
      {:error, _} = error -> error
    end
  end

  defp inventory_blob_prefix(blobs_path, prefix) do
    prefix_path = Path.join(blobs_path, prefix)

    case File.lstat(prefix_path) do
      {:ok, %File.Stat{type: :directory}} ->
        validate_and_inventory_prefix(blobs_path, prefix)

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, Error.integrity_violation("blob path contains a symlink", %{path: prefix_path})}

      {:ok, _} ->
        {:error, Error.integrity_violation("blobs entry has invalid type", %{path: prefix_path})}

      {:error, reason} ->
        {:error, Error.internal_error("cannot list blobs", %{cause: inspect(reason)})}
    end
  end

  defp validate_and_inventory_prefix(blobs_path, prefix) do
    if prefix =~ ~r/^[0-9a-f]{2}$/ do
      inventory_prefix(blobs_path, prefix)
    else
      {:error, Error.integrity_violation("blob prefix directory is malformed", %{prefix: prefix})}
    end
  end

  defp inventory_prefix(blobs_path, prefix) do
    prefix_path = Path.join(blobs_path, prefix)

    case File.ls(prefix_path) do
      {:ok, files} ->
        Enum.reduce_while(files, {:ok, []}, &collect_blob_file(prefix_path, prefix, &1, &2))

      {:error, reason} ->
        {:error, Error.internal_error("cannot list blob prefix", %{cause: inspect(reason)})}
    end
  end

  defp collect_blob_file(prefix_path, prefix, file, {:ok, acc}) do
    case inventory_blob_file(prefix_path, prefix, file) do
      {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp inventory_blob_file(prefix_path, prefix, file) do
    file_path = Path.join(prefix_path, file)

    case File.lstat(file_path) do
      {:ok, %File.Stat{type: :regular}} ->
        hash_blob_file(file_path, prefix, file)

      {:ok, %File.Stat{type: :symlink}} ->
        {:error, Error.integrity_violation("blob representation is a symlink", %{path: file_path})}

      {:ok, _} ->
        {:error, Error.integrity_violation("blob entry has invalid type", %{path: file_path})}

      {:error, reason} ->
        {:error, Error.internal_error("cannot stat blob file", %{cause: inspect(reason)})}
    end
  end

  defp hash_blob_file(file_path, prefix, file) do
    with {:ok, digest} <- parse_blob_filename(prefix, file),
         {:ok, bytes} <- file_bytes(file_path),
         {:ok, sha256} <- hash_file(file_path) do
      {:ok,
       %{
         digest: digest,
         bytes: bytes,
         sha256: sha256,
         relative_path: Path.join(["blobs", prefix, file])
       }}
    else
      :error -> {:error, Error.integrity_violation("blob filename is malformed", %{file: file})}
      {:error, _} = error -> error
    end
  end

  defp parse_blob_filename(prefix, file) do
    # quality:reason parsing mirrors SQLite integrity, while this layer owns
    # the storage-neutral offline manifest inventory
    case Regex.run(~r/^([0-9a-f]{64})\.blob$/, file) do
      [_, digest] when binary_part(digest, 0, 2) == prefix -> {:ok, digest}
      [_, _digest] -> :error
      _ -> :error
    end
  end

  defp blobs_tree_sha256([]), do: :crypto.hash(:sha256, "") |> Base.encode16(case: :lower)

  defp blobs_tree_sha256(entries) do
    sorted = Enum.sort_by(entries, & &1.digest)

    payload =
      Enum.map_join(sorted, "\n", fn entry -> "#{entry.digest}:#{entry.bytes}:#{entry.sha256}" end)

    :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
  end

  defp bundle_size(bundle_path) do
    bundle_path
    |> Path.expand()
    |> do_bundle_size(0)
  end

  defp do_bundle_size(path, acc) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory}} ->
        bundle_directory_size(path, acc)

      {:ok, %File.Stat{type: :regular, size: size}} ->
        {:ok, acc + size}

      {:ok, %File.Stat{type: :symlink}} ->
        {:error,
         VialKeeper.Error.integrity_violation("bundle path contains a symlink", %{path: path})}

      {:ok, _} ->
        {:ok, acc}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp bundle_directory_size(path, acc) do
    case File.ls(path) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, acc}, &accumulate_bundle_size(path, &1, &2))

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp accumulate_bundle_size(path, entry, {:ok, acc}) do
    case do_bundle_size(Path.join(path, entry), acc) do
      {:ok, next} -> {:cont, {:ok, next}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp file_bytes(path) do
    case File.stat(path) do
      {:ok, %File.Stat{size: size, type: :regular}} ->
        {:ok, size}

      {:ok, %File.Stat{}} ->
        {:error, Error.invalid_request("path is not a regular file", %{path: path})}

      {:error, :enoent} ->
        {:error, Error.invalid_request("file does not exist", %{path: path})}

      {:error, reason} ->
        {:error, Error.internal_error("cannot stat file", %{cause: inspect(reason)})}
    end
  end

  defp hash_file(path) do
    case File.open(path, [:read, :binary]) do
      {:ok, io} ->
        try do
          hash_io(io, :crypto.hash_init(:sha256))
        after
          File.close(io)
        end

      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("cannot open file for hashing", %{cause: inspect(reason)})}
    end
  end

  defp hash_io(io, ctx) do
    case IO.binread(io, 1_048_576) do
      :eof ->
        {:ok, :crypto.hash_final(ctx) |> Base.encode16(case: :lower)}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("cannot read file for hashing", %{cause: inspect(reason)})}

      data when is_binary(data) ->
        hash_io(io, :crypto.hash_update(ctx, data))
    end
  end

  defp ensure_no_lease(bundle_path) do
    lease_path = bundle_path <> ".lease"

    if File.exists?(lease_path) do
      {:error,
       VialKeeper.Error.invalid_request("closed bundle must not have a .lease file", %{
         path: lease_path
       })}
    else
      :ok
    end
  end

  defp ensure_no_hot_journal(bundle_path) do
    wal = Path.join(bundle_path, "database.sqlite3-wal")
    shm = Path.join(bundle_path, "database.sqlite3-shm")

    cond do
      File.exists?(wal) ->
        {:error,
         VialKeeper.Error.invalid_request("closed bundle must not have a hot WAL file", %{path: wal})}

      File.exists?(shm) ->
        {:error,
         VialKeeper.Error.invalid_request("closed bundle must not have a hot SHM file", %{path: shm})}

      true ->
        :ok
    end
  end

  defp verify_bundle_path(manifest, bundle_path) do
    expected_bytes = manifest["bundle_bytes"]

    case bundle_size(bundle_path) do
      {:ok, actual} when actual == expected_bytes ->
        :ok

      {:ok, actual} ->
        {:error,
         VialKeeper.Error.integrity_violation("bundle_bytes mismatch", %{
           expected: expected_bytes,
           actual: actual
         })}

      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.internal_error("cannot verify bundle size", %{cause: inspect(reason)})}
    end
  end

  defp verify_sqlite(manifest, bundle_path) do
    sqlite_path = Path.join(bundle_path, "database.sqlite3")
    expected = manifest["artifacts"]["sqlite"] || %{}

    with {:ok, actual_bytes} <- file_bytes(sqlite_path),
         {:ok, actual_sha} <- hash_file(sqlite_path) do
      expected_bytes = expected["bytes"]
      expected_sha = expected["sha256"]

      cond do
        expected_bytes != actual_bytes ->
          {:error,
           VialKeeper.Error.integrity_violation("sqlite bytes mismatch", %{
             expected: expected_bytes,
             actual: actual_bytes
           })}

        expected_sha != actual_sha ->
          {:error,
           VialKeeper.Error.integrity_violation("sqlite sha256 mismatch", %{
             expected: expected_sha,
             actual: actual_sha
           })}

        true ->
          :ok
      end
    end
  end

  defp verify_blobs(manifest, bundle_path) do
    blobs_path = Path.join(bundle_path, "blobs")
    expected = manifest["artifacts"]["blobs"] || %{}

    case inventory_blobs(blobs_path) do
      {:ok, entries} ->
        expected_count = expected["count"]
        expected_bytes = expected["bytes"]
        expected_sha = expected["sha256"]

        actual_count = length(entries)
        actual_bytes = Enum.reduce(entries, 0, fn entry, acc -> acc + entry.bytes end)
        actual_sha = blobs_tree_sha256(entries)

        cond do
          expected_count != actual_count ->
            {:error,
             Error.integrity_violation("blobs count mismatch", %{
               expected: expected_count,
               actual: actual_count
             })}

          expected_bytes != actual_bytes ->
            {:error,
             Error.integrity_violation("blobs bytes mismatch", %{
               expected: expected_bytes,
               actual: actual_bytes
             })}

          expected_sha != actual_sha ->
            {:error,
             Error.integrity_violation("blobs sha256 mismatch", %{
               expected: expected_sha,
               actual: actual_sha
             })}

          true ->
            verify_blob_entries(expected["entries"] || [], entries)
        end

      {:error, _} = error ->
        error
    end
  end

  defp verify_blob_entries([], _actual), do: :ok

  defp verify_blob_entries(expected_entries, actual_entries) do
    expected_map = Map.new(expected_entries, fn entry -> {entry["digest"], entry} end)
    actual_map = Map.new(actual_entries, fn entry -> {entry.digest, entry} end)

    mismatches =
      Enum.filter(expected_map, fn {digest, expected} ->
        case Map.get(actual_map, digest) do
          nil -> true
          actual -> expected["bytes"] != actual.bytes or expected["sha256"] != actual.sha256
        end
      end)

    if mismatches == [] and map_size(expected_map) == map_size(actual_map) do
      :ok
    else
      {:error,
       VialKeeper.Error.integrity_violation("blobs entries mismatch", %{
         expected: map_size(expected_map),
         actual: map_size(actual_map)
       })}
    end
  end

  defp load_manifest(manifest) when is_map(manifest), do: {:ok, manifest}

  defp load_manifest(path) when is_binary(path) do
    case File.read(path) do
      {:ok, content} ->
        decode_json(content)

      {:error, reason} ->
        {:error,
         VialKeeper.Error.invalid_request("cannot read manifest file", %{cause: inspect(reason)})}
    end
  end

  defp normalize_map_values(map) do
    Enum.reduce_while(map, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case normalize_value(value) do
        {:ok, normalized} -> {:cont, {:ok, Map.put(acc, key, normalized)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp normalize_value(%_{} = value), do: {:ok, value}

  defp normalize_value(value) when is_map(value) do
    case VialKeeper.MapAccess.string_keys(value) do
      {:ok, normalized} ->
        normalize_map_values(normalized)

      :key_collision ->
        {:error, VialKeeper.Error.invalid_request("map has atom/string key collision")}
    end
  end

  defp normalize_value(value) when is_list(value) do
    Enum.reduce_while(value, {:ok, []}, fn item, {:ok, acc} ->
      case normalize_value(item) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _} = error -> error
    end
  end

  defp normalize_value(value), do: {:ok, value}

  defp validate_version(%{"manifest_version" => @manifest_version}), do: :ok

  defp validate_version(%{"manifest_version" => other}),
    do: {:error, VialKeeper.Error.invalid_request("unsupported manifest_version", %{got: other})}

  defp validate_version(_),
    do: {:error, VialKeeper.Error.invalid_request("manifest_version is required")}

  defp validate_generation_id(%{"generation_id" => id}) when is_binary(id) do
    validate_uuid_v4(id, "generation_id")
  end

  defp validate_generation_id(_),
    do: {:error, VialKeeper.Error.invalid_request("generation_id is required")}

  defp validate_created_at(%{"created_at" => bin}) when is_binary(bin) do
    case DateTime.from_iso8601(bin) do
      {:ok, _dt, 0} -> :ok
      _ -> {:error, VialKeeper.Error.invalid_request("created_at must be ISO8601 UTC")}
    end
  end

  defp validate_created_at(_),
    do: {:error, VialKeeper.Error.invalid_request("created_at is required")}

  defp validate_database_uuid(%{"database_uuid" => id}) when is_binary(id) do
    validate_uuid(id, "database_uuid")
  end

  defp validate_database_uuid(_),
    do: {:error, VialKeeper.Error.invalid_request("database_uuid is required")}

  defp validate_source_path(%{"source_path" => path}) when is_binary(path),
    do: validate_relative_source_path(path)

  defp validate_source_path(_),
    do: {:error, VialKeeper.Error.invalid_request("source_path is required")}

  defp validate_bundle_bytes(%{"bundle_bytes" => bytes}) when is_integer(bytes) and bytes >= 0,
    do: :ok

  defp validate_bundle_bytes(_),
    do: {:error, VialKeeper.Error.invalid_request("bundle_bytes is required")}

  defp validate_diagnostics(%{"diagnostics" => diag}) when is_map(diag) do
    valid? =
      is_binary(diag["app_version"]) and is_binary(diag["elixir"]) and
        is_binary(diag["otp"]) and is_integer(diag["protocol_major"]) and
        is_integer(diag["revision_algorithm_version"]) and
        is_integer(diag["canonicalization_version"]) and
        is_binary(diag["storage_backend"]) and is_map(diag["backend"])

    if valid? do
      :ok
    else
      {:error,
       VialKeeper.Error.invalid_request("diagnostics must contain Diagnostics.runtime/0 identity")}
    end
  end

  defp validate_diagnostics(_),
    do: {:error, VialKeeper.Error.invalid_request("diagnostics is required")}

  defp validate_storage(%{"storage" => storage}) when is_map(storage) do
    Enum.reduce_while(@storage_version_keys, :ok, fn key, :ok ->
      case require_integer(storage, key) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_storage(_), do: {:error, VialKeeper.Error.invalid_request("storage is required")}

  defp validate_artifacts(%{"artifacts" => artifacts}) when is_map(artifacts) do
    with :ok <- validate_sqlite_artifact(artifacts["sqlite"]) do
      validate_blobs_artifact(artifacts["blobs"])
    end
  end

  defp validate_artifacts(_),
    do: {:error, VialKeeper.Error.invalid_request("artifacts is required")}

  defp validate_sqlite_artifact(%{"relative_path" => path, "bytes" => bytes, "sha256" => sha})
       when is_binary(path) and is_integer(bytes) and bytes >= 0 and is_binary(sha) do
    if path == "database.sqlite3" and sha =~ ~r/^[0-9a-f]{64}$/ do
      :ok
    else
      {:error,
       VialKeeper.Error.invalid_request(
         "artifacts.sqlite must name database.sqlite3 with lowercase SHA-256 hex"
       )}
    end
  end

  defp validate_sqlite_artifact(_),
    do: {:error, VialKeeper.Error.invalid_request("artifacts.sqlite is invalid")}

  defp validate_blobs_artifact(%{
         "count" => count,
         "bytes" => bytes,
         "sha256" => sha,
         "entries" => entries
       })
       when is_integer(count) and count >= 0 and is_integer(bytes) and bytes >= 0 and
              is_binary(sha) and is_list(entries) do
    with true <- sha =~ ~r/^[0-9a-f]{64}$/,
         :ok <- validate_blob_entries(entries),
         true <- count == length(entries) do
      :ok
    else
      false ->
        {:error,
         VialKeeper.Error.invalid_request(
           "artifacts.blobs hash, count, or entries are inconsistent"
         )}

      {:error, _} = error ->
        error
    end
  end

  defp validate_blobs_artifact(_),
    do: {:error, VialKeeper.Error.invalid_request("artifacts.blobs is invalid")}

  defp validate_integrity(%{"integrity" => %{"ok" => true}}), do: :ok

  defp validate_integrity(%{"integrity" => integrity}) when is_map(integrity) do
    if integrity["ok"] == true do
      :ok
    else
      {:error,
       VialKeeper.Error.invalid_request("integrity.ok must be true for a verified generation")}
    end
  end

  defp validate_integrity(_),
    do: {:error, VialKeeper.Error.invalid_request("integrity is required")}

  defp require_integer(map, key) do
    value = Map.get(map, key)

    if is_integer(value) and value >= 0 do
      :ok
    else
      {:error, VialKeeper.Error.invalid_request("#{key} must be a non-negative integer")}
    end
  end

  defp validate_uuid(value, field) do
    if Regex.match?(@uuid_pattern, value) do
      :ok
    else
      {:error, VialKeeper.Error.invalid_request("#{field} must be a UUID")}
    end
  end

  defp validate_uuid_v4(value, field) do
    if Regex.match?(@uuid_v4_pattern, value) do
      :ok
    else
      {:error, VialKeeper.Error.invalid_request("#{field} must be a UUID v4")}
    end
  end

  defp validate_relative_source_path(path) do
    valid? =
      path != "" and Path.type(path) != :absolute and path != "." and path != ".." and
        not Enum.any?(Path.split(path), &(&1 in [".", ".."])) and
        not String.contains?(path, "\\")

    if valid?,
      do: :ok,
      else: {:error, VialKeeper.Error.invalid_request("source_path must be a safe relative path")}
  end

  defp validate_blob_entries(entries) do
    with {:ok, digests} <- collect_blob_entry_digests(entries),
         true <- length(digests) == length(Enum.uniq(digests)) do
      :ok
    else
      false -> {:error, VialKeeper.Error.invalid_request("blob entry digests must be unique")}
      {:error, _} = error -> error
    end
  end

  defp collect_blob_entry_digests(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, digests} ->
      case validate_blob_entry(entry) do
        {:ok, digest} -> {:cont, {:ok, [digest | digests]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_blob_entry(%{
         "digest" => digest,
         "bytes" => bytes,
         "sha256" => sha,
         "relative_path" => relative_path
       })
       when is_binary(digest) and is_integer(bytes) and bytes >= 0 and is_binary(sha) and
              is_binary(relative_path) do
    expected_path =
      Path.join(["blobs", binary_part(digest, 0, min(byte_size(digest), 2)), "#{digest}.blob"])

    if digest =~ ~r/^[0-9a-f]{64}$/ and sha =~ ~r/^[0-9a-f]{64}$/ and
         relative_path == expected_path do
      {:ok, digest}
    else
      {:error, VialKeeper.Error.invalid_request("artifacts.blobs entry is invalid")}
    end
  end

  defp validate_blob_entry(_),
    do: {:error, VialKeeper.Error.invalid_request("artifacts.blobs entry is invalid")}

  defp ensure_manifest_sidecar_path(bundle_path, manifest_path) do
    expanded_bundle = Path.expand(bundle_path)
    expanded_manifest = Path.expand(manifest_path)
    same_parent? = Path.dirname(expanded_bundle) == Path.dirname(expanded_manifest)

    if VialKeeper.PathSafety.within_root?(expanded_manifest, expanded_bundle) or not same_parent? do
      {:error,
       VialKeeper.Error.invalid_request("backup manifest must be stored beside the bundle", %{
         path: manifest_path
       })}
    else
      :ok
    end
  end

  defp atomic_write(path, content) do
    dir = Path.dirname(path)

    with :ok <- File.mkdir_p(dir) do
      case VialKeeper.AtomicWrite.write(path, content) do
        :ok ->
          :ok

        {:error, reason} ->
          {:error,
           VialKeeper.Error.internal_error("cannot write manifest", %{cause: inspect(reason)})}
      end
    end
  end
end
