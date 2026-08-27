defmodule VialKeeper.Storage.SQLite.BackupManifest do
  @moduledoc """
  Offline helpers for building backup manifests from a closed SQLite bundle.

  These helpers run outside the `DatabaseOwner` lifecycle. They open the
  bundle's `database.sqlite3` through SQLite's immutable read-only URI, run the
  logical and physical integrity checks, read `db_meta`, and supply the fields
  required by `VialKeeper.Backup.Manifest` without creating WAL sidecars.

  This module lives in the SQLite backend layer so that `application` and
  `core` code do not directly depend on `VialKeeper.Storage.SQLite.*`.
  Mix tasks and operator tooling may call it directly.
  """

  alias VialKeeper.Backup.Manifest
  alias VialKeeper.Storage.SQLite.{Connection, IndexCatalog, Integrity}

  @doc "Builds and writes a manifest using identity metadata read from the copied bundle."
  @spec write(String.t(), map()) :: {:ok, Manifest.t()} | {:error, VialKeeper.Error.t()}
  def write(bundle_path, attrs) when is_binary(bundle_path) and is_map(attrs) do
    with {:ok, identity} <- read_bundle_identity(bundle_path),
         {:ok, integrity} <- integrity_check(bundle_path) do
      attrs
      |> Map.drop([
        :database_uuid,
        "database_uuid",
        :storage,
        "storage",
        :integrity,
        "integrity"
      ])
      |> Map.put(:database_uuid, identity.database_uuid)
      |> Map.put(:storage, identity.storage)
      |> Map.put(:integrity, integrity)
      |> then(&Manifest.write(bundle_path, &1))
    end
  end

  @doc "Runs the complete logical and physical integrity check on a closed bundle."
  @spec integrity_check(String.t()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def integrity_check(bundle_path) when is_binary(bundle_path) do
    with_readonly_connection(bundle_path, fn conn ->
      with {:ok, indexes} <- IndexCatalog.list(conn, cache: false),
           {:ok, report} <- Integrity.run(conn, indexes, bundle_path) do
        {:ok, Map.put(report, :ok, true)}
      end
    end)
  end

  @doc """
  Reads the database UUID and storage versions from a closed bundle.

  Returns `{:ok, %{database_uuid: uuid, storage: storage_map}}` or `{:error, reason}`.
  """
  @spec read_bundle_identity(String.t()) ::
          {:ok, %{database_uuid: String.t(), storage: map()}} | {:error, VialKeeper.Error.t()}
  def read_bundle_identity(bundle_path) when is_binary(bundle_path) do
    with_readonly_connection(bundle_path, fn conn ->
      case Connection.query(
             conn,
             "SELECT database_uuid, file_format_version, logical_schema_version, revision_algorithm_version, canonicalization_version, replication_protocol_major FROM db_meta WHERE id = 1"
           ) do
        {:ok, [[uuid, file_fmt, logical, rev_algo, canon, repl]]} ->
          {:ok,
           %{
             database_uuid: uuid,
             storage: %{
               "file_format_version" => file_fmt,
               "logical_schema_version" => logical,
               "revision_algorithm_version" => rev_algo,
               "canonicalization_version" => canon,
               "replication_protocol_major" => repl
             }
           }}

        {:ok, []} ->
          {:error, VialKeeper.Error.integrity_violation("bundle db_meta is missing")}

        {:error, reason} ->
          {:error,
           VialKeeper.Error.integrity_violation("cannot read bundle db_meta", %{
             cause: inspect(reason)
           })}
      end
    end)
  end

  defp with_readonly_connection(bundle_path, fun) do
    sqlite_path = Path.join(bundle_path, "database.sqlite3")
    immutable_uri = "file:" <> URI.encode(sqlite_path) <> "?immutable=1"

    with :ok <- ensure_closed_bundle(bundle_path),
         {:ok, conn} <- Connection.open(immutable_uri, mode: [:readonly]) do
      result = fun.(conn)

      case Connection.close(conn) do
        :ok -> result
        {:error, reason} -> close_error(reason)
      end
    else
      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         VialKeeper.Error.database_unavailable("cannot open bundle sqlite for manifest", %{
           cause: inspect(reason)
         })}
    end
  end

  defp ensure_closed_bundle(bundle_path) do
    sidecars = [
      bundle_path <> ".lease",
      Path.join(bundle_path, "database.sqlite3-wal"),
      Path.join(bundle_path, "database.sqlite3-shm")
    ]

    case Enum.find(sidecars, &File.exists?/1) do
      nil -> :ok
      path -> {:error, VialKeeper.Error.invalid_request("bundle is not closed", %{path: path})}
    end
  end

  defp close_error(reason) do
    {:error,
     VialKeeper.Error.database_unavailable("cannot close bundle sqlite after manifest read", %{
       cause: inspect(reason)
     })}
  end
end
