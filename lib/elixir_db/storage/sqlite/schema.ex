defmodule ElixirDB.Storage.SQLite.Schema do
  @moduledoc false
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.UUID

  @application_id 0x45584442
  @type storage_mode :: :disk | :memory

  @spec configure(Connection.handle(), keyword()) :: :ok | {:error, term()}
  def configure(conn, opts \\ []) do
    storage_mode = Keyword.get(opts, :storage_mode, :disk)

    with :ok <- Connection.execute(conn, journal_mode_sql(storage_mode)),
         :ok <- Connection.execute(conn, synchronous_sql(storage_mode)),
         :ok <- Connection.execute(conn, "PRAGMA foreign_keys = ON"),
         :ok <- Connection.execute(conn, "PRAGMA locking_mode = NORMAL") do
      Connection.execute(conn, "PRAGMA trusted_schema = OFF")
    end
  end

  @spec create(Connection.handle(), binary(), binary(), keyword()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def create(conn, database_uuid, config_json, opts \\ []) do
    schema =
      File.read!(Path.join([Application.app_dir(:elixir_db), "priv", "sqlite", "schema_v1.sql"]))

    with :ok <- execute_script(conn, schema),
         :ok <- configure(conn, opts),
         {:ok, _} <-
           Connection.query(
             conn,
             "INSERT INTO db_meta VALUES (1, ?, ?, 1, 1, 1, 1, 1, 0, 0, 0, NULL, ?, ?)",
             [
               database_uuid,
               UUID.v4(),
               DateTime.utc_now() |> DateTime.to_iso8601(),
               config_json
             ]
           ) do
      :ok
    else
      {:error, reason} ->
        {:error,
         ElixirDB.Error.internal_error("could not initialize SQLite schema", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec validate(Connection.handle(), keyword()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def validate(conn, opts \\ []) do
    storage_mode = Keyword.get(opts, :storage_mode, :disk)

    with {:ok, [[application_id]]} <- Connection.pragma(conn, "application_id"),
         {:ok, [[user_version]]} <- Connection.pragma(conn, "user_version"),
         {:ok, [[journal_mode]]} <- Connection.pragma(conn, "journal_mode"),
         {:ok, [[synchronous]]} <- Connection.pragma(conn, "synchronous"),
         {:ok, [[locking_mode]]} <- Connection.pragma(conn, "locking_mode"),
         {:ok, [[trusted_schema]]} <- Connection.pragma(conn, "trusted_schema"),
         {:ok, [meta]} <-
           Connection.query(
             conn,
             "SELECT database_uuid, history_epoch, file_format_version, logical_schema_version, revision_algorithm_version, canonicalization_version, replication_protocol_major, current_sequence, retention_floor_sequence, compaction_epoch, retention_boundary_digest, config_json FROM db_meta WHERE id = 1"
           ) do
      validate_schema_metadata(
        application_id,
        user_version,
        journal_mode,
        synchronous,
        locking_mode,
        trusted_schema,
        meta,
        storage_mode
      )
    else
      {:ok, []} ->
        {:error, ElixirDB.Error.unsupported_format("SQLite file has no ElixirDB metadata")}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.unsupported_format("SQLite schema validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate_schema_metadata(
         @application_id,
         1,
         journal_mode,
         synchronous,
         locking_mode,
         trusted_schema,
         meta,
         storage_mode
       ) do
    if valid_storage_pragmas?(storage_mode, journal_mode, synchronous, locking_mode, trusted_schema) do
      validate_metadata_row(meta)
    else
      {:error,
       ElixirDB.Error.unsupported_format("SQLite file is not a Version 1 ElixirDB database")}
    end
  end

  defp validate_schema_metadata(
         _application_id,
         _user_version,
         _journal_mode,
         _synchronous,
         _locking_mode,
         _trusted_schema,
         _meta,
         _storage_mode
       ),
       do:
         {:error,
          ElixirDB.Error.unsupported_format("SQLite file is not a Version 1 ElixirDB database")}

  defp validate_metadata_row(row) do
    case row do
      [
        _uuid,
        _history_epoch,
        _format,
        _schema,
        _revision,
        _canonical,
        _protocol,
        _sequence,
        _retention_floor,
        _compaction_epoch,
        _boundary_digest,
        config_json
      ] ->
        with :ok <- validate_metadata_scalars(row) do
          validate_config_json(config_json, metadata_identity(row))
        end

      _ ->
        {:error, ElixirDB.Error.unsupported_format("SQLite database metadata is invalid")}
    end
  end

  defp validate_metadata_scalars(row) do
    [
      uuid,
      history_epoch,
      format,
      schema,
      revision,
      canonical,
      protocol,
      sequence,
      retention_floor,
      compaction_epoch,
      boundary_digest,
      _config_json
    ] = row

    validators = [
      fn -> validate_metadata_uuid(uuid) end,
      fn -> validate_metadata_uuid(history_epoch) end,
      fn -> validate_metadata_versions(format, schema, revision, canonical, protocol) end,
      fn -> validate_metadata_sequence(sequence) end,
      fn -> validate_metadata_retention_floor(retention_floor) end,
      fn -> validate_metadata_compaction_epoch(compaction_epoch) end,
      fn -> validate_metadata_boundary_digest(boundary_digest) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_metadata_uuid(value) do
    if valid_uuid?(value), do: nil, else: metadata_invalid_error()
  end

  defp validate_metadata_versions(format, schema, revision, canonical, protocol) do
    if format == 1 and schema == 1 and revision == 1 and canonical == 1 and protocol == 1,
      do: nil,
      else: metadata_invalid_error()
  end

  defp validate_metadata_sequence(sequence) do
    if is_integer(sequence) and sequence >= 0, do: nil, else: metadata_invalid_error()
  end

  defp validate_metadata_retention_floor(retention_floor) do
    if is_integer(retention_floor) and retention_floor >= 0, do: nil, else: metadata_invalid_error()
  end

  defp validate_metadata_compaction_epoch(compaction_epoch) do
    if is_integer(compaction_epoch) and compaction_epoch >= 0,
      do: nil,
      else: metadata_invalid_error()
  end

  defp validate_metadata_boundary_digest(boundary_digest) do
    if is_nil(boundary_digest) or is_binary(boundary_digest),
      do: nil,
      else: metadata_invalid_error()
  end

  defp metadata_invalid_error,
    do: {:error, ElixirDB.Error.unsupported_format("SQLite database metadata is invalid")}

  defp metadata_identity([
         uuid,
         history_epoch,
         format,
         schema,
         revision,
         canonical,
         protocol,
         sequence,
         retention_floor,
         compaction_epoch,
         boundary_digest,
         config_json
       ]) do
    %{
      database_uuid: uuid,
      history_epoch: history_epoch,
      file_format_version: format,
      logical_schema_version: schema,
      revision_algorithm_version: revision,
      canonicalization_version: canonical,
      replication_protocol_major: protocol,
      current_sequence: sequence,
      retention_floor_sequence: retention_floor,
      compaction_epoch: compaction_epoch,
      retention_boundary_digest: boundary_digest,
      config_json: config_json
    }
  end

  defp validate_config_json(config_json, identity) do
    case StrictDecoder.decode(config_json) do
      {:ok, config} when is_map(config) -> validate_database_config(config, identity)
      _ -> {:error, ElixirDB.Error.unsupported_format("SQLite database configuration is invalid")}
    end
  end

  defp validate_database_config(config, identity) do
    case ElixirDB.Config.validate(config) do
      {:ok, _} ->
        {:ok, identity}

      {:error, error} ->
        {:error,
         ElixirDB.Error.unsupported_format("SQLite database configuration is invalid", %{
           cause: error.code
         })}
    end
  end

  defp valid_uuid?(uuid) when is_binary(uuid) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      uuid
    )
  end

  defp valid_uuid?(_), do: false

  defp journal_mode_sql(:disk), do: "PRAGMA journal_mode = DELETE"
  defp journal_mode_sql(:memory), do: "PRAGMA journal_mode = MEMORY"

  defp synchronous_sql(:disk), do: "PRAGMA synchronous = EXTRA"
  defp synchronous_sql(:memory), do: "PRAGMA synchronous = NORMAL"

  defp valid_storage_pragmas?(:disk, journal_mode, synchronous, locking_mode, trusted_schema) do
    String.downcase(to_string(journal_mode)) == "delete" and synchronous in [3, "3"] and
      String.downcase(to_string(locking_mode)) == "normal" and trusted_schema in [0, "0"]
  end

  defp valid_storage_pragmas?(:memory, journal_mode, synchronous, locking_mode, trusted_schema) do
    String.downcase(to_string(journal_mode)) == "memory" and synchronous in [1, "1"] and
      String.downcase(to_string(locking_mode)) == "normal" and trusted_schema in [0, "0"]
  end

  defp valid_storage_pragmas?(
         _storage_mode,
         _journal_mode,
         _synchronous,
         _locking_mode,
         _trusted_schema
       ),
       do: false

  defp execute_script(conn, script) do
    script
    |> String.split(";", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while(:ok, fn statement, :ok ->
      case Connection.execute(conn, statement) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
