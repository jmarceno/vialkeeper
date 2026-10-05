defmodule VialKeeper.Storage.SQLite.Schema do
  @moduledoc "Creates and validates the fixed Version 1 SQLite schema and metadata."
  alias VialKeeper.DerivedView.Engine
  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Shadow.Metadata
  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.UUID

  @application_id 0x45584442
  @sqlite_magic "SQLite format 3\0"
  @required_tables ~w(
    db_meta documents revisions changes local_records replication_jobs
    index_definitions revision_attachments pending_blobs view_definitions
    view_state view_rows
  )
  @type storage_mode :: :disk | :memory

  @spec configure(Connection.handle(), keyword()) :: :ok | {:error, term()}
  def configure(conn, opts \\ []) do
    Connection.driver(conn).configure(conn, Keyword.get(opts, :storage_mode, :disk))
  end

  @doc """
  Configures a readonly snapshot connection.

  Does not set the journal mode; the writer connection already owns it. Sets
  query_only where the engine supports it so the handle cannot mutate the file.
  """
  @spec configure_reader(Connection.handle()) :: :ok | {:error, term()}
  def configure_reader(conn), do: Connection.driver(conn).configure_reader(conn)

  @spec required_tables_present(Connection.handle()) :: :ok | {:error, :missing_tables}
  def required_tables_present(conn) do
    case Connection.query(conn, "SELECT name FROM sqlite_master WHERE type = 'table'") do
      {:ok, rows} ->
        present = MapSet.new(rows, &List.first/1)

        if Enum.all?(@required_tables, &MapSet.member?(present, &1)),
          do: :ok,
          else: {:error, :missing_tables}

      {:error, _reason} ->
        {:error, :missing_tables}
    end
  end

  @doc """
  Rejects empty or non-SQLite artifacts without opening the engine.

  Version fields live in page 1 and may exist only in WAL until checkpoint, so
  `application_id` / `user_version` are confirmed after open via `recognize_v1/1`.
  """
  @spec recognize_artifact(binary()) :: :ok | {:error, Error.t()}
  def recognize_artifact(path) when is_binary(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} when size >= 16 ->
        recognize_sqlite_magic(path)

      {:ok, %{type: :regular}} ->
        {:error, unrecognized_v1_error()}

      {:ok, _stat} ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite schema validation failed", %{cause: inspect(reason)})}
    end
  end

  @doc """
  Confirms Version 1 identity, metadata, and required tables on an open connection.

  Does not set persistent pragmas. Call this before `configure/2` on open so a
  foreign or future-format file is not rewritten to WAL.
  """
  @spec recognize_v1(Connection.handle()) :: :ok | {:error, Error.t()}
  def recognize_v1(conn) do
    with {:ok, [[application_id]]} <- Connection.pragma(conn, "application_id"),
         {:ok, [[user_version]]} <- Connection.pragma(conn, "user_version"),
         :ok <- recognize_header_versions(application_id, user_version),
         :ok <- required_tables_as_format(conn),
         {:ok, [row]} <-
           Connection.query(
             conn,
             "SELECT file_format_version, logical_schema_version, revision_algorithm_version, canonicalization_version, replication_protocol_major FROM db_meta WHERE id = 1"
           ),
         :ok <- recognize_version_row(row),
         :ok <- recognize_engine(conn) do
      :ok
    else
      {:ok, []} ->
        {:error, Error.unsupported_format("SQLite file has no VialKeeper metadata")}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite schema validation failed", %{cause: inspect(reason)})}
    end
  end

  @spec read_database_uuid(Connection.handle()) ::
          {:ok, binary()} | {:error, Error.t()}
  def read_database_uuid(conn) do
    case Connection.query(conn, "SELECT database_uuid FROM db_meta WHERE id = 1") do
      {:ok, [[uuid]]} when is_binary(uuid) ->
        {:ok, uuid}

      {:ok, _} ->
        {:error, Error.unsupported_format("SQLite file has no VialKeeper metadata")}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite schema validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec create(Connection.handle(), binary(), binary(), keyword()) ::
          :ok | {:error, Error.t()}
  def create(conn, database_uuid, config_json, opts \\ []) do
    schema =
      File.read!(Path.join([Application.app_dir(:vial_keeper), "priv", "sqlite", "schema_v1.sql"]))

    with {:ok, database_kind} <-
           VialKeeper.DatabaseKind.normalize(Keyword.get(opts, :database_kind, :ordinary)),
         :ok <- begin_initialization(conn),
         :ok <- execute_script(conn, schema),
         :ok <- insert_metadata(conn, database_uuid, database_kind, config_json),
         :ok <- initialize_derived(conn, database_kind, Keyword.get(opts, :initial_derived_view)),
         :ok <- initialize_shadow(conn, database_kind, Keyword.get(opts, :shadow_metadata)),
         :ok <- commit_initialization(conn) do
      :ok
    else
      {:error, reason} ->
        _ = rollback_initialization(conn)

        {:error,
         Error.internal_error("could not initialize SQLite schema", %{
           cause: inspect(reason)
         })}
    end
  end

  @spec validate(Connection.handle(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def validate(conn, opts \\ []) do
    storage_mode = Keyword.get(opts, :storage_mode, :disk)

    with {:ok, [[application_id]]} <- Connection.pragma(conn, "application_id"),
         {:ok, [[user_version]]} <- Connection.pragma(conn, "user_version"),
         :ok <- recognize_engine(conn),
         {:ok, [meta]} <-
           Connection.query(
             conn,
             "SELECT database_uuid, database_kind, history_epoch, file_format_version, logical_schema_version, revision_algorithm_version, canonicalization_version, replication_protocol_major, sequence_reserved_through, retention_floor_sequence, compaction_epoch, retention_boundary_digest, config_json FROM db_meta WHERE id = 1"
           ) do
      with {:ok, identity} <-
             validate_schema_metadata(
               application_id,
               user_version,
               Connection.driver(conn).valid_pragmas?(conn, storage_mode),
               meta
             ),
           :ok <- validate_kind_state(conn, identity) do
        {:ok, identity}
      end
    else
      {:ok, []} ->
        {:error, Error.unsupported_format("SQLite file has no VialKeeper metadata")}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite schema validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate_schema_metadata(@application_id, 1, true, meta), do: validate_metadata_row(meta)

  defp validate_schema_metadata(_application_id, _user_version, _valid_pragmas?, _meta),
    do: {:error, Error.unsupported_format("SQLite file is not a Version 1 VialKeeper database")}

  defp validate_metadata_row(row) do
    case row do
      [
        _uuid,
        _database_kind,
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
        with :ok <- validate_metadata_scalars(row),
             {:ok, _} <- VialKeeper.DatabaseKind.from_storage(Enum.at(row, 1)) do
          validate_config_json(config_json, metadata_identity(row))
        end

      _ ->
        {:error, Error.unsupported_format("SQLite database metadata is invalid")}
    end
  end

  defp validate_metadata_scalars(row) do
    [
      uuid,
      database_kind,
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
      fn -> validate_metadata_kind(database_kind) end,
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

  defp validate_metadata_kind(value) do
    if value in ["ordinary", "derived", "shadow"], do: nil, else: metadata_invalid_error()
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
    do: {:error, Error.unsupported_format("SQLite database metadata is invalid")}

  defp metadata_identity([
         uuid,
         database_kind,
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
    {:ok, database_kind} = VialKeeper.DatabaseKind.from_storage(database_kind)

    %{
      database_uuid: uuid,
      database_kind: database_kind,
      history_epoch: history_epoch,
      file_format_version: format,
      logical_schema_version: schema,
      revision_algorithm_version: revision,
      canonicalization_version: canonical,
      replication_protocol_major: protocol,
      sequence_reserved_through: sequence,
      retention_floor_sequence: retention_floor,
      compaction_epoch: compaction_epoch,
      retention_boundary_digest: boundary_digest,
      config_json: config_json
    }
  end

  defp validate_config_json(config_json, identity) do
    case StrictDecoder.decode(config_json) do
      {:ok, config} when is_map(config) -> validate_database_config(config, identity)
      _ -> {:error, Error.unsupported_format("SQLite database configuration is invalid")}
    end
  end

  defp validate_database_config(config, identity) do
    case VialKeeper.Config.validate(config) do
      {:ok, _} ->
        {:ok, identity}

      {:error, error} ->
        {:error,
         Error.unsupported_format("SQLite database configuration is invalid", %{
           cause: error.code
         })}
    end
  end

  defp validate_kind_state(conn, %{database_kind: :ordinary}) do
    case Connection.query(conn, "SELECT COUNT(*) FROM derived_view") do
      {:ok, [[0]]} ->
        :ok

      {:ok, [[count]]} when is_integer(count) ->
        {:error, Error.integrity_violation("ordinary database contains derived metadata")}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite derived metadata validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate_kind_state(conn, %{database_kind: :derived}) do
    with {:ok, [[1]]} <- Connection.query(conn, "SELECT COUNT(*) FROM derived_view"),
         {:ok, [[source_count]]} <- Connection.query(conn, "SELECT COUNT(*) FROM derived_sources"),
         true <- is_integer(source_count) and source_count > 0 do
      :ok
    else
      false ->
        {:error, Error.unsupported_format("derived database has no source metadata")}

      {:ok, [[count]]} ->
        {:error,
         Error.unsupported_format("derived database metadata is incomplete", %{
           count: count
         })}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite derived metadata validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate_kind_state(conn, %{database_kind: :shadow, database_uuid: database_uuid}) do
    case Connection.query(
           conn,
           "SELECT source_database_uuid, shadow_database_uuid, generation, operation_id, attachment_store_type, attachment_location, specification_digest, created_at FROM shadow_metadata WHERE id = 1"
         ) do
      {:ok, [row]} ->
        validate_shadow_record(row, database_uuid)

      {:ok, []} ->
        {:error, Error.unsupported_format("shadow database metadata is incomplete")}

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite shadow metadata validation failed", %{
           cause: inspect(reason)
         })}
    end
  end

  defp validate_shadow_record(
         [
           source_uuid,
           shadow_uuid,
           generation,
           operation_id,
           store_type,
           location,
           digest,
           created_at
         ],
         database_uuid
       ) do
    metadata =
      Metadata.new(
        source_uuid,
        shadow_uuid,
        generation,
        operation_id,
        store_type,
        location,
        digest,
        created_at
      )

    with :ok <- validate_shadow_identity(source_uuid, shadow_uuid),
         true <- shadow_uuid == database_uuid,
         {:ok, _} <- fetch_shadow_uuid(metadata, :source_database_uuid),
         {:ok, _} <- fetch_shadow_uuid(metadata, :shadow_database_uuid),
         {:ok, _} <- fetch_shadow_generation(metadata),
         {:ok, _} <- fetch_shadow_uuid(metadata, :operation_id),
         {:ok, _} <- fetch_shadow_store_type(metadata),
         {:ok, _} <- fetch_shadow_path(metadata),
         {:ok, _} <- fetch_shadow_digest(metadata),
         {:ok, _} <- fetch_shadow_created_at(metadata) do
      :ok
    else
      false ->
        {:error,
         Error.shadow_identity_conflict("shadow metadata UUID does not match database identity")}

      {:error, _} ->
        {:error, Error.unsupported_format("shadow database metadata is invalid")}
    end
  end

  defp begin_initialization(conn), do: Connection.execute(conn, "BEGIN IMMEDIATE")

  defp commit_initialization(conn), do: Connection.execute(conn, "COMMIT")

  defp rollback_initialization(conn), do: Connection.execute(conn, "ROLLBACK")

  defp insert_metadata(conn, database_uuid, database_kind, config_json) do
    Connection.execute(
      conn,
      "INSERT INTO db_meta (id, database_uuid, database_kind, history_epoch, file_format_version, logical_schema_version, revision_algorithm_version, canonicalization_version, replication_protocol_major, sequence_reserved_through, retention_floor_sequence, compaction_epoch, retention_boundary_digest, created_at, config_json, storage_engine) VALUES (1, ?, ?, ?, 1, 1, 1, 1, 1, 0, 0, 0, NULL, ?, ?, ?)",
      [
        database_uuid,
        VialKeeper.DatabaseKind.storage(database_kind),
        UUID.v4(),
        DateTime.utc_now() |> DateTime.to_iso8601(),
        config_json,
        Connection.driver(conn).engine()
      ]
    )
  end

  defp initialize_derived(_conn, :ordinary, nil), do: :ok

  defp initialize_derived(_conn, :ordinary, _initial),
    do: {:error, Error.invalid_request("ordinary database cannot include derived metadata")}

  defp initialize_derived(conn, :derived, initial) when is_map(initial) do
    with {:ok, name} <- fetch_binary(initial, :name),
         {:ok, definition_json} <- fetch_binary(initial, :definition_json),
         {:ok, definition_digest} <- fetch_binary(initial, :definition_digest),
         {:ok, options_json} <- fetch_binary(initial, :options_json),
         {:ok, materialization_id} <- fetch_binary(initial, :materialization_id),
         {:ok, enabled} <- fetch_boolean(initial, :enabled),
         {:ok, status} <- fetch_status(initial),
         sources when is_list(sources) <-
           Map.get(initial, :sources, Map.get(initial, "sources")),
         :ok <-
           insert_derived_view(
             conn,
             materialization_id,
             name,
             definition_json,
             definition_digest,
             enabled,
             status,
             options_json
           ),
         :ok <- insert_derived_sources(conn, sources) do
      :ok
    else
      nil ->
        {:error, Error.invalid_request("derived source metadata is required")}

      {:error, _} = error ->
        error

      _ ->
        {:error, Error.invalid_request("derived metadata is invalid")}
    end
  end

  defp initialize_derived(_conn, :derived, _),
    do: {:error, Error.invalid_request("derived metadata is required")}

  defp initialize_derived(_conn, :shadow, nil), do: :ok

  defp initialize_derived(_conn, :shadow, _initial),
    do: {:error, Error.invalid_request("shadow database cannot include derived metadata")}

  defp initialize_shadow(_conn, :ordinary, nil), do: :ok

  defp initialize_shadow(_conn, :ordinary, _metadata),
    do: {:error, Error.invalid_request("ordinary database cannot include shadow metadata")}

  defp initialize_shadow(_conn, :derived, nil), do: :ok

  defp initialize_shadow(_conn, :derived, _metadata),
    do: {:error, Error.invalid_request("derived database cannot include shadow metadata")}

  defp initialize_shadow(conn, :shadow, metadata) when is_map(metadata) do
    with {:ok, source_uuid} <- fetch_shadow_uuid(metadata, :source_database_uuid),
         {:ok, shadow_uuid} <- fetch_shadow_uuid(metadata, :shadow_database_uuid),
         {:ok, generation} <- fetch_shadow_generation(metadata),
         {:ok, operation_id} <- fetch_shadow_uuid(metadata, :operation_id),
         {:ok, attachment_store_type} <- fetch_shadow_store_type(metadata),
         {:ok, attachment_location} <- fetch_shadow_path(metadata),
         {:ok, specification_digest} <- fetch_shadow_digest(metadata),
         {:ok, created_at} <- fetch_shadow_created_at(metadata),
         :ok <- validate_shadow_identity(source_uuid, shadow_uuid) do
      Connection.execute(
        conn,
        "INSERT INTO shadow_metadata (id, source_database_uuid, shadow_database_uuid, generation, operation_id, attachment_store_type, attachment_location, specification_digest, created_at) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
          source_uuid,
          shadow_uuid,
          generation,
          operation_id,
          attachment_store_type,
          attachment_location,
          specification_digest,
          created_at
        ]
      )
    end
  end

  defp initialize_shadow(_conn, :shadow, _metadata),
    do: {:error, Error.invalid_request("shadow database metadata is required")}

  defp fetch_shadow_uuid(metadata, key) do
    value = Map.get(metadata, key, Map.get(metadata, Atom.to_string(key)))

    if is_binary(value) and valid_uuid?(value),
      do: {:ok, value},
      else: {:error, Error.invalid_request("shadow metadata UUID is invalid")}
  end

  defp fetch_shadow_generation(metadata) do
    value = Map.get(metadata, :generation, Map.get(metadata, "generation"))

    if is_integer(value) and value > 0,
      do: {:ok, value},
      else: {:error, Error.invalid_request("shadow metadata generation is invalid")}
  end

  defp fetch_shadow_path(metadata) do
    value = Map.get(metadata, :attachment_location, Map.get(metadata, "attachment_location"))

    if is_binary(value) and Path.type(value) == :absolute and value != "",
      do: {:ok, value},
      else: {:error, Error.invalid_request("shadow attachment location must be absolute")}
  end

  defp fetch_shadow_store_type(metadata) do
    value = Map.get(metadata, :attachment_store_type, Map.get(metadata, "attachment_store_type"))

    if value == "external_cas",
      do: {:ok, value},
      else: {:error, Error.invalid_request("shadow attachment store type is invalid")}
  end

  defp fetch_shadow_digest(metadata) do
    value = Map.get(metadata, :specification_digest, Map.get(metadata, "specification_digest"))

    if is_binary(value) and value != "",
      do: {:ok, value},
      else: {:error, Error.invalid_request("shadow specification digest is invalid")}
  end

  defp fetch_shadow_created_at(metadata) do
    value = Map.get(metadata, :created_at, Map.get(metadata, "created_at"))

    case DateTime.from_iso8601(value || "") do
      {:ok, _datetime, 0} -> {:ok, value}
      _ -> {:error, Error.invalid_request("shadow creation time is invalid")}
    end
  end

  defp validate_shadow_identity(source_uuid, shadow_uuid) do
    if source_uuid == shadow_uuid,
      do: {:error, Error.shadow_identity_conflict("shadow and source UUIDs must differ")},
      else: :ok
  end

  defp insert_derived_view(
         conn,
         materialization_id,
         name,
         definition_json,
         definition_digest,
         enabled,
         status,
         options_json
       ) do
    Connection.execute(
      conn,
      "INSERT INTO derived_view (id, materialization_id, name, definition_json, definition_digest, enabled, status, options_json, last_error_code) VALUES (1, ?, ?, ?, ?, ?, ?, ?, NULL)",
      [
        materialization_id,
        name,
        definition_json,
        definition_digest,
        bool_to_integer(enabled),
        status,
        options_json
      ]
    )
  end

  defp insert_derived_sources(conn, sources) do
    sources
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {source, ordinal}, :ok ->
      case insert_derived_source(conn, source, ordinal) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp insert_derived_source(conn, source, ordinal) do
    case Engine.source_uuid(source) do
      uuid when is_binary(uuid) ->
        Connection.execute(
          conn,
          "INSERT INTO derived_sources (source_ordinal, source_database_uuid, source_history_epoch, checkpoint_sequence, state, rebuild_generation, rebuild_start_sequence, rebuild_after_document_id, rebuild_catchup_sequence, last_error_code) VALUES (?, ?, NULL, 0, 'pending', 0, NULL, NULL, NULL, NULL)",
          [ordinal, uuid]
        )

      _ ->
        {:error, Error.invalid_request("derived source metadata is invalid")}
    end
  end

  defp fetch_binary(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_binary(value) and value != "" do
      {:ok, value}
    else
      {:error, Error.invalid_request("derived metadata field is invalid")}
    end
  end

  defp fetch_boolean(map, key) do
    value = Map.get(map, key, Map.get(map, Atom.to_string(key)))

    if is_boolean(value) do
      {:ok, value}
    else
      {:error, Error.invalid_request("derived metadata enabled flag is invalid")}
    end
  end

  defp fetch_status(map) do
    value = Map.get(map, :status, Map.get(map, "status"))

    if is_atom(value), do: {:ok, Atom.to_string(value)}, else: fetch_binary(map, :status)
  end

  defp bool_to_integer(true), do: 1
  defp bool_to_integer(false), do: 0

  defp valid_uuid?(uuid) when is_binary(uuid) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      uuid
    )
  end

  defp valid_uuid?(_), do: false

  # Each engine owns its bundles: a file created by the other engine is
  # rejected, never converted.
  defp recognize_engine(conn) do
    expected = Connection.driver(conn).engine()

    case Connection.query(conn, "SELECT storage_engine FROM db_meta WHERE id = 1") do
      {:ok, [[^expected]]} -> :ok
      {:ok, [[engine]]} when is_binary(engine) -> {:error, foreign_engine_error(engine)}
      {:ok, _} -> {:error, unrecognized_v1_error()}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "The error for a bundle that another storage engine created."
  @spec foreign_engine_error(binary()) :: Error.t()
  def foreign_engine_error(engine),
    do: Error.unsupported_format("bundle was created by the #{engine} storage engine")

  defp required_tables_as_format(conn) do
    case required_tables_present(conn) do
      :ok ->
        :ok

      {:error, :missing_tables} ->
        {:error, unrecognized_v1_error()}
    end
  end

  defp recognize_version_row([1, 1, 1, 1, 1]), do: :ok
  defp recognize_version_row(_row), do: {:error, unrecognized_v1_error()}

  defp recognize_header_versions(@application_id, 1), do: :ok

  defp recognize_header_versions(_application_id, _user_version),
    do: {:error, unrecognized_v1_error()}

  defp recognize_sqlite_magic(path) do
    case File.open(path, [:read, :raw, :binary]) do
      {:ok, io} ->
        magic =
          try do
            IO.binread(io, 16)
          after
            File.close(io)
          end

        if magic == @sqlite_magic do
          :ok
        else
          {:error, unrecognized_v1_error()}
        end

      {:error, reason} ->
        {:error,
         Error.unsupported_format("SQLite schema validation failed", %{cause: inspect(reason)})}
    end
  end

  defp unrecognized_v1_error,
    do: Error.unsupported_format("SQLite file is not a Version 1 VialKeeper database")

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
