defmodule ElixirDB.Storage.SQLite.RetentionRecords do
  @moduledoc """
  Local-record namespaces for compact-retention metadata.

  ## Namespaces

  * `"peer_ledger"` — key is `peer_database_uuid`, value is a `PeerPosition` wire map.
  * `"retention_boundaries"` — key is `source_database_uuid\\0document_id\\0history_id`
    (NUL-separated so document IDs may contain `:` or `/`). Value wraps `boundary`
    (wire map), `source_database_uuid`, `source_history_epoch`, and `compaction_epoch`.
  * `"retention_boundary_state"` — the last complete boundary set installed for a
    source, used to detect same-epoch digest changes during incremental replication.
  * `"retention_boundary_staging"` — resumable, non-authoritative boundary pages;
    staged rows become visible only after the complete-set digest verifies.
  * `"retention_maintenance"` — key `"counter"` holds the maintenance counter;
    key `"last_result"` holds the most recent compaction stats map.
  * `"replication_state"` — key `"pending_local_causal"` tracks unacknowledged local
    mutations per peer (`pending_any` plus optional per-peer ack flags).
  """

  alias ElixirDB.Domain.{BoundaryPage, PeerPosition, RetentionBoundary}
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Retention.Service, as: RetentionService
  alias ElixirDB.Storage.SQLite.Connection

  @peer_ledger "peer_ledger"
  @retention_boundaries "retention_boundaries"
  @boundary_state "retention_boundary_state"
  @boundary_staging "retention_boundary_staging"
  @retention_maintenance "retention_maintenance"
  @replication_state "replication_state"

  # NUL cannot appear in UTF-8 document IDs or UUID text.
  @boundary_key_sep <<0>>

  @spec boundary_key(binary(), binary(), binary()) :: binary()
  def boundary_key(source_database_uuid, document_id, history_id),
    do: BoundaryPage.record_key(source_database_uuid, document_id, history_id)

  @spec parse_boundary_key(binary()) :: {binary(), binary(), binary()} | :error
  def parse_boundary_key(key) when is_binary(key), do: BoundaryPage.parse_record_key(key)

  @spec list_by_namespace(Connection.handle(), binary()) ::
          {:ok, [%{key: binary(), version: non_neg_integer(), value: map()}]}
          | {:error, ElixirDB.Error.t()}
  def list_by_namespace(conn, namespace) when is_binary(namespace) do
    case Connection.query(
           conn,
           "SELECT record_key, record_version, value_json FROM local_records WHERE namespace = ? ORDER BY record_key",
           [namespace]
         ) do
      {:ok, rows} ->
        decode_namespace_rows(rows)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_namespace_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [key, version, json], {:ok, acc} ->
      decode_namespace_row(key, version, json, acc)
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      error -> error
    end
  end

  defp decode_namespace_row(key, version, json, acc) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:cont, {:ok, [%{key: key, version: version, value: value} | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  @spec list_peers(Connection.handle()) ::
          {:ok, [PeerPosition.t()]} | {:error, ElixirDB.Error.t()}
  def list_peers(conn) do
    with {:ok, rows} <- list_by_namespace(conn, @peer_ledger) do
      decode_peer_rows(rows)
    end
  end

  defp decode_peer_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn %{value: value}, {:ok, acc} ->
      decode_peer_row(value, acc)
    end)
    |> case do
      {:ok, peers} -> {:ok, Enum.reverse(peers)}
      error -> error
    end
  end

  defp decode_peer_row(value, acc) do
    case PeerPosition.from_wire(value) do
      {:ok, peer} -> {:cont, {:ok, [peer | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  @type stored_boundary :: %{
          source_database_uuid: binary(),
          source_history_epoch: binary(),
          boundary: RetentionBoundary.t(),
          compaction_epoch: non_neg_integer()
        }

  @type boundary_install_state :: %{
          required(:source_database_uuid) => binary(),
          required(:source_history_epoch) => binary(),
          required(:compaction_epoch) => non_neg_integer(),
          required(:boundary_digest) => binary()
        }

  @spec list_boundaries(Connection.handle(), keyword()) ::
          {:ok, [stored_boundary()]} | {:error, ElixirDB.Error.t()}
  def list_boundaries(conn, opts \\ []) do
    with {:ok, rows} <- list_by_namespace(conn, @retention_boundaries) do
      decode_boundary_rows(rows, Keyword.get(opts, :source_database_uuid))
    end
  end

  defp decode_boundary_rows(rows, source_filter) do
    Enum.reduce_while(rows, {:ok, []}, fn %{key: key, value: value}, {:ok, acc} ->
      decode_boundary_row(key, value, source_filter, acc)
    end)
    |> case do
      {:ok, boundaries} -> {:ok, Enum.reverse(boundaries)}
      error -> error
    end
  end

  defp decode_boundary_row(_key, value, source_filter, acc) do
    with {:ok, boundary} <- boundary_from_value(value),
         source_uuid <- Map.get(value, "source_database_uuid"),
         true <-
           is_binary(source_uuid) and source_uuid != "" and
             source_filter in [nil, source_uuid],
         source_epoch <- Map.get(value, "source_history_epoch", ""),
         true <- is_binary(source_epoch) and source_epoch != "",
         epoch <- Map.get(value, "compaction_epoch", 0),
         true <- is_integer(epoch) and epoch >= 0 do
      {:cont,
       {:ok,
        [
          stored_boundary(source_uuid, source_epoch, boundary, epoch)
          | acc
        ]}}
    else
      {:error, error} ->
        {:halt, {:error, error}}

      false ->
        {:cont, {:ok, acc}}
    end
  end

  defp boundary_from_value(%{"boundary" => value}) do
    RetentionBoundary.from_wire(value)
  end

  defp boundary_from_value(_),
    do: {:error, ElixirDB.Error.integrity_violation("retention boundary record is invalid")}

  @spec encode_boundary(
          RetentionBoundary.t(),
          binary(),
          binary(),
          non_neg_integer()
        ) :: map()
  def encode_boundary(
        %RetentionBoundary{} = boundary,
        source_database_uuid,
        source_history_epoch,
        compaction_epoch
      ) do
    %{
      "source_database_uuid" => source_database_uuid,
      "source_history_epoch" => source_history_epoch,
      "boundary" => %{
        "document_id" => boundary.document_id,
        "history_id" => boundary.history_id,
        "minimum_retained_generation" => boundary.minimum_retained_generation,
        "retired" => boundary.retired,
        "retired_branch_roots" => boundary.retired_branch_roots
      },
      "compaction_epoch" => compaction_epoch
    }
  end

  @spec encode_stored_boundary(stored_boundary()) :: map()
  def encode_stored_boundary(%{
        source_database_uuid: source_uuid,
        source_history_epoch: source_epoch,
        boundary: %RetentionBoundary{} = boundary,
        compaction_epoch: compaction_epoch
      }) do
    encode_boundary(boundary, source_uuid, source_epoch, compaction_epoch)
  end

  @spec boundary_install_state(Connection.handle(), binary()) ::
          {:ok, boundary_install_state() | nil} | {:error, ElixirDB.Error.t()}
  def boundary_install_state(conn, source_uuid) when is_binary(source_uuid) do
    case fetch_local_record(conn, @boundary_state, source_uuid) do
      {:ok, nil} -> derive_boundary_install_state(conn, source_uuid)
      {:ok, %{value: value}} -> decode_boundary_install_state(value, source_uuid)
      {:error, error} -> {:error, error}
    end
  end

  @spec put_boundary_install_state(Connection.handle(), boundary_install_state()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def put_boundary_install_state(conn, state) when is_map(state) do
    source_uuid = Map.fetch!(state, :source_database_uuid)

    put_local_record(
      conn,
      @boundary_state,
      source_uuid,
      Map.new([
        {"source_database_uuid", source_uuid},
        {"source_history_epoch", Map.fetch!(state, :source_history_epoch)},
        {"compaction_epoch", Map.fetch!(state, :compaction_epoch)},
        {"boundary_digest", Map.fetch!(state, :boundary_digest)}
      ])
    )
  end

  @doc "Stages one complete boundary transfer without changing the authoritative set."
  @spec begin_boundary_install(Connection.handle(), binary(), boundary_install_state()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def begin_boundary_install(conn, install_id, state)
      when is_binary(install_id) and is_map(state) do
    with :ok <- delete_staging_install(conn, install_id),
         {:ok, json} <-
           Canonical.encode(%{
             "kind" => "metadata",
             "source_database_uuid" => Map.fetch!(state, :source_database_uuid),
             "source_history_epoch" => Map.fetch!(state, :source_history_epoch),
             "compaction_epoch" => Map.fetch!(state, :compaction_epoch),
             "boundary_digest" => Map.fetch!(state, :boundary_digest),
             "created_at_ms" => System.system_time(:millisecond)
           }) do
      put_staging_record(conn, staging_metadata_key(install_id), json)
    end
  rescue
    KeyError ->
      {:error, ElixirDB.Error.invalid_request("boundary install state is incomplete")}
  end

  @spec stage_boundary_page(Connection.handle(), binary(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def stage_boundary_page(conn, install_id, page) when is_binary(install_id) and is_map(page) do
    with {:ok, metadata} <- staging_metadata(conn, install_id),
         :ok <- RetentionService.same_boundary_install?(metadata, page) do
      Enum.reduce_while(page.boundaries, :ok, fn boundary, :ok ->
        stage_boundary(conn, install_id, metadata, boundary)
      end)
    end
  end

  defp stage_boundary(conn, install_id, metadata, boundary) do
    stored =
      stored_boundary(
        metadata.source_database_uuid,
        metadata.source_history_epoch,
        boundary,
        metadata.compaction_epoch
      )

    key =
      staging_boundary_key(
        install_id,
        boundary_key(
          stored.source_database_uuid,
          boundary.document_id,
          boundary.history_id
        )
      )

    with {:ok, json} <- Canonical.encode(encode_stored_boundary(stored)),
         :ok <- put_staging_record(conn, key, json) do
      {:cont, :ok}
    else
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  @spec complete_boundary_install(Connection.handle(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def complete_boundary_install(conn, install_id) when is_binary(install_id) do
    with {:ok, metadata} <- staging_metadata(conn, install_id),
         {:ok, boundaries} <- staged_boundaries(conn, install_id),
         digest <- BoundaryPage.digest_for(boundaries),
         true <- digest == metadata.boundary_digest,
         :ok <- replace_authoritative_boundaries(conn, metadata, boundaries),
         :ok <- put_boundary_install_state(conn, metadata),
         :ok <- delete_staging_install(conn, install_id) do
      {:ok,
       %{
         installed: length(boundaries),
         boundary_digest: metadata.boundary_digest,
         compaction_epoch: metadata.compaction_epoch
       }}
    else
      false ->
        {:error, ElixirDB.Error.boundary_conflict("installed boundary digest mismatch")}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec replace_boundary_set(
          Connection.handle(),
          boundary_install_state(),
          [RetentionBoundary.t()]
        ) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def replace_boundary_set(conn, state, boundaries)
      when is_map(state) and is_list(boundaries) do
    with :ok <- validate_boundary_set_digest(state, boundaries),
         :ok <- replace_authoritative_boundaries(conn, state, boundaries),
         :ok <- put_boundary_install_state(conn, state) do
      {:ok,
       %{
         installed: length(boundaries),
         boundary_digest: Map.fetch!(state, :boundary_digest),
         compaction_epoch: Map.fetch!(state, :compaction_epoch)
       }}
    end
  rescue
    KeyError ->
      {:error, ElixirDB.Error.invalid_request("boundary install state is incomplete")}
  end

  defp validate_boundary_set_digest(state, boundaries) do
    expected = Map.get(state, :boundary_digest)

    if is_binary(expected) and expected == BoundaryPage.digest_for(boundaries) do
      :ok
    else
      {:error, ElixirDB.Error.boundary_conflict("installed boundary digest mismatch")}
    end
  end

  @spec install_imported_boundaries(Connection.handle(), [map()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def install_imported_boundaries(conn, boundaries) when is_list(boundaries) do
    Enum.reduce_while(boundaries, :ok, fn raw, :ok ->
      install_imported_boundary(conn, raw)
    end)
  end

  defp install_imported_boundary(conn, raw) do
    with {:ok, stored} <- normalize_stored_boundary(raw),
         :ok <- upsert_stored_boundary(conn, stored) do
      {:cont, :ok}
    else
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp derive_boundary_install_state(conn, source_uuid) do
    with {:ok, boundaries} <- list_boundaries(conn, source_database_uuid: source_uuid) do
      derive_boundary_install_state_from_boundaries(boundaries, source_uuid)
    end
  end

  defp derive_boundary_install_state_from_boundaries([], _source_uuid), do: {:ok, nil}

  defp derive_boundary_install_state_from_boundaries(
         [%{source_history_epoch: source_epoch} | _] = values,
         source_uuid
       ) do
    epochs = Enum.map(values, & &1.compaction_epoch)

    case Enum.uniq(Enum.map(values, & &1.source_history_epoch)) do
      [^source_epoch] ->
        {:ok,
         boundary_install_state(
           source_uuid,
           source_epoch,
           Enum.max(epochs),
           BoundaryPage.digest_for(Enum.map(values, & &1.boundary))
         )}

      _ ->
        {:error,
         ElixirDB.Error.integrity_violation(
           "installed boundaries contain multiple source history epochs"
         )}
    end
  end

  defp decode_boundary_install_state(
         %{
           "source_database_uuid" => source_uuid,
           "source_history_epoch" => source_epoch,
           "compaction_epoch" => compaction_epoch,
           "boundary_digest" => boundary_digest
         },
         expected_source_uuid
       )
       when source_uuid == expected_source_uuid and is_binary(source_epoch) and
              source_epoch != "" and is_integer(compaction_epoch) and compaction_epoch >= 0 and
              is_binary(boundary_digest) and boundary_digest != "" do
    {:ok, boundary_install_state(source_uuid, source_epoch, compaction_epoch, boundary_digest)}
  end

  defp decode_boundary_install_state(_, _),
    do: {:error, ElixirDB.Error.integrity_violation("boundary install state is invalid")}

  defp staging_metadata(conn, install_id) do
    case fetch_local_record(conn, @boundary_staging, staging_metadata_key(install_id)) do
      {:ok, nil} ->
        {:error, ElixirDB.Error.boundary_conflict("boundary install session is missing")}

      {:ok, %{value: value}} ->
        decode_staging_metadata(value)

      {:error, error} ->
        {:error, error}
    end
  end

  defp decode_staging_metadata(%{
         "kind" => "metadata",
         "source_database_uuid" => source_uuid,
         "source_history_epoch" => source_epoch,
         "compaction_epoch" => compaction_epoch,
         "boundary_digest" => digest
       }) do
    if is_binary(source_uuid) and source_uuid != "" and is_binary(source_epoch) and
         source_epoch != "" and is_integer(compaction_epoch) and compaction_epoch >= 0 and
         is_binary(digest) and digest != "" do
      {:ok, boundary_install_state(source_uuid, source_epoch, compaction_epoch, digest)}
    else
      {:error, ElixirDB.Error.integrity_violation("boundary install session is invalid")}
    end
  end

  defp decode_staging_metadata(_value),
    do: {:error, ElixirDB.Error.integrity_violation("boundary install session is invalid")}

  defp staged_boundaries(conn, install_id) do
    case list_by_namespace(conn, @boundary_staging) do
      {:ok, rows} ->
        prefix = install_id <> @boundary_key_sep

        rows
        |> Enum.filter(fn %{key: key} -> String.starts_with?(key, prefix) end)
        |> Enum.reject(fn %{key: key} -> key == staging_metadata_key(install_id) end)
        |> decode_staged_boundaries()

      {:error, error} ->
        {:error, error}
    end
  end

  defp decode_staged_boundaries(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn %{value: value}, {:ok, acc} ->
      case normalize_stored_boundary(value) do
        {:ok, stored} -> {:cont, {:ok, [stored | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.map(values, & &1.boundary)}
      error -> error
    end
  end

  defp replace_authoritative_boundaries(conn, state, boundaries) do
    source_uuid = Map.fetch!(state, :source_database_uuid)
    source_epoch = Map.fetch!(state, :source_history_epoch)
    compaction_epoch = Map.fetch!(state, :compaction_epoch)

    with :ok <- delete_source_boundaries(conn, source_uuid) do
      insert_authoritative_boundaries(conn, source_uuid, source_epoch, compaction_epoch, boundaries)
    end
  rescue
    KeyError -> {:error, ElixirDB.Error.invalid_request("boundary install state is incomplete")}
  end

  defp insert_authoritative_boundaries(
         conn,
         source_uuid,
         source_epoch,
         compaction_epoch,
         boundaries
       ) do
    Enum.reduce_while(boundaries, :ok, fn boundary, :ok ->
      stored = stored_boundary(source_uuid, source_epoch, boundary, compaction_epoch)

      case upsert_stored_boundary(conn, stored) do
        :ok -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp normalize_stored_boundary(%{
         "source_database_uuid" => source_uuid,
         "source_history_epoch" => source_epoch,
         "boundary" => boundary,
         "compaction_epoch" => compaction_epoch
       }) do
    with true <- is_binary(source_uuid) and source_uuid != "",
         true <- is_binary(source_epoch) and source_epoch != "",
         true <- is_integer(compaction_epoch) and compaction_epoch >= 0,
         {:ok, boundary} <- RetentionBoundary.from_wire(boundary) do
      {:ok, stored_boundary(source_uuid, source_epoch, boundary, compaction_epoch)}
    else
      false -> {:error, ElixirDB.Error.invalid_request("purged boundary metadata is invalid")}
      {:error, error} -> {:error, error}
    end
  end

  defp normalize_stored_boundary(%{source_database_uuid: source_uuid} = value)
       when is_binary(source_uuid) do
    {:ok, value}
  end

  defp normalize_stored_boundary(_),
    do: {:error, ElixirDB.Error.invalid_request("purged boundary metadata is invalid")}

  defp stored_boundary(source_uuid, source_epoch, boundary, compaction_epoch) do
    Map.new(
      source_database_uuid: source_uuid,
      source_history_epoch: source_epoch,
      boundary: boundary,
      compaction_epoch: compaction_epoch
    )
  end

  defp boundary_install_state(source_uuid, source_epoch, compaction_epoch, boundary_digest) do
    Map.new(
      source_database_uuid: source_uuid,
      source_history_epoch: source_epoch,
      compaction_epoch: compaction_epoch,
      boundary_digest: boundary_digest
    )
  end

  defp upsert_stored_boundary(conn, stored) do
    key =
      boundary_key(
        stored.source_database_uuid,
        stored.boundary.document_id,
        stored.boundary.history_id
      )

    case Canonical.encode(encode_stored_boundary(stored)) do
      {:ok, json} ->
        case Connection.execute(
               conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
               [@retention_boundaries, key, json]
             ) do
          :ok -> :ok
          {:error, reason} -> {:error, normalize_error(reason)}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp delete_source_boundaries(conn, source_uuid) do
    case Connection.query(
           conn,
           "SELECT record_key FROM local_records WHERE namespace = ? ORDER BY record_key",
           [@retention_boundaries]
         ) do
      {:ok, rows} ->
        keys =
          rows
          |> Enum.map(&List.first/1)
          |> Enum.filter(&String.starts_with?(&1, source_uuid <> @boundary_key_sep))

        delete_local_record_keys(conn, @retention_boundaries, keys)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp staging_metadata_key(install_id), do: install_id <> @boundary_key_sep <> "__metadata__"

  defp staging_boundary_key(install_id, boundary_key),
    do: install_id <> @boundary_key_sep <> boundary_key

  defp put_staging_record(conn, key, json) do
    case Connection.execute(
           conn,
           "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
           [@boundary_staging, key, json]
         ) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp delete_staging_install(conn, install_id) do
    case Connection.query(
           conn,
           "SELECT record_key FROM local_records WHERE namespace = ? ORDER BY record_key",
           [@boundary_staging]
         ) do
      {:ok, rows} ->
        prefix = install_id <> @boundary_key_sep

        keys =
          rows
          |> Enum.map(&List.first/1)
          |> Enum.filter(&String.starts_with?(&1, prefix))

        delete_local_record_keys(conn, @boundary_staging, keys)

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp delete_local_record_keys(_conn, _namespace, []), do: :ok

  defp delete_local_record_keys(conn, namespace, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case Connection.execute(
             conn,
             "DELETE FROM local_records WHERE namespace = ? AND record_key = ?",
             [namespace, key]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  @spec pending_local_causal?(Connection.handle()) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def pending_local_causal?(conn), do: pending_local_causal?(conn, nil)

  @spec pending_local_causal?(Connection.handle(), binary() | nil) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def pending_local_causal?(conn, peer_database_uuid)
      when is_nil(peer_database_uuid) or is_binary(peer_database_uuid) do
    case fetch_pending_local_causal(conn) do
      {:ok, state} -> {:ok, pending_for_peer?(state, peer_database_uuid)}
      {:error, error} -> {:error, error}
    end
  end

  @spec mark_pending_local_causal(Connection.handle()) :: :ok | {:error, ElixirDB.Error.t()}
  def mark_pending_local_causal(conn) do
    case fetch_pending_local_causal(conn) do
      {:ok, state} ->
        peers =
          state
          |> Map.get("peers", %{})
          |> Map.new(fn {peer, _} -> {peer, true} end)

        put_replication_state(conn, "pending_local_causal", %{
          "pending_any" => true,
          "peers" => peers
        })

      {:error, error} ->
        {:error, error}
    end
  end

  @spec set_pending_local_causal(Connection.handle(), boolean()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def set_pending_local_causal(conn, true), do: mark_pending_local_causal(conn)

  def set_pending_local_causal(conn, false) do
    put_replication_state(conn, "pending_local_causal", %{"pending_any" => false, "peers" => %{}})
  end

  @spec clear_pending_local_causal(Connection.handle(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def clear_pending_local_causal(conn, nil), do: set_pending_local_causal(conn, false)

  def clear_pending_local_causal(conn, peer_database_uuid)
      when is_binary(peer_database_uuid) and peer_database_uuid != "" do
    case fetch_pending_local_causal(conn) do
      {:ok, state} ->
        peers = Map.put(Map.get(state, "peers", %{}), peer_database_uuid, false)

        put_replication_state(conn, "pending_local_causal", %{
          "pending_any" => Map.get(state, "pending_any", false),
          "peers" => peers
        })

      {:error, error} ->
        {:error, error}
    end
  end

  defp fetch_pending_local_causal(conn) do
    case fetch_replication_state(conn, "pending_local_causal") do
      {:ok, nil} ->
        {:ok, %{"pending_any" => false, "peers" => %{}}}

      {:ok, %{value: %{"pending" => pending}}} when is_boolean(pending) ->
        {:ok, %{"pending_any" => pending, "peers" => %{}}}

      {:ok, %{value: %{"pending_any" => pending_any} = value}} when is_boolean(pending_any) ->
        peers = normalize_pending_peers(Map.get(value, "peers", %{}))
        {:ok, %{"pending_any" => pending_any, "peers" => peers}}

      {:ok, _} ->
        {:error, ElixirDB.Error.integrity_violation("pending local causal flag is invalid")}

      {:error, error} ->
        {:error, error}
    end
  end

  defp normalize_pending_peers(peers) when is_map(peers) do
    peers
    |> Enum.filter(fn {key, value} -> is_binary(key) and is_boolean(value) end)
    |> Map.new()
  end

  defp normalize_pending_peers(_), do: %{}

  defp pending_for_peer?(%{"pending_any" => pending_any, "peers" => peers}, nil) do
    pending_any or Enum.any?(peers, fn {_peer, pending} -> pending end)
  end

  defp pending_for_peer?(%{"pending_any" => pending_any, "peers" => peers}, peer_uuid) do
    case Map.fetch(peers, peer_uuid) do
      {:ok, pending} -> pending
      :error -> pending_any
    end
  end

  @spec maintenance_counter(Connection.handle()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def maintenance_counter(conn) do
    case fetch_maintenance(conn, "counter") do
      {:ok, nil} ->
        {:ok, 0}

      {:ok, %{value: %{"value" => value}}} when is_integer(value) and value >= 0 ->
        {:ok, value}

      {:ok, _} ->
        {:error, ElixirDB.Error.integrity_violation("retention maintenance counter is invalid")}

      {:error, error} ->
        {:error, error}
    end
  end

  @spec put_maintenance_counter(Connection.handle(), non_neg_integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def put_maintenance_counter(conn, counter) when is_integer(counter) and counter >= 0 do
    put_maintenance(conn, "counter", %{"value" => counter})
  end

  @spec put_last_result(Connection.handle(), map()) :: :ok | {:error, ElixirDB.Error.t()}
  def put_last_result(conn, result) when is_map(result) do
    put_maintenance(conn, "last_result", result)
  end

  @spec get_last_result(Connection.handle()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def get_last_result(conn) do
    case fetch_maintenance(conn, "last_result") do
      {:ok, nil} -> {:ok, nil}
      {:ok, %{value: value}} when is_map(value) -> {:ok, value}
      {:ok, _} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Applies a shared compaction effect to SQLite physical stores."
  @spec apply_compaction_effect(Connection.handle(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def apply_compaction_effect(conn, effect) when is_map(effect) do
    source_uuid = Map.fetch!(effect, :source_database_uuid)
    source_epoch = Map.fetch!(effect, :source_history_epoch)
    new_epoch = Map.fetch!(effect, :new_compaction_epoch)
    digest = Map.get(effect, :boundary_digest) || conn_digest(conn, source_uuid)

    with :ok <- persist_expired_peers(conn, Map.get(effect, :peers_to_expire, [])),
         :ok <-
           upsert_boundaries(
             conn,
             Map.get(effect, :boundaries_to_upsert, []),
             source_uuid,
             source_epoch,
             new_epoch
           ),
         :ok <-
           remove_boundaries(conn, Map.get(effect, :boundaries_to_remove, []), source_uuid),
         :ok <- delete_revisions(conn, Map.get(effect, :removals, %{})),
         :ok <- empty_documents(conn, Map.get(effect, :documents_to_empty, [])),
         :ok <- delete_changes(conn, Map.get(effect, :delete_changes_through, 0)),
         :ok <- update_meta(conn, Map.fetch!(effect, :new_floor), new_epoch, digest),
         :ok <- maybe_increment_maintenance(conn, Map.get(effect, :increment_maintenance?, false)) do
      put_last_result(conn, Map.get(effect, :result_stats, %{}))
    end
  end

  @doc "Updates a peer ledger wire value without compare-and-swap."
  @spec update_peer_wire(Connection.handle(), binary(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def update_peer_wire(conn, peer_uuid, wire)
      when is_binary(peer_uuid) and is_map(wire) do
    case Canonical.encode(wire) do
      {:ok, json} ->
        case Connection.execute(
               conn,
               "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET value_json = excluded.value_json",
               [@peer_ledger, peer_uuid, json]
             ) do
          :ok -> :ok
          {:error, reason} -> {:error, normalize_error(reason)}
        end

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  @spec peer_ledger_namespace() :: binary()
  def peer_ledger_namespace, do: @peer_ledger

  @spec retention_boundaries_namespace() :: binary()
  def retention_boundaries_namespace, do: @retention_boundaries

  defp persist_expired_peers(_conn, []), do: :ok

  defp persist_expired_peers(conn, peers) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      case update_peer_wire(conn, peer.peer_database_uuid, peer_wire(peer)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp peer_wire(peer) do
    %{
      "peer_database_uuid" => peer.peer_database_uuid,
      "peer_history_epoch" => peer.peer_history_epoch,
      "source_database_uuid" => peer.source_database_uuid,
      "source_history_epoch" => peer.source_history_epoch,
      "safe_source_sequence" => peer.safe_source_sequence,
      "installed_source_compaction_epoch" => peer.installed_source_compaction_epoch,
      "last_seen_at" => peer.last_seen_at,
      "lease_expires_at" => peer.lease_expires_at,
      "status" => Atom.to_string(peer.status)
    }
  end

  defp upsert_boundaries(_conn, [], _source_uuid, _source_epoch, _epoch), do: :ok

  defp upsert_boundaries(conn, boundaries, source_uuid, source_epoch, epoch) do
    Enum.reduce_while(boundaries, :ok, fn boundary, :ok ->
      upsert_boundary(conn, boundary, source_uuid, source_epoch, epoch)
    end)
  end

  defp upsert_boundary(conn, boundary, source_uuid, source_epoch, epoch) do
    key = boundary_key(source_uuid, boundary.document_id, boundary.history_id)
    value = encode_boundary(boundary, source_uuid, source_epoch, epoch)

    case Canonical.encode(value) do
      {:ok, json} -> execute_boundary_upsert(conn, key, json)
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp execute_boundary_upsert(conn, key, json) do
    case Connection.execute(
           conn,
           "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
           [@retention_boundaries, key, json]
         ) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp remove_boundaries(_conn, [], _source_uuid), do: :ok

  defp remove_boundaries(conn, boundaries, source_uuid) do
    Enum.reduce_while(boundaries, :ok, fn boundary, :ok ->
      key = boundary_key(source_uuid, boundary.document_id, boundary.history_id)

      case Connection.execute(
             conn,
             "DELETE FROM local_records WHERE namespace = ? AND record_key = ?",
             [@retention_boundaries, key]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  defp delete_revisions(_conn, removals) when map_size(removals) == 0, do: :ok

  defp delete_revisions(conn, removals) do
    Enum.reduce_while(removals, :ok, fn {document_id, revision_ids}, :ok ->
      delete_document_revisions(conn, document_id, revision_ids)
    end)
    |> normalize_delete_result()
  end

  defp delete_document_revisions(conn, document_id, revision_ids) do
    case doc_key_for(conn, document_id) do
      {:ok, doc_key} -> delete_revision_ids(conn, doc_key, revision_ids)
      {:error, %ElixirDB.Error{code: :document_not_found}} -> {:cont, :ok}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp delete_revision_ids(conn, doc_key, revision_ids) do
    Enum.reduce_while(revision_ids, :ok, fn revision_id, :ok ->
      delete_single_revision(conn, doc_key, revision_id)
    end)
    |> case do
      :ok -> {:cont, :ok}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp delete_single_revision(conn, doc_key, revision_id) do
    case Connection.execute(
           conn,
           "DELETE FROM revisions WHERE doc_key = ? AND revision_id = ?",
           [doc_key, revision_id]
         ) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
    end
  end

  defp empty_documents(_conn, []), do: :ok

  defp empty_documents(conn, document_ids) do
    Enum.reduce_while(document_ids, :ok, fn document_id, :ok ->
      case Connection.execute(
             conn,
             "UPDATE documents SET winning_revision = NULL, winning_body_json = NULL, winning_body_term = NULL, winning_deleted = 1, update_sequence = 0 WHERE document_id = ?",
             [document_id]
           ) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, normalize_error(reason)}}
      end
    end)
  end

  defp normalize_delete_result(:ok), do: :ok
  defp normalize_delete_result({:error, error}), do: {:error, error}

  defp delete_changes(_conn, 0), do: :ok

  defp delete_changes(conn, through) when is_integer(through) and through > 0 do
    case Connection.execute(conn, "DELETE FROM changes WHERE sequence <= ?", [through]) do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp update_meta(conn, floor, compaction_epoch, digest) do
    Connection.execute(
      conn,
      "UPDATE db_meta SET retention_floor_sequence = ?, compaction_epoch = ?, retention_boundary_digest = ? WHERE id = 1",
      [floor, compaction_epoch, digest]
    )
    |> case do
      :ok -> :ok
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp conn_digest(conn, source_database_uuid) do
    case list_boundaries(conn, source_database_uuid: source_database_uuid) do
      {:ok, boundaries} ->
        BoundaryPage.digest_for(Enum.map(boundaries, & &1.boundary))

      _ ->
        nil
    end
  end

  defp maybe_increment_maintenance(_conn, false), do: :ok

  defp maybe_increment_maintenance(conn, true) do
    with {:ok, counter} <- maintenance_counter(conn) do
      put_maintenance_counter(conn, counter + 1)
    end
  end

  defp doc_key_for(conn, document_id) do
    case Connection.query(conn, "SELECT doc_key FROM documents WHERE document_id = ?", [
           document_id
         ]) do
      {:ok, [[doc_key]]} -> {:ok, doc_key}
      {:ok, []} -> {:error, ElixirDB.Error.document_not_found("document not found")}
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp fetch_maintenance(conn, key),
    do: fetch_local_record(conn, @retention_maintenance, key)

  defp fetch_replication_state(conn, key),
    do: fetch_local_record(conn, @replication_state, key)

  defp fetch_local_record(conn, namespace, key) do
    case Connection.query(
           conn,
           "SELECT record_version, value_json FROM local_records WHERE namespace = ? AND record_key = ?",
           [namespace, key]
         ) do
      {:ok, []} -> {:ok, nil}
      {:ok, [[version, json]]} -> decode_record(version, json)
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  defp put_maintenance(conn, key, value) do
    put_local_record(conn, @retention_maintenance, key, value)
  end

  defp put_replication_state(conn, key, value) do
    put_local_record(conn, @replication_state, key, value)
  end

  defp put_local_record(conn, namespace, key, value) do
    case Canonical.encode(value) do
      {:ok, json} ->
        Connection.execute(
          conn,
          "INSERT INTO local_records(namespace, record_key, record_version, value_json) VALUES (?, ?, 1, ?) ON CONFLICT(namespace, record_key) DO UPDATE SET record_version = record_version + 1, value_json = excluded.value_json",
          [namespace, key, json]
        )

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp decode_record(version, json) do
    case StrictDecoder.decode(json) do
      {:ok, value} -> {:ok, %{version: version, value: value}}
      {:error, error} -> {:error, error}
    end
  end

  defp normalize_error(%ElixirDB.Error{} = error), do: error

  defp normalize_error(reason),
    do: ElixirDB.Error.internal_error("SQLite operation failed", %{cause: inspect(reason)})
end
