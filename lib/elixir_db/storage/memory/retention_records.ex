defmodule ElixirDB.Storage.Memory.RetentionRecords do
  @moduledoc """
  Memory retention-records fact port for peer ledgers, boundary pages, and
  compaction effects.

  Mirrors the SQLite local-record namespaces with in-memory maps so shared
  retention services can run without SQL.
  """
  @behaviour ElixirDB.Storage.Ports.RetentionRecords

  alias ElixirDB.Domain.{BoundaryPage, RetentionBoundary}
  alias ElixirDB.MapAccess
  alias ElixirDB.Retention.Service, as: RetentionService
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Peers, Store}
  alias ElixirDB.Storage.Ports.Errors

  @impl true
  def retention_state(%BackendContext{} = _context) do
    {:error, ElixirDB.Error.invalid_request("use ElixirDB.Storage.Services.retention_state/1")}
  end

  @impl true
  def list_peer_positions(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Peers.decode(Store.get(adapter.store).peers)
    end
  end

  @impl true
  def put_peer_position_cas(%BackendContext{} = context, request) when is_map(request) do
    put_peer_position_record(context, request)
  end

  @impl true
  def read_boundary_pages(%BackendContext{} = _context, _request) do
    {:error, ElixirDB.Error.invalid_request("use ElixirDB.Storage.Services.read_boundary_pages/2")}
  end

  @impl true
  def install_boundary_pages(%BackendContext{} = _context, _request) do
    {:error,
     ElixirDB.Error.invalid_request("use ElixirDB.Storage.Services.install_boundary_pages/2")}
  end

  @impl true
  def get_compaction_result(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.get(adapter.store).compaction_result}
    end
  end

  @impl true
  def put_compaction_result(%BackendContext{} = context, result) when is_map(result) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state -> {:ok, %{state | compaction_result: result}, :ok} end)
      |> normalize_ok()
    end
  end

  @impl true
  def list_boundaries(%BackendContext{} = context), do: list_boundaries(context, [])

  @impl true
  def list_boundaries(%BackendContext{} = context, opts) when is_list(opts) do
    with {:ok, adapter} <- Context.unwrap(context) do
      source = Keyword.get(opts, :source_database_uuid)

      boundaries =
        adapter.store
        |> Store.get()
        |> Map.fetch!(:boundaries)
        |> Enum.filter(fn stored ->
          is_nil(source) or stored.source_database_uuid == source
        end)

      {:ok, boundaries}
    end
  end

  @impl true
  def install_imported_boundaries(%BackendContext{} = context, boundaries)
      when is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context) do
      merge_imported_boundaries(adapter, boundaries)
    end
  end

  @impl true
  def mark_pending_local_causal(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        {:ok, %{state | pending_local_causal: true}, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def pending_local_causal?(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.get(adapter.store).pending_local_causal}
    end
  end

  @impl true
  def encode_stored_boundary(stored) when is_map(stored) do
    boundary = Map.fetch!(stored, :boundary)

    %{
      "source_database_uuid" => stored.source_database_uuid,
      "source_history_epoch" => stored.source_history_epoch,
      "compaction_epoch" => stored.compaction_epoch,
      "boundary" => %{
        "document_id" => boundary.document_id,
        "history_id" => boundary.history_id,
        "minimum_retained_generation" => boundary.minimum_retained_generation,
        "retired" => boundary.retired,
        "retired_branch_roots" => boundary.retired_branch_roots
      }
    }
  end

  @impl true
  def apply_compaction_effect(%BackendContext{} = context, effect) when is_map(effect) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&compaction_update(&1, effect))
      |> normalize_ok()
    end
  end

  @impl true
  def boundary_install_state(%BackendContext{} = context, source_uuid)
      when is_binary(source_uuid) do
    with {:ok, adapter} <- Context.unwrap(context) do
      state = Store.get(adapter.store)

      case Map.get(state.boundary_install_state, source_uuid) do
        nil -> derive_boundary_install_state(state, source_uuid)
        install -> {:ok, install}
      end
    end
  end

  @impl true
  def begin_boundary_install(%BackendContext{} = context, install_id, install_state)
      when is_binary(install_id) and is_map(install_state) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, normalized} <- normalize_install_state(install_state) do
      adapter.store
      |> Store.update(fn state ->
        staging =
          state.boundary_staging
          |> Map.delete(install_id)
          |> Map.put(install_id, %{state: normalized, boundaries: %{}})

        {:ok, %{state | boundary_staging: staging}, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def stage_boundary_page(%BackendContext{} = context, install_id, page)
      when is_binary(install_id) and is_map(page) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(&stage_boundary_page_update(&1, install_id, page))
      |> normalize_ok()
    end
  end

  @impl true
  def complete_boundary_install(%BackendContext{} = context, install_id)
      when is_binary(install_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &do_complete_boundary_install(&1, install_id))
    end
  end

  @impl true
  def replace_boundary_set(%BackendContext{} = context, install_state, boundaries)
      when is_map(install_state) and is_list(boundaries) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, normalized} <- normalize_install_state(install_state),
         :ok <- validate_boundary_set_digest(normalized, boundaries) do
      Store.update(adapter.store, fn state ->
        new_state = replace_authoritative(state, normalized, boundaries)

        {:ok, new_state,
         %{
           installed: length(boundaries),
           boundary_digest: normalized.boundary_digest,
           compaction_epoch: normalized.compaction_epoch
         }}
      end)
    end
  end

  @impl true
  def maintenance_counter(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.get(adapter.store).maintenance_counter}
    end
  end

  @impl true
  def update_peer_wire(%BackendContext{} = context, peer_uuid, wire)
      when is_binary(peer_uuid) and is_map(wire) do
    with {:ok, adapter} <- Context.unwrap(context) do
      adapter.store
      |> Store.update(fn state ->
        {:ok, new_state} = Store.update_peer_wire(state, peer_uuid, wire)
        {:ok, new_state, :ok}
      end)
      |> normalize_ok()
    end
  end

  @impl true
  def put_peer_position_record(%BackendContext{} = context, request) when is_map(request) do
    peer_uuid = MapAccess.get(request, :peer_database_uuid)
    expected = MapAccess.get(request, :expected_version, 0)
    value = MapAccess.get(request, :value)

    with {:ok, adapter} <- Context.unwrap(context),
         true <- is_binary(peer_uuid),
         true <- is_map(value) do
      Store.update(adapter.store, &Store.put_peer_cas(&1, peer_uuid, expected, value))
    else
      false -> {:error, ElixirDB.Error.invalid_request("peer position CAS fields are invalid")}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  defp compaction_update(state, effect) do
    with {:ok, new_state} <- Store.apply_compaction_effect(state, effect) do
      {:ok, new_state, :ok}
    end
  end

  defp stage_boundary_page_update(state, install_id, page) do
    with {:ok, new_state} <- do_stage_boundary_page(state, install_id, page) do
      {:ok, new_state, :ok}
    end
  end

  defp derive_boundary_install_state(state, source_uuid) do
    boundaries =
      Enum.filter(state.boundaries, &(&1.source_database_uuid == source_uuid))

    case boundaries do
      [] ->
        {:ok, nil}

      [%{source_history_epoch: source_epoch} | _] = values ->
        epochs = Enum.map(values, & &1.compaction_epoch)

        case Enum.uniq(Enum.map(values, & &1.source_history_epoch)) do
          [^source_epoch] ->
            {:ok,
             %{
               source_database_uuid: source_uuid,
               source_history_epoch: source_epoch,
               compaction_epoch: Enum.max(epochs),
               boundary_digest: BoundaryPage.digest_for(Enum.map(values, & &1.boundary))
             }}

          _ ->
            {:error,
             ElixirDB.Error.integrity_violation(
               "installed boundaries contain multiple source history epochs"
             )}
        end
    end
  end

  defp normalize_install_state(state) when is_map(state) do
    source_uuid = MapAccess.get(state, :source_database_uuid)
    source_epoch = MapAccess.get(state, :source_history_epoch)
    compaction_epoch = MapAccess.get(state, :compaction_epoch)
    boundary_digest = MapAccess.get(state, :boundary_digest)

    if is_binary(source_uuid) and source_uuid != "" and is_binary(source_epoch) and
         source_epoch != "" and is_integer(compaction_epoch) and compaction_epoch >= 0 and
         is_binary(boundary_digest) and boundary_digest != "" do
      {:ok,
       %{
         source_database_uuid: source_uuid,
         source_history_epoch: source_epoch,
         compaction_epoch: compaction_epoch,
         boundary_digest: boundary_digest
       }}
    else
      {:error, ElixirDB.Error.invalid_request("boundary install state is incomplete")}
    end
  end

  defp do_stage_boundary_page(state, install_id, page) do
    case Map.fetch(state.boundary_staging, install_id) do
      :error ->
        {:error, ElixirDB.Error.boundary_conflict("boundary install session is missing")}

      {:ok, staging} ->
        stage_into_session(state, install_id, staging, page)
    end
  end

  defp stage_into_session(state, install_id, staging, page) do
    with :ok <- RetentionService.same_boundary_install?(staging.state, page) do
      boundaries = merge_staged_boundaries(staging.boundaries, page.boundaries)
      staging = %{staging | boundaries: boundaries}
      {:ok, %{state | boundary_staging: Map.put(state.boundary_staging, install_id, staging)}}
    end
  end

  defp merge_staged_boundaries(existing, boundaries) do
    Enum.reduce(boundaries, existing, fn boundary, acc ->
      Map.put(acc, {boundary.document_id, boundary.history_id}, boundary)
    end)
  end

  defp do_complete_boundary_install(state, install_id) do
    case Map.fetch(state.boundary_staging, install_id) do
      :error ->
        {:error, ElixirDB.Error.boundary_conflict("boundary install session is missing")}

      {:ok, staging} ->
        complete_staged_install(state, install_id, staging)
    end
  end

  defp complete_staged_install(state, install_id, %{state: metadata, boundaries: by_key}) do
    boundaries = Map.values(by_key)
    digest = BoundaryPage.digest_for(boundaries)

    if digest == metadata.boundary_digest do
      new_state =
        state
        |> replace_authoritative(metadata, boundaries)
        |> then(fn s ->
          %{s | boundary_staging: Map.delete(s.boundary_staging, install_id)}
        end)

      {:ok, new_state,
       %{
         installed: length(boundaries),
         boundary_digest: metadata.boundary_digest,
         compaction_epoch: metadata.compaction_epoch
       }}
    else
      {:error, ElixirDB.Error.boundary_conflict("installed boundary digest mismatch")}
    end
  end

  defp validate_boundary_set_digest(state, boundaries) do
    expected = Map.get(state, :boundary_digest)

    if is_binary(expected) and expected == BoundaryPage.digest_for(boundaries) do
      :ok
    else
      {:error, ElixirDB.Error.boundary_conflict("installed boundary digest mismatch")}
    end
  end

  defp replace_authoritative(state, install_state, boundaries) do
    source_uuid = install_state.source_database_uuid
    source_epoch = install_state.source_history_epoch
    compaction_epoch = install_state.compaction_epoch

    kept =
      Enum.reject(state.boundaries, &(&1.source_database_uuid == source_uuid))

    installed =
      Enum.map(boundaries, fn boundary ->
        %{
          source_database_uuid: source_uuid,
          source_history_epoch: source_epoch,
          compaction_epoch: compaction_epoch,
          boundary: boundary
        }
      end)

    %{
      state
      | boundaries: kept ++ installed,
        boundary_install_state: Map.put(state.boundary_install_state, source_uuid, install_state)
    }
  end

  defp normalize_all(boundaries, existing) do
    Enum.reduce_while(boundaries, {:ok, existing}, fn raw, {:ok, acc} ->
      case normalize_one(raw) do
        {:ok, stored} -> {:cont, {:ok, upsert(acc, stored)}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp merge_imported_boundaries(adapter, boundaries) do
    adapter.store
    |> Store.update(fn state ->
      case normalize_all(boundaries, state.boundaries) do
        {:ok, merged} -> {:ok, %{state | boundaries: merged}, :ok}
        {:error, error} -> {:error, error}
      end
    end)
    |> normalize_ok()
  end

  defp normalize_one(raw) do
    with {:ok, boundary} <-
           RetentionBoundary.from_wire(
             MapAccess.get(raw, :boundary) || MapAccess.get(raw, "boundary")
           ),
         source_uuid when is_binary(source_uuid) <-
           MapAccess.get(raw, :source_database_uuid) || MapAccess.get(raw, "source_database_uuid"),
         source_epoch when is_binary(source_epoch) <-
           MapAccess.get(raw, :source_history_epoch) || MapAccess.get(raw, "source_history_epoch"),
         compaction_epoch when is_integer(compaction_epoch) <-
           MapAccess.get(raw, :compaction_epoch) || MapAccess.get(raw, "compaction_epoch") || 0 do
      {:ok,
       %{
         source_database_uuid: source_uuid,
         source_history_epoch: source_epoch,
         compaction_epoch: compaction_epoch,
         boundary: boundary
       }}
    else
      _ -> {:error, ElixirDB.Error.invalid_request("purged boundary is invalid")}
    end
  end

  defp upsert(list, stored) do
    key = {stored.source_database_uuid, stored.boundary.document_id, stored.boundary.history_id}

    list
    |> Enum.reject(fn existing ->
      {existing.source_database_uuid, existing.boundary.document_id, existing.boundary.history_id} ==
        key
    end)
    |> Kernel.++([stored])
  end

  defp normalize_ok({:ok, :ok}), do: :ok
  defp normalize_ok({:error, reason}), do: {:error, Errors.normalize(reason)}
end
