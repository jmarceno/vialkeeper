defmodule VialKeeper.MaterializedViews do
  @moduledoc "Application facade for creating materialized federated views."

  alias VialKeeper.DatabaseKind
  alias VialKeeper.DerivedView.{Definition, Path}
  alias VialKeeper.DerivedView.{Manager, Worker}
  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Runtime.DatabaseCatalog

  @spec create(map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def create(request) when is_map(request) do
    with {:ok, definition} <- Definition.normalize(request),
         :ok <- validate_source_kinds(definition.sources),
         database_uuid <- VialKeeper.UUID.v4(),
         relative_path <- Path.for(definition.name, database_uuid),
         initial <- Definition.initial_metadata(definition, database_uuid),
         {:ok, identity} <-
           DatabaseCatalog.create_internal(relative_path, %{
             database_uuid: database_uuid,
             database_kind: DatabaseKind.derived(),
             initial_derived_view: initial
           }) do
      case maybe_start_materializer(database_uuid, definition) do
        :ok ->
          {:ok,
           Map.merge(identity, %{
             materialization_id: database_uuid,
             database_path: relative_path,
             definition_digest: definition.definition_digest
           })}

        {:error, %Error{} = error} ->
          cleanup_failed_creation(database_uuid, relative_path)
          {:error, error}
      end
    end
  end

  def create(_request),
    do: {:error, VialKeeper.Error.invalid_request("materialized view request must be an object")}

  @spec list() :: {:ok, [map()]} | {:error, Error.t()}
  def list do
    with {:ok, entries} <- DatabaseCatalog.list() do
      entries
      |> Enum.filter(&derived_entry?/1)
      |> Enum.sort_by(&entry_uuid/1)
      |> collect_views()
    end
  end

  @spec get(binary()) :: {:ok, map()} | {:error, Error.t()}
  def get(uuid) when is_binary(uuid) do
    with {:ok, identity} <- DatabaseCatalog.info(uuid),
         :ok <- ensure_derived(identity),
         {:ok, metadata} <- metadata(uuid),
         {:ok, sources} <- sources(uuid),
         {:ok, definition} <- public_definition(metadata.definition_json) do
      {:ok, public_view(uuid, metadata, sources, definition)}
    end
  end

  @spec enable(binary()) :: {:ok, map()} | {:error, Error.t()}
  def enable(uuid) when is_binary(uuid), do: set_enabled(uuid, true)

  @spec disable(binary()) :: {:ok, map()} | {:error, Error.t()}
  def disable(uuid) when is_binary(uuid), do: set_enabled(uuid, false)

  @spec refresh(binary()) :: {:ok, map()} | {:error, Error.t()}
  def refresh(uuid) when is_binary(uuid), do: run_action(uuid, &Manager.refresh/1)

  @spec rebuild(binary()) :: {:ok, map()} | {:error, Error.t()}
  def rebuild(uuid) when is_binary(uuid), do: run_action(uuid, &Manager.rebuild/1)

  defp validate_source_kinds(sources) do
    Enum.reduce_while(sources, :ok, fn source_uuid, :ok ->
      case DatabaseCatalog.info(source_uuid) do
        {:ok, %{database_kind: :ordinary}} ->
          {:cont, :ok}

        {:ok, %{"database_kind" => "ordinary"}} ->
          {:cont, :ok}

        {:ok, %{database_kind: :derived}} ->
          {:halt,
           {:error,
            VialKeeper.Error.invalid_request("materialized view sources must be ordinary databases")}}

        {:ok, %{"database_kind" => "derived"}} ->
          {:halt,
           {:error,
            VialKeeper.Error.invalid_request("materialized view sources must be ordinary databases")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp set_enabled(uuid, enabled) do
    with {:ok, metadata} <- metadata(uuid),
         {:ok, _result} <-
           DatabaseCatalog.command(
             uuid,
             {:command, :set_derived_enabled,
              %{materialization_id: metadata.materialization_id, enabled: enabled}}
           ),
         {:ok, view} <- get(uuid) do
      {:ok, Map.put(view, "accepted", true)}
    end
  end

  defp run_action(uuid, action) when is_function(action, 1) do
    with {:ok, view} <- get(uuid),
         :ok <- require_enabled(view),
         :ok <- ensure_worker(uuid, view),
         :ok <- action.(uuid),
         {:ok, updated} <- get(uuid) do
      {:ok, Map.put(updated, "accepted", true)}
    end
  end

  defp metadata(uuid) do
    with {:ok, identity} <- DatabaseCatalog.info(uuid),
         :ok <- ensure_derived(identity) do
      DatabaseCatalog.command(uuid, {:command, :get_derived_view, %{}})
    end
  end

  defp collect_views(entries) do
    Enum.reduce_while(entries, {:ok, []}, &collect_view/2)
    |> case do
      {:ok, views} -> {:ok, Enum.reverse(views)}
      {:error, _} = error -> error
    end
  end

  defp collect_view(entry, {:ok, acc}) do
    case get(entry_uuid(entry)) do
      {:ok, view} -> {:cont, {:ok, [view | acc]}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp sources(uuid),
    do: DatabaseCatalog.command(uuid, {:command, :list_derived_sources, %{}})

  defp public_definition(json) when is_binary(json) do
    case StrictDecoder.decode(json) do
      {:ok, definition} when is_map(definition) -> {:ok, definition}
      _ -> {:error, Error.integrity_violation("derived view definition is invalid")}
    end
  end

  defp public_view(uuid, metadata, sources, definition) do
    runtime_status = runtime_status(uuid, metadata.enabled)

    %{
      "materialization_id" => metadata.materialization_id,
      "database_uuid" => uuid,
      "database_kind" => "derived",
      "database_path" => registered_path(uuid),
      "name" => metadata.name,
      "definition" => definition,
      "definition_digest" => metadata.definition_digest,
      "sources" => Enum.map(sources, &public_source/1),
      "enabled" => metadata.enabled,
      "persistent_state" => state_string(metadata.status),
      "runtime_status" => runtime_status,
      "status" => runtime_status
    }
  end

  defp public_source(source) do
    %{
      "source_ordinal" => source.source_ordinal,
      "source_database_uuid" => source.source_database_uuid,
      "source_history_epoch" => source.history_epoch,
      "checkpoint_sequence" => source.checkpoint_sequence,
      "state" => state_string(source.state),
      "rebuild_generation" => source.rebuild_generation,
      "last_error_code" => source.last_error_code
    }
  end

  defp runtime_status(uuid, true) do
    case Worker.status(uuid) do
      {:ok, %{status: status}} -> state_string(status)
      {:error, _} -> "stopped"
    end
  end

  defp runtime_status(_uuid, false), do: "stopped"

  defp registered_path(uuid) do
    case DatabaseCatalog.list() do
      {:ok, entries} ->
        entries
        |> Enum.find(&(&1.database_uuid == uuid))
        |> case do
          %{path: path} -> path
          _ -> nil
        end

      {:error, _} ->
        nil
    end
  end

  defp ensure_worker(uuid, view) do
    source_uuids = Enum.map(view["sources"], & &1["source_database_uuid"])

    case Manager.start(uuid, source_uuids) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         Error.database_unavailable("derived materializer could not start", %{
           cause: inspect(reason)
         })}
    end
  end

  defp maybe_start_materializer(_uuid, %{enabled: false}), do: :ok

  defp maybe_start_materializer(uuid, %{enabled: true, sources: sources}) do
    case Manager.start(uuid, sources) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         Error.database_unavailable("derived materializer could not start", %{
           cause: inspect(reason)
         })}
    end
  end

  defp cleanup_failed_creation(uuid, relative_path) do
    _ = DatabaseCatalog.command(uuid, {:command, :set_derived_enabled, %{enabled: false}})

    with :ok <- Manager.close(uuid),
         :ok <- DatabaseCatalog.close(uuid),
         :ok <- DatabaseCatalog.unregister(uuid) do
      _ = File.rm_rf(Elixir.Path.join(VialKeeper.Config.database_root(), relative_path))
      :ok
    else
      _ -> :ok
    end
  catch
    :exit, _ -> :ok
  end

  defp require_enabled(%{"enabled" => true}), do: :ok

  defp require_enabled(_view),
    do: {:error, Error.invalid_request("materialized view must be enabled")}

  defp ensure_derived(identity) do
    kind = Map.get(identity, :database_kind, Map.get(identity, "database_kind"))

    if kind in [:derived, "derived"],
      do: :ok,
      else: {:error, Error.invalid_request("database is not a materialized view")}
  end

  defp derived_entry?(entry), do: entry_kind(entry) in [:derived, "derived"]

  defp entry_kind(entry), do: Map.get(entry, :database_kind, Map.get(entry, "database_kind"))

  defp entry_uuid(entry), do: Map.get(entry, :database_uuid, Map.get(entry, "database_uuid"))

  defp state_string(nil), do: nil
  defp state_string(value) when is_atom(value), do: Atom.to_string(value)
  defp state_string(value) when is_binary(value), do: value
  defp state_string(value), do: to_string(value)
end
