defmodule VialKeeper.Bench.Runtime do
  @moduledoc """
  Isolated VialKeeper application lifecycle rooted at the external benchmark root.
  """

  alias VialKeeper.Bench.Root
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.Manager

  @spec with_isolated(Root.t(), (-> result)) :: result when result: var
  def with_isolated(%Root{} = context, fun) when is_function(fun, 0) do
    previous = snapshot_env()
    ensure_application_stopped!()

    Application.put_env(:vial_keeper, :database_root, context.root)

    Application.put_env(
      :vial_keeper,
      :registration_manifest,
      Path.join(context.root, "registrations.json")
    )

    Application.put_env(:vial_keeper, :listener, ip: {127, 0, 0, 1}, port: 0)
    Application.put_env(:vial_keeper, :host_limits, elevated_limits(previous.host_limits))

    try do
      {:ok, _started} = Application.ensure_all_started(:vial_keeper)
      fun.()
    after
      _ = Application.stop(:vial_keeper)
      restore_env(previous)
    end
  end

  @spec create_work_database(Root.t(), binary(), binary()) ::
          {:ok, binary(), binary()} | {:error, binary()}
  def create_work_database(%Root{} = context, benchmark, run_id) do
    relative = Path.join(["work", benchmark, run_id, "bench.vialkeeper"])

    with {:ok, _absolute} <- Root.resolve(context, Path.split(relative)),
         {:ok, identity} <- create_bundle(relative) do
      open_bundle(identity.database_uuid, relative)
    end
  end

  defp create_bundle(relative) do
    case DatabaseCatalog.create(relative) do
      {:ok, identity} -> {:ok, identity}
      {:error, error} -> {:error, "could not create benchmark database: #{inspect(error)}"}
    end
  end

  defp open_bundle(uuid, relative) do
    case DatabaseCatalog.open(uuid) do
      {:ok, _} ->
        :ok = Manager.await_resumed(uuid)
        {:ok, uuid, relative}

      {:error, error} ->
        {:error, "could not open benchmark database: #{inspect(error)}"}
    end
  end

  @spec close_work_database(Root.t(), binary(), binary()) :: :ok
  def close_work_database(%Root{} = context, uuid, relative) do
    _ = DatabaseCatalog.close(uuid)
    _ = DatabaseCatalog.unregister(uuid)

    case Root.resolve(context, Path.split(relative)) do
      {:ok, absolute} ->
        _ = Root.remove_work_run!(context, Path.dirname(absolute))
        :ok

      {:error, _} ->
        :ok
    end
  end

  defp ensure_application_stopped! do
    if Enum.any?(Application.started_applications(), &match?({:vial_keeper, _, _}, &1)) do
      Mix.raise("benchmark must be launched with mix run --no-start")
    end
  end

  defp snapshot_env do
    %{
      database_root: Application.get_env(:vial_keeper, :database_root),
      registration_manifest: Application.get_env(:vial_keeper, :registration_manifest),
      listener: Application.get_env(:vial_keeper, :listener),
      host_limits: Application.get_env(:vial_keeper, :host_limits)
    }
  end

  defp restore_env(previous) do
    restore(:database_root, previous.database_root)
    restore(:registration_manifest, previous.registration_manifest)
    restore(:listener, previous.listener)
    restore(:host_limits, previous.host_limits)
  end

  defp restore(key, nil), do: Application.delete_env(:vial_keeper, key)
  defp restore(key, value), do: Application.put_env(:vial_keeper, key, value)

  defp elevated_limits(nil), do: elevated_limits([])

  defp elevated_limits(limits) when is_list(limits) do
    Keyword.merge(limits,
      max_document_bytes: 67_108_864,
      max_request_bytes: 134_217_728,
      max_query_execution_ms: 300_000,
      max_search_rebuild_ms: 3_600_000,
      max_full_scan_documents: 200_000,
      max_query_results: 500
    )
  end
end
