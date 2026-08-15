defmodule VialKeeper.Storage.SQLite.InspectionPort do
  @moduledoc """
  SQLite inspection port for integrity snapshots and capability probes.
  """
  @behaviour VialKeeper.Storage.Ports.Inspection

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Errors
  alias VialKeeper.Storage.SQLite.{Adapter, Capabilities, Context, Integrity}

  @impl true
  def integrity_check(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    VialKeeper.Storage.Services.Integrity.check(context, options)
  end

  @impl true
  def load_integrity_snapshot(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Integrity.load_integrity_snapshot(adapter.conn))
    end
  end

  @impl true
  def physical_integrity_check(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, indexes} <- Adapter.list_indexes(adapter) do
      Errors.wrap(
        Integrity.physical_integrity_check(
          adapter.conn,
          indexes,
          bundle_root(adapter)
        )
      )
    end
  end

  @impl true
  def capabilities_report, do: Capabilities.report()

  @impl true
  def validate_capabilities!, do: Capabilities.validate!()

  defp bundle_root(adapter) do
    case {Map.get(adapter, :storage_mode), Map.get(adapter, :path)} do
      {:memory, _path} -> nil
      {_mode, path} when is_binary(path) -> Path.dirname(Path.expand(path))
      _ -> nil
    end
  end
end
