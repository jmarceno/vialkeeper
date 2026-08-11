defmodule ElixirDB.Storage.SQLite.InspectionPort do
  @moduledoc """
  SQLite inspection port for integrity snapshots and capability probes.
  """
  @behaviour ElixirDB.Storage.Ports.Inspection

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Capabilities, Context}

  @impl true
  def integrity_check(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.integrity_check(adapter, options))
    end
  end

  @impl true
  def capabilities_report, do: Capabilities.report()

  @impl true
  def validate_capabilities!, do: Capabilities.validate!()
end
