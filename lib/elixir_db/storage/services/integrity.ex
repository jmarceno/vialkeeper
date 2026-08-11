defmodule ElixirDB.Storage.Services.Integrity do
  @moduledoc """
  Shared integrity orchestration over storage inspection ports.

  Loads a normalized logical snapshot, runs `ElixirDB.Integrity.Rules`, and
  merges backend-owned physical probe details when available.
  """

  alias ElixirDB.Integrity.Rules
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access

  @doc """
  Runs logical integrity rules and optional physical backend probes.

  Prefer `load_integrity_snapshot/2` on the inspection port. When that callback
  is absent, returns an unsupported-operation style internal error rather than
  inventing a partial snapshot from other ports.
  """
  @spec check(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def check(%BackendContext{} = context, options \\ %{}) when is_map(options) do
    inspection = Access.port(context, :inspection)

    with {:ok, snapshot} <- load_snapshot(inspection, context, options),
         :ok <- Rules.validate(snapshot),
         {:ok, physical} <- physical_check(inspection, context, options) do
      {:ok, Map.merge(%{ok: true}, physical)}
    end
  end

  defp load_snapshot(inspection, context, options) do
    _ = Code.ensure_loaded(inspection)

    if function_exported?(inspection, :load_integrity_snapshot, 2) do
      inspection.load_integrity_snapshot(context, options)
    else
      {:error,
       ElixirDB.Error.internal_error("inspection port does not load integrity snapshots", %{
         port: inspection
       })}
    end
  end

  defp physical_check(inspection, context, options) do
    _ = Code.ensure_loaded(inspection)

    if function_exported?(inspection, :physical_integrity_check, 2) do
      case inspection.physical_integrity_check(context, options) do
        {:ok, details} when is_map(details) -> {:ok, details}
        {:error, _} = error -> error
      end
    else
      {:ok, %{backend_details: %{}}}
    end
  end
end
