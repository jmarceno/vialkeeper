defmodule VialKeeper.Diagnostics do
  @moduledoc """
  Generic runtime diagnostics for releases and support.

  Backend-specific capability details are supplied by the configured storage
  backend and appear under `:backend` as an opaque map.
  """

  alias VialKeeper.Storage.Registry, as: StorageRegistry

  @doc "Returns generic runtime identity plus opaque selected-backend capabilities."
  @spec runtime() :: map()
  def runtime do
    backend = StorageRegistry.backend()

    %{
      app_version: app_version(),
      elixir: System.version(),
      otp: :erlang.system_info(:otp_release) |> List.to_string(),
      protocol_major: VialKeeper.protocol_major(),
      revision_algorithm_version: VialKeeper.revision_algorithm_version(),
      canonicalization_version: VialKeeper.canonicalization_version(),
      storage_backend: inspect(backend),
      backend: backend_report(backend)
    }
  end

  @doc """
  Returns the assembled `:vial_keeper` application version from the BEAM app
  resource (the Mix project version), not from any VCS metadata.
  """
  @spec app_version() :: binary()
  def app_version do
    case Application.spec(:vial_keeper, :vsn) do
      vsn when is_list(vsn) -> List.to_string(vsn)
      vsn when is_binary(vsn) -> vsn
      _ -> "unknown"
    end
  end

  @doc "Validates required capabilities for the configured storage backend."
  @spec validate_backend!() :: term()
  def validate_backend! do
    StorageRegistry.backend().validate_capabilities!()
  end

  defp backend_report(backend) do
    if function_exported?(backend, :capabilities_report, 0) do
      backend.capabilities_report()
    else
      %{engine: "unknown"}
    end
  end
end
