defmodule VialKeeper.Storage.Registry do
  @moduledoc """
  Backend selection boundary for the storage port layer.

  Runtime code resolves a configured backend module through this registry
  instead of aliasing a physical engine.

  Configure `:storage_backend` under the `:vial_keeper` application environment.
  """

  @doc "Returns the configured storage backend module."
  @spec backend() :: module()
  def backend do
    case Application.get_env(:vial_keeper, :storage_backend) do
      module when is_atom(module) and not is_nil(module) ->
        module

      other ->
        raise ArgumentError,
              "vial_keeper :storage_backend must be a backend module, got: #{inspect(other)}"
    end
  end

  @doc "Returns true when `module` exports the Storage.Adapter lifecycle callbacks."
  @spec adapter_module?(module()) :: boolean()
  def adapter_module?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :create, 2) and
      function_exported?(module, :open, 2) and function_exported?(module, :close, 1) and
      function_exported?(module, :identity, 1)
  end

  def adapter_module?(_), do: false

  @doc "Returns true when `module` exposes a port composition map."
  @spec port_backend?(module()) :: boolean()
  def port_backend?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :port_modules, 0) and
      function_exported?(module, :run_transaction, 2)
  end

  def port_backend?(_), do: false
end
