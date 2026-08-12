defmodule ElixirDB.Storage.Ports.Access do
  @moduledoc """
  Resolves port implementation modules from an opaque `BackendContext`.

  Shared services call ports through this helper so they never unwrap
  backend-private handles.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Registry

  @doc "Returns the module implementing `family` for `context`."
  @spec port(BackendContext.t(), atom()) :: module()
  def port(%BackendContext{} = context, family) when is_atom(family) do
    backend = BackendContext.backend(context)

    cond do
      function_exported?(backend, :port, 1) ->
        backend.port(family)

      function_exported?(backend, :port_modules, 0) ->
        Map.fetch!(backend.port_modules(), family)

      true ->
        raise ArgumentError,
              "storage backend #{inspect(backend)} does not expose port composition"
    end
  end

  @doc "Returns true when the backend exposes an implementation for `family`."
  @spec available?(BackendContext.t(), atom()) :: boolean()
  def available?(%BackendContext{} = context, family) when is_atom(family) do
    backend = BackendContext.backend(context)

    cond do
      function_exported?(backend, :port_modules, 0) ->
        Map.has_key?(backend.port_modules(), family)

      function_exported?(backend, :port, 1) ->
        try do
          _ = backend.port(family)
          true
        rescue
          KeyError -> false
        end

      true ->
        false
    end
  end

  @doc "Returns true when `context` backend advertises port composition."
  @spec port_backend?(BackendContext.t()) :: boolean()
  def port_backend?(%BackendContext{} = context) do
    Registry.port_backend?(BackendContext.backend(context))
  end
end
