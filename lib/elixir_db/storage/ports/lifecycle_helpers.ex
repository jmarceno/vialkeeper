defmodule ElixirDB.Storage.Ports.LifecycleHelpers do
  @moduledoc """
  Shared helpers for lifecycle ports that wrap Storage.Adapter create/open into
  an opaque `BackendContext`.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors

  @doc """
  Injects `create/1-2` and `open/1-2` callbacks that return `BackendContext`
  values through `adapter`.
  """
  defmacro __using__(opts) do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      @impl true
      def create(path, options \\ %{}) when is_binary(path) and is_map(options) do
        unquote(__MODULE__).create_context(unquote(adapter), path, options)
      end

      @impl true
      def open(path, options \\ %{}) when is_binary(path) and is_map(options) do
        unquote(__MODULE__).open_context(unquote(adapter), path, options)
      end
    end
  end

  @doc "Creates a backend through `adapter_mod` and returns a context."
  @spec create_context(module(), binary(), map()) ::
          {:ok, BackendContext.t()} | {:error, ElixirDB.Error.t()}
  def create_context(adapter_mod, path, options)
      when is_atom(adapter_mod) and is_binary(path) and is_map(options) do
    case adapter_mod.create(path, options) do
      {:ok, adapter} -> {:ok, adapter_mod.to_context(adapter)}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end

  @doc "Opens a backend through `adapter_mod` and returns a context."
  @spec open_context(module(), binary(), map()) ::
          {:ok, BackendContext.t()} | {:error, ElixirDB.Error.t()}
  def open_context(adapter_mod, path, options)
      when is_atom(adapter_mod) and is_binary(path) and is_map(options) do
    case adapter_mod.open(path, options) do
      {:ok, adapter} -> {:ok, adapter_mod.to_context(adapter)}
      {:error, reason} -> {:error, Errors.normalize(reason)}
    end
  end
end
