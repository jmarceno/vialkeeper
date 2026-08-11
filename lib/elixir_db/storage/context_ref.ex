defmodule ElixirDB.Storage.ContextRef do
  @moduledoc """
  Shared macros for refreshing opaque backend context references.

  Expands into the calling Context module so `OpaqueHandle` authorization still
  sees a backend Context frame on the call stack.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.OpaqueHandle

  @doc """
  Replaces the opaque backend ref and mirrors `adapter.identity` onto the context.
  """
  defmacro replace_ref(context, adapter) do
    quote do
      case {unquote(context), unquote(adapter)} do
        {%BackendContext{backend_ref: %OpaqueHandle{} = handle} = ctx, adapter} ->
          _ = OpaqueHandle.replace(handle, adapter)
          %{ctx | identity: Map.get(adapter, :identity) || %{}}

        {%BackendContext{} = ctx, adapter} ->
          %{
            ctx
            | backend_ref: OpaqueHandle.wrap(adapter),
              identity: Map.get(adapter, :identity) || %{}
          }
      end
    end
  end
end
