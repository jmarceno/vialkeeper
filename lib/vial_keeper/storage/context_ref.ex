defmodule VialKeeper.Storage.ContextRef do
  @moduledoc """
  Shared macros for refreshing opaque backend context references.

  Expands into the calling Context module so `OpaqueHandle` authorization still
  sees a backend Context frame on the call stack.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.OpaqueHandle

  @doc """
  Replaces the opaque backend ref and mirrors `adapter.identity` onto the context.
  """
  defmacro replace_ref(context, adapter) do
    quote do
      case {unquote(context), unquote(adapter)} do
        {%BackendContext{backend_ref: %OpaqueHandle{} = handle} = ctx, adapter} ->
          _ = OpaqueHandle.replace(handle, adapter)
          unquote(__MODULE__).mirror_identity(ctx, adapter)

        {%BackendContext{} = ctx, adapter} ->
          %{
            ctx
            | backend_ref: OpaqueHandle.wrap(adapter),
              identity: Map.get(adapter, :identity) || %{}
          }
      end
    end
  end

  @doc "Mirrors `adapter.identity` onto the context without touching its handle."
  @spec mirror_identity(BackendContext.t(), map()) :: BackendContext.t()
  def mirror_identity(%BackendContext{} = context, adapter) when is_map(adapter),
    do: %{context | identity: Map.get(adapter, :identity) || %{}}
end
