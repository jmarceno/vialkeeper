defmodule ElixirDB.Storage.Services.Shadows do
  @moduledoc "Shared shadow identity, origin, and durable watermark services."

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access

  @spec metadata(BackendContext.t()) :: {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def metadata(%BackendContext{} = context),
    do: Access.port(context, :shadow_state).metadata(context)

  @spec origin(BackendContext.t(), binary()) ::
          {:ok, non_neg_integer() | nil} | {:error, ElixirDB.Error.t()}
  def origin(%BackendContext{} = context, document_id),
    do: Access.port(context, :shadow_state).origin(context, document_id)

  @spec put_origin(BackendContext.t(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def put_origin(%BackendContext{} = context, document_id, sequence),
    do: Access.port(context, :shadow_state).put_origin(context, document_id, sequence)

  @spec watermark(BackendContext.t()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def watermark(%BackendContext{} = context),
    do: Access.port(context, :shadow_state).watermark(context)

  @spec put_watermark(BackendContext.t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def put_watermark(%BackendContext{} = context, sequence),
    do: Access.port(context, :shadow_state).put_watermark(context, sequence)
end
