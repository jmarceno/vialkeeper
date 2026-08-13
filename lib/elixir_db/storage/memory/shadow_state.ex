defmodule ElixirDB.Storage.Memory.ShadowState do
  @moduledoc "In-memory shadow identity and source-origin port implementation."
  @behaviour ElixirDB.Storage.Ports.ShadowState

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Memory.{Context, Store}

  @impl true
  def metadata(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Map.get(Store.get(adapter.store), :shadow_metadata)}
    end
  end

  @impl true
  def put_metadata(%BackendContext{} = context, metadata) when is_map(metadata) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &put_metadata_state(&1, metadata))
    end
  end

  @impl true
  def origin(%BackendContext{} = context, document_id) when is_binary(document_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Map.get(Store.get(adapter.store).shadow_origins, document_id)}
    end
  end

  @impl true
  def put_origin(%BackendContext{} = context, document_id, sequence)
      when is_binary(document_id) and is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &put_origin_state(&1, document_id, sequence))
    end
  end

  def put_origin(_context, _document_id, _sequence),
    do: {:error, ElixirDB.Error.invalid_request("shadow origin is invalid")}

  @impl true
  def watermark(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      {:ok, Store.get(adapter.store).shadow_watermark}
    end
  end

  @impl true
  def put_watermark(%BackendContext{} = context, sequence)
      when is_integer(sequence) and sequence >= 0 do
    with {:ok, adapter} <- Context.unwrap(context) do
      Store.update(adapter.store, &put_watermark_state(&1, sequence))
    end
  end

  def put_watermark(_context, _sequence),
    do: {:error, ElixirDB.Error.invalid_request("shadow watermark is invalid")}

  defp put_metadata_state(state, metadata) do
    case Map.get(state, :shadow_metadata) do
      nil -> {:ok, %{state | shadow_metadata: metadata}, metadata}
      ^metadata -> {:ok, state, metadata}
      _ -> {:error, ElixirDB.Error.shadow_identity_conflict("shadow metadata is immutable")}
    end
  end

  defp put_origin_state(state, document_id, sequence) do
    current = Map.get(state.shadow_origins, document_id)

    if is_nil(current) or sequence >= current do
      origins = Map.put(state.shadow_origins, document_id, sequence)
      {:ok, %{state | shadow_origins: origins}, sequence}
    else
      {:error,
       ElixirDB.Error.shadow_generation_conflict(
         "source origin sequence cannot move backwards",
         %{document_id: document_id, current: current, requested: sequence}
       )}
    end
  end

  defp put_watermark_state(state, sequence) when sequence >= state.shadow_watermark do
    {:ok, %{state | shadow_watermark: sequence}, sequence}
  end

  defp put_watermark_state(state, sequence),
    do:
      {:error,
       ElixirDB.Error.shadow_generation_conflict(
         "shadow watermark cannot move backwards",
         %{current: state.shadow_watermark, requested: sequence}
       )}
end
