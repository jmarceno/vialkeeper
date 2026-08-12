defmodule ElixirDB.Attachments.Compression do
  @moduledoc "Streaming Zstandard helpers for attachment payloads."

  @zstd_buffer 64 * 1024
  @level 1

  @type context :: reference()

  @doc "Returns whether streaming Zstandard is available."
  @spec enabled?() :: boolean()
  def enabled?, do: true

  @doc "Probes compressibility of a bounded prefix (at most 256 KiB)."
  @spec probe(binary()) ::
          {:ok, %{input_size: pos_integer(), compressed_size: pos_integer()}}
          | {:error, term()}
  def probe(prefix) when is_binary(prefix) and byte_size(prefix) > 0 do
    with {:ok, ctx} <- new_compression_context(),
         :ok <- set_level(ctx),
         {:ok, compressed} <- compress_chunks(ctx, [prefix], true) do
      {:ok, %{input_size: byte_size(prefix), compressed_size: byte_size(compressed)}}
    end
  end

  def probe(_), do: {:error, :empty_prefix}

  @doc "Returns true when compressed probe output is at most 90% of probe input."
  @spec worthwhile?(%{input_size: pos_integer(), compressed_size: pos_integer()}) :: boolean()
  def worthwhile?(%{input_size: input, compressed_size: output}) do
    output * 10 <= input * 9
  end

  @spec new_compression_context() :: {:ok, context()} | {:error, term()}
  def new_compression_context do
    case :ezstd.create_compression_context(@zstd_buffer) do
      ctx when is_reference(ctx) -> {:ok, ctx}
      {:error, _} = error -> error
    end
  end

  @spec set_level(context()) :: :ok | {:error, term()}
  def set_level(ctx) do
    case :ezstd.set_compression_parameter(ctx, :zstd_c_compression_level, @level) do
      :ok -> :ok
      {:error, _} = error -> error
    end
  end

  @spec compress_chunk(context(), binary()) :: {:ok, iodata(), context()} | {:error, term()}
  def compress_chunk(ctx, chunk) when is_binary(chunk) do
    case :ezstd.compress_streaming(ctx, chunk) do
      output when is_list(output) -> {:ok, output, ctx}
      {:error, _} = error -> error
    end
  end

  @spec finish_compression(context(), binary()) :: {:ok, iodata(), context()} | {:error, term()}
  def finish_compression(ctx, <<>>) do
    case :ezstd.compress_streaming_end(ctx, <<>>) do
      output when is_list(output) -> {:ok, output, ctx}
      {:error, _} = error -> error
    end
  end

  def finish_compression(ctx, chunk) when is_binary(chunk) do
    with {:ok, mid, ctx} <- compress_chunk(ctx, chunk),
         {:ok, tail, ctx} <- finish_compression(ctx, <<>>) do
      {:ok, [mid | tail], ctx}
    end
  end

  @spec compress_chunks(context(), [binary()], boolean()) ::
          {:ok, binary()} | {:error, term()}
  def compress_chunks(ctx, chunks, finish?) do
    with {:ok, ctx, outputs} <- compress_chunk_loop(ctx, chunks, []),
         {:ok, final, _ctx} <- maybe_finish(outputs, ctx, finish?) do
      {:ok, IO.iodata_to_binary(final)}
    end
  end

  defp compress_chunk_loop(ctx, [], acc), do: {:ok, ctx, Enum.reverse(acc)}

  defp compress_chunk_loop(ctx, [chunk | rest], acc) do
    case compress_chunk(ctx, chunk) do
      {:ok, output, ctx} -> compress_chunk_loop(ctx, rest, [output | acc])
      {:error, _} = error -> error
    end
  end

  defp maybe_finish(outputs, ctx, true) do
    case finish_compression(ctx, <<>>) do
      {:ok, tail, _ctx} -> {:ok, Enum.reverse(outputs, tail), ctx}
      {:error, _} = error -> error
    end
  end

  defp maybe_finish(outputs, ctx, false), do: {:ok, Enum.reverse(outputs), ctx}

  @spec new_decompression_context() :: {:ok, context()} | {:error, term()}
  def new_decompression_context do
    case :ezstd.create_decompression_context(@zstd_buffer) do
      ctx when is_reference(ctx) -> {:ok, ctx}
      {:error, _} = error -> error
    end
  end

  @spec decompress_chunk(context(), binary()) ::
          {:ok, iodata(), context()} | {:error, term()}
  def decompress_chunk(ctx, chunk) when is_binary(chunk) do
    case :ezstd.decompress_streaming(ctx, chunk) do
      output when is_list(output) -> {:ok, output, ctx}
      {:error, _} = error -> error
    end
  end

  @spec decompress_chunks(context(), [binary()]) :: {:ok, binary()} | {:error, term()}
  def decompress_chunks(ctx, chunks) do
    Enum.reduce_while(chunks, {:ok, ctx, []}, fn chunk, {:ok, ctx, acc} ->
      case decompress_chunk(ctx, chunk) do
        {:ok, output, ctx} -> {:cont, {:ok, ctx, [output | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, _ctx, outputs} -> {:ok, outputs |> Enum.reverse() |> IO.iodata_to_binary()}
      {:error, _} = error -> error
    end
  end
end
