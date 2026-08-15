defmodule VialKeeper.Bench.IO do
  @moduledoc "Streaming file reads for attachment ingest without whole-file binaries."

  @chunk 65_536

  @spec file_chunks(Path.t()) :: Enumerable.t()
  def file_chunks(path) when is_binary(path) do
    Stream.resource(
      fn -> File.open!(path, [:read, :binary, :raw]) end,
      fn io ->
        case Elixir.IO.binread(io, @chunk) do
          :eof -> {:halt, io}
          {:error, reason} -> raise "failed to read #{path}: #{inspect(reason)}"
          data -> {[data], io}
        end
      end,
      &File.close/1
    )
  end

  @spec consume_stream(Enumerable.t()) :: non_neg_integer()
  def consume_stream(stream) do
    Enum.reduce(stream, 0, fn chunk, acc -> acc + byte_size(chunk) end)
  end
end
