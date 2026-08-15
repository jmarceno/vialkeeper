defmodule VialKeeper.Bench.Gzip do
  @moduledoc "Line streaming for gzip-compressed inventory files."

  @spec stream_lines(Path.t()) :: Enumerable.t()
  def stream_lines(path) when is_binary(path) do
    Stream.resource(
      fn -> File.open!(path, [:read, :binary, :compressed]) end,
      fn io ->
        case IO.read(io, :line) do
          :eof -> {:halt, io}
          {:error, reason} -> raise "failed to read #{path}: #{inspect(reason)}"
          line -> {[line], io}
        end
      end,
      &File.close/1
    )
  end
end
