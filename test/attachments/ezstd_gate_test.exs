defmodule ElixirDB.Attachments.EzstdGateTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Attachments.Compression

  @chunk 4 * 1024
  @large_chunks 2_048

  test "incremental compression and decompression across multiple chunks" do
    input = for _ <- 1..32, do: :binary.copy(<<0>>, @chunk)
    expected = IO.iodata_to_binary(input)

    assert {:ok, ctx} = Compression.new_compression_context()
    assert :ok = Compression.set_level(ctx)

    {ctx, compressed_parts} =
      Enum.reduce(input, {ctx, []}, fn chunk, {ctx, acc} ->
        assert {:ok, output, ctx} = Compression.compress_chunk(ctx, chunk)
        {ctx, [output | acc]}
      end)

    assert {:ok, final, _ctx} = Compression.finish_compression(ctx, <<>>)
    compressed_parts = Enum.reverse(compressed_parts, [final])
    assert [_ | _] = compressed_parts

    compressed = IO.iodata_to_binary(compressed_parts)
    assert byte_size(compressed) < byte_size(expected)

    assert {:ok, dctx} = Compression.new_decompression_context()

    {decoded_parts, {_ctx, _}} =
      Enum.map_reduce(compressed_parts, {dctx, []}, fn part, {ctx, acc} ->
        assert {:ok, output, ctx} = Compression.decompress_chunk(ctx, IO.iodata_to_binary(part))
        {output, {ctx, [output | acc]}}
      end)

    assert [_ | _] = decoded_parts
    assert IO.iodata_to_binary(Enum.reverse(decoded_parts)) == expected
  end

  test "bounded memory for substantially larger-than-chunk payload without whole-file concat" do
    peak = :atomics.new(1, signed: false)

    compress_input =
      Stream.repeatedly(fn -> :crypto.strong_rand_bytes(@chunk) end)
      |> Enum.take(@large_chunks)

    assert {:ok, ctx} = Compression.new_compression_context()
    assert :ok = Compression.set_level(ctx)

    {ctx, outputs} =
      Enum.reduce(compress_input, {ctx, []}, fn chunk, {ctx, acc} ->
        track_peak(peak, chunk)
        assert {:ok, output, ctx} = Compression.compress_chunk(ctx, chunk)
        track_peak(peak, output)
        {ctx, [output | acc]}
      end)

    assert {:ok, tail, _ctx} = Compression.finish_compression(ctx, <<>>)
    track_peak(peak, tail)
    compressed_bin = outputs |> Enum.reverse([tail]) |> IO.iodata_to_binary()

    assert {:ok, dctx} = Compression.new_decompression_context()

    decoded =
      chunk_binary(compressed_bin, @chunk)
      |> Enum.reduce({dctx, []}, fn chunk, {ctx, acc} ->
        track_peak(peak, chunk)
        assert {:ok, output, ctx} = Compression.decompress_chunk(ctx, chunk)
        track_peak(peak, output)
        {ctx, [output | acc]}
      end)
      |> then(fn {_ctx, parts} -> IO.iodata_to_binary(Enum.reverse(parts)) end)

    expected = IO.iodata_to_binary(compress_input)
    assert byte_size(expected) > 4 * @chunk
    assert decoded == expected
    assert :atomics.get(peak, 1) <= 2 * @chunk
  end

  test "clean abort on truncated and corrupt compressed data" do
    assert {:ok, ctx} = Compression.new_compression_context()
    assert :ok = Compression.set_level(ctx)
    assert {:ok, compressed} = Compression.compress_chunks(ctx, [:binary.copy(<<0>>, 8_192)], true)

    truncated = binary_part(compressed, 0, div(byte_size(compressed), 2))
    assert match?({:error, _}, :ezstd.decompress(truncated))

    corrupt = <<0, 1, 2, 3, 4>>
    assert match?({:error, _}, :ezstd.decompress(corrupt))
  end

  defp chunk_binary(binary, size) do
    do_chunk_binary(binary, size, [])
  end

  defp do_chunk_binary(<<>>, _size, acc), do: Enum.reverse(acc)

  defp do_chunk_binary(rest, size, acc) when byte_size(rest) <= size do
    Enum.reverse([rest | acc])
  end

  defp do_chunk_binary(rest, size, acc) do
    part = binary_part(rest, 0, size)
    tail = binary_part(rest, size, byte_size(rest) - size)
    do_chunk_binary(tail, size, [part | acc])
  end

  defp track_peak(peak, data) do
    data_size =
      case data do
        bin when is_binary(bin) -> byte_size(bin)
        iodata -> IO.iodata_length(iodata)
      end

    current = :atomics.get(peak, 1)
    if data_size > current, do: :atomics.put(peak, 1, data_size)
  end
end
