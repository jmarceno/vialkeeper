defmodule VialKeeper.Attachments.EzstdGateTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Attachments.Compression

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
    path =
      Path.join(System.tmp_dir!(), "vialkeeper-ezstd-#{System.unique_integer([:positive])}.zst")

    on_exit(fn -> File.rm(path) end)
    peak = :atomics.new(1, signed: false)
    baseline = process_memory()

    assert {:ok, ctx} = Compression.new_compression_context()
    assert :ok = Compression.set_level(ctx)
    {:ok, fd} = File.open(path, [:write, :binary, :raw])

    {ctx, expected_hash, expected_size} =
      Enum.reduce(1..@large_chunks, {ctx, :crypto.hash_init(:sha256), 0}, fn index,
                                                                             {ctx, hash, size} ->
        chunk = generated_chunk(index)
        assert {:ok, output, ctx} = Compression.compress_chunk(ctx, chunk)
        assert :ok = :file.write(fd, output)
        track_process_memory(peak)
        {ctx, :crypto.hash_update(hash, chunk), size + byte_size(chunk)}
      end)

    assert {:ok, tail, _ctx} = Compression.finish_compression(ctx, <<>>)
    assert :ok = :file.write(fd, tail)
    assert :ok = :file.close(fd)

    expected_digest = :crypto.hash_final(expected_hash) |> Base.encode16(case: :lower)
    assert expected_size == @chunk * @large_chunks
    assert File.stat!(path).size < expected_size

    assert {:ok, dctx} = Compression.new_decompression_context()
    {:ok, read_fd} = File.open(path, [:read, :binary, :raw])

    {actual_hash, actual_size} = decode_file(read_fd, dctx, :crypto.hash_init(:sha256), 0, peak)
    assert :ok = :file.close(read_fd)

    actual_digest = :crypto.hash_final(actual_hash) |> Base.encode16(case: :lower)
    assert actual_digest == expected_digest
    assert actual_size == expected_size
    assert :atomics.get(peak, 1) <= baseline + 2 * 1024 * 1024
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

  defp generated_chunk(index) do
    :binary.copy(<<rem(index, 251)>>, @chunk)
  end

  defp decode_file(fd, ctx, hash, size, peak) do
    case :file.read(fd, @chunk) do
      :eof ->
        {hash, size}

      {:ok, compressed} ->
        assert {:ok, output, ctx} = Compression.decompress_chunk(ctx, compressed)
        track_process_memory(peak)

        decode_file(
          fd,
          ctx,
          :crypto.hash_update(hash, output),
          size + IO.iodata_length(output),
          peak
        )
    end
  end

  defp process_memory do
    :erlang.garbage_collect()
    {:memory, memory} = :erlang.process_info(self(), :memory)
    memory
  end

  defp track_process_memory(peak) do
    data_size = process_memory()
    current = :atomics.get(peak, 1)
    if data_size > current, do: :atomics.put(peak, 1, data_size)
  end
end
