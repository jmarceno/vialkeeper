defmodule ElixirDB.Attachments.FilesystemStoreTest do
  @moduledoc "Covers filesystem attachment storage and database bundle cleanup."

  use ExUnit.Case, async: true

  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.DatabaseBundle

  @moduletag :attachments

  setup do
    root = unique_tmp_path("elixirdb-store")

    on_exit(fn -> File.rm_rf(root) end)

    assert {:ok, bundle} = DatabaseBundle.create(root)
    File.write!(Path.join(DatabaseBundle.root(bundle), "database.sqlite3"), "sqlite")

    %{bundle: bundle, root: bundle.root}
  end

  test "incremental hash over many chunks", %{bundle: bundle} do
    chunks = for i <- 1..64, do: <<i::8, i::8, i::8, i::8>>
    payload = IO.iodata_to_binary(chunks)
    expected_digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, 1_000_000, %{})
    Enum.each(chunks, fn chunk -> :ok = FilesystemStore.write_chunk(writer, chunk) end)
    assert {:ok, %{digest: digest, logical_size: size}} = FilesystemStore.finish_put(writer)

    assert digest == expected_digest
    assert size == byte_size(payload)
    assert FilesystemStore.verify(bundle.root, digest, size) == :ok
  end

  test "accepts exact max size and rejects max plus one", %{bundle: bundle} do
    max = 1_024
    exact = :binary.copy(<<1>>, max)
    over = <<exact::binary, 0>>

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, max, %{})
    assert :ok = FilesystemStore.write_chunk(writer, exact)
    assert {:ok, _} = FilesystemStore.finish_put(writer)

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, max, %{})

    assert {:error, %ElixirDB.Error{code: :payload_too_large}} =
             FilesystemStore.write_chunk(writer, over)
  end

  test "chunk boundary independence produces identical digest", %{bundle: bundle} do
    payload = :crypto.strong_rand_bytes(8_192)

    {:ok, %{digest: one}} = put_chunks(bundle.root, chunk_binary(payload, 17))
    {:ok, %{digest: two}} = put_chunks(bundle.root, chunk_binary(payload, 503))

    assert one == two
    assert FilesystemStore.exists?(bundle.root, one)
  end

  test "dedup leaves one final blob", %{bundle: bundle} do
    payload = compressible_payload()

    assert {:ok, first} = put_whole(bundle.root, payload)
    assert {:ok, second} = put_whole(bundle.root, payload)

    assert first.digest == second.digest
    assert {:ok, digests} = FilesystemStore.list_digests(bundle.root)
    assert digests == [first.digest]

    prefix = String.slice(first.digest, 0, 2)
    blob_dir = Path.join([bundle.root, "blobs", prefix])
    entries = File.ls!(blob_dir)
    assert [_single] = entries
  end

  test "dedup rejects corrupted existing blob instead of silently reusing", %{bundle: bundle} do
    payload = "validate-existing"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    prefix = String.slice(digest, 0, 2)
    path = Path.join([bundle.root, "blobs", prefix, digest <> ".raw"])
    File.write!(path, "corrupted")

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             put_whole(bundle.root, payload)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "failed install removes the private temporary upload", %{bundle: bundle} do
    payload = "failed-install"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle.root, "blobs", prefix])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, digest <> ".raw"), "corrupt")

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, byte_size(payload), %{})
    tmp_path = FilesystemStore.writer_tmp_path(writer)
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.finish_put(writer)

    refute File.exists?(tmp_path)
  end

  test "blob prefix symlinks cannot redirect installation", %{bundle: bundle} do
    payload = "symlinked-prefix"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    prefix = String.slice(digest, 0, 2)
    outside = unique_tmp_path("elixirdb-outside")
    File.mkdir_p!(outside)
    File.ln_s!(outside, Path.join([bundle.root, "blobs", prefix]))

    on_exit(fn -> File.rm_rf(outside) end)

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, byte_size(payload), %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.finish_put(writer)

    assert File.ls!(outside) == []
  end

  test "concurrent same-digest install leaves one valid blob", %{bundle: bundle} do
    payload = compressible_payload()
    parent = self()

    workers =
      for _ <- 1..8 do
        Task.async(fn ->
          assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, byte_size(payload) + 1, %{})
          assert :ok = FilesystemStore.write_chunk(writer, payload)
          send(parent, {:ready_to_install, self()})

          receive do
            :install -> FilesystemStore.finish_put(writer)
          end
        end)
      end

    for _ <- workers, do: assert_receive({:ready_to_install, _pid}, 1_000)
    Enum.each(workers, &send(&1.pid, :install))

    results =
      Enum.map(workers, fn worker ->
        case Task.await(worker, 5_000) do
          {:ok, result} -> result
          {:error, reason} -> flunk("concurrent install failed: #{inspect(reason)}")
        end
      end)

    digests = Enum.map(results, & &1.digest)
    assert Enum.uniq(digests) == [hd(digests)]
    assert FilesystemStore.verify(bundle.root, hd(digests), byte_size(payload)) == :ok

    assert {:ok, listed} = FilesystemStore.list_digests(bundle.root)
    assert listed == [hd(digests)]
  end

  test "user attachment names never affect blob paths", %{bundle: bundle} do
    payload = "diagram-bytes"
    assert {:ok, %{digest: digest}} = put_whole(bundle.root, payload)

    path = blob_path_for(bundle.root, digest)
    refute String.contains?(path, "diagram")
    refute String.contains?(path, "svg")
    assert String.ends_with?(path, ".raw") or String.ends_with?(path, ".zst")
  end

  test "malformed digest cannot escape blob root", %{bundle: bundle} do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             FilesystemStore.open_read(bundle.root, "../../../etc/passwd")

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             FilesystemStore.delete(bundle.root, String.duplicate("G", 64))

    refute File.exists?(Path.join([bundle.root, "blobs", "et"]))
  end

  test "abort removes temp file", %{bundle: bundle} do
    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, 1_000, %{})
    tmp_path = FilesystemStore.writer_tmp_path(writer)
    assert File.regular?(tmp_path)
    :ok = FilesystemStore.abort_put(writer)
    refute File.exists?(tmp_path)
  end

  test "raw read returns exact bytes", %{bundle: bundle} do
    payload = :crypto.strong_rand_bytes(4_096)
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    assert {:ok, %{encoding: :raw}} = FilesystemStore.stat(bundle.root, digest)
    assert {:ok, reader} = FilesystemStore.open_read(bundle.root, digest)
    assert collect_reader(reader) == payload
    assert size == byte_size(payload)
  end

  @tag :compressed
  test "compressed read returns exact original bytes when worthwhile", %{bundle: bundle} do
    payload = compressible_payload()
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    assert {:ok, %{encoding: encoding}} = FilesystemStore.stat(bundle.root, digest)
    assert encoding == :compressed

    assert {:ok, reader} = FilesystemStore.open_read(bundle.root, digest)
    assert collect_reader(reader) == payload
    assert size == byte_size(payload)
  end

  @tag :compressed
  test "truncated compressed blobs fail integrity verification", %{bundle: bundle} do
    payload = compressible_payload()
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    path = blob_path_for(bundle.root, digest)
    compressed = File.read!(path)
    File.write!(path, binary_part(compressed, 0, byte_size(compressed) - 1))

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "corruption is detected on verify", %{bundle: bundle} do
    payload = "integrity-check"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    path = blob_path_for(bundle.root, digest)
    File.write!(path, "corrupt")

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "both raw and zst representations are integrity violations", %{bundle: bundle} do
    digest = String.duplicate("a", 64)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle.root, "blobs", prefix])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, digest <> ".raw"), "raw")
    File.write!(Path.join(dir, digest <> ".zst"), "zst")

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, 3)
  end

  test "large stream memory remains bounded during read and write", %{bundle: bundle} do
    chunk = 4 * 1024
    chunk_count = 2_048
    peak = :atomics.new(1, signed: false)
    baseline = process_memory()

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, chunk * chunk_count + 1, %{})

    {expected_hash, expected_size} =
      Enum.reduce(1..chunk_count, {:crypto.hash_init(:sha256), 0}, fn index, {hash, size} ->
        part = stream_chunk(index, chunk)
        :ok = FilesystemStore.write_chunk(writer, part)
        track_process_memory(peak)
        {:crypto.hash_update(hash, part), size + byte_size(part)}
      end)

    expected_digest = :crypto.hash_final(expected_hash) |> Base.encode16(case: :lower)

    assert {:ok, %{digest: digest, logical_size: ^expected_size}} =
             FilesystemStore.finish_put(writer)

    assert digest == expected_digest
    assert :atomics.get(peak, 1) <= baseline + 2 * 1024 * 1024

    assert {:ok, reader} = FilesystemStore.open_read(bundle.root, digest)

    {actual_hash, actual_size} =
      drain_reader(reader, :crypto.hash_init(:sha256), 0, peak)

    actual_digest = :crypto.hash_final(actual_hash) |> Base.encode16(case: :lower)
    assert actual_digest == expected_digest
    assert actual_size == expected_size
    assert :atomics.get(peak, 1) <= baseline + 2 * 1024 * 1024
  end

  test "cleanup_tmp removes only expired temp files", %{bundle: bundle} do
    old = Path.join([bundle.root, "tmp", "old-upload"])
    new = Path.join([bundle.root, "tmp", "new-upload"])
    File.write!(old, "old")
    File.write!(new, "new")

    old_time = {{2000, 1, 1}, {0, 0, 0}}
    File.touch!(old, old_time)

    cutoff = DateTime.utc_now() |> DateTime.add(-60, :second)
    assert :ok = FilesystemStore.cleanup_tmp(bundle.root, cutoff)

    refute File.exists?(old)
    assert File.exists?(new)
  end

  test "delete removes installed blob", %{bundle: bundle} do
    assert {:ok, %{digest: digest}} = put_whole(bundle.root, "delete-me")
    assert FilesystemStore.exists?(bundle.root, digest)
    assert :ok = FilesystemStore.delete(bundle.root, digest)
    refute FilesystemStore.exists?(bundle.root, digest)
  end

  defp put_whole(bundle_path, payload) do
    with {:ok, writer} <- FilesystemStore.begin_put(bundle_path, byte_size(payload) + 1, %{}),
         :ok <- FilesystemStore.write_chunk(writer, payload) do
      FilesystemStore.finish_put(writer)
    end
  end

  defp put_chunks(bundle_path, chunks) do
    total = Enum.reduce(chunks, 0, fn c, acc -> acc + byte_size(c) end)

    with {:ok, writer} <- FilesystemStore.begin_put(bundle_path, total + 1, %{}),
         :ok <-
           Enum.reduce(chunks, :ok, fn chunk, :ok -> FilesystemStore.write_chunk(writer, chunk) end) do
      FilesystemStore.finish_put(writer)
    end
  end

  defp collect_reader(reader) do
    collect_reader_loop(reader, <<>>)
  end

  defp collect_reader_loop(reader, acc) do
    case FilesystemStore.read_chunks(reader) do
      {:ok, chunk} ->
        collect_reader_loop(reader, acc <> chunk)

      {:done, _} ->
        FilesystemStore.close_read(reader)
        acc

      {:error, reason} ->
        flunk("read failed: #{inspect(reason)}")
    end
  end

  defp drain_reader(reader, hash, size, peak) do
    case FilesystemStore.read_chunks(reader) do
      {:ok, chunk} ->
        track_process_memory(peak)
        drain_reader(reader, :crypto.hash_update(hash, chunk), size + byte_size(chunk), peak)

      {:done, _} ->
        {hash, size}

      {:error, reason} ->
        flunk("read failed: #{inspect(reason)}")
    end
  end

  defp stream_chunk(index, size) do
    seed = :crypto.hash(:sha256, <<index::64>>)
    repeats = div(size, byte_size(seed))
    remainder = rem(size, byte_size(seed))
    :binary.copy(seed, repeats) <> binary_part(seed, 0, remainder)
  end

  defp process_memory do
    :erlang.garbage_collect()
    {:memory, memory} = :erlang.process_info(self(), :memory)
    memory
  end

  defp track_process_memory(peak) do
    memory = process_memory()
    current = :atomics.get(peak, 1)
    if memory > current, do: :atomics.put(peak, 1, memory)
  end

  defp chunk_binary(binary, size) do
    Stream.unfold(binary, fn
      <<>> ->
        nil

      rest when byte_size(rest) <= size ->
        {rest, <<>>}

      rest ->
        part = binary_part(rest, 0, size)
        tail = binary_part(rest, size, byte_size(rest) - size)
        {part, tail}
    end)
    |> Enum.to_list()
  end

  defp blob_path_for(bundle_root, digest) do
    case FilesystemStore.stat(bundle_root, digest) do
      {:ok, %{encoding: :raw}} ->
        Path.join([bundle_root, "blobs", String.slice(digest, 0, 2), digest <> ".raw"])

      {:ok, %{encoding: :compressed}} ->
        Path.join([bundle_root, "blobs", String.slice(digest, 0, 2), digest <> ".zst"])
    end
  end

  defp compressible_payload do
    :binary.copy(<<0>>, 300 * 1024)
  end

  defp unique_tmp_path(prefix) do
    suffix = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    Path.join(System.tmp_dir!(), "#{prefix}-#{suffix}")
  end
end
