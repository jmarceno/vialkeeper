defmodule VialKeeper.Attachments.FilesystemStoreTest do
  @moduledoc "Covers filesystem attachment storage and database bundle cleanup."

  use ExUnit.Case, async: true

  alias VialKeeper.Attachments.Compression
  alias VialKeeper.Attachments.FilesystemStore
  alias VialKeeper.Attachments.Representation
  alias VialKeeper.DatabaseBundle

  @moduletag :attachments

  setup do
    root = unique_tmp_path("vialkeeper-store")

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

  test "buffered finish becomes durable after sync_digests", %{bundle: bundle} do
    payload = :crypto.strong_rand_bytes(2048)

    assert {:ok, writer} =
             FilesystemStore.begin_put(bundle.root, byte_size(payload) + 1, %{durable: false})

    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:ok, %{digest: digest, logical_size: size, deduplicated?: false}} =
             FilesystemStore.finish_put(writer)

    assert FilesystemStore.exists?(bundle.root, digest)
    assert :ok = FilesystemStore.sync_digests(bundle.root, [digest])
    assert :ok = FilesystemStore.sync_digests(bundle.root, [])
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

    assert {:error, %VialKeeper.Error{code: :payload_too_large}} =
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
    path = Path.join([bundle.root, "blobs", prefix, digest <> ".blob"])
    File.write!(path, "corrupted")

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             put_whole(bundle.root, payload)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "failed install removes the private temporary upload", %{bundle: bundle} do
    payload = "failed-install"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle.root, "blobs", prefix])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, digest <> ".blob"), "corrupt")

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, byte_size(payload), %{})
    tmp_path = FilesystemStore.writer_tmp_path(writer)
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.finish_put(writer)

    refute File.exists?(tmp_path)
  end

  test "blob prefix symlinks cannot redirect installation", %{bundle: bundle} do
    payload = "symlinked-prefix"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    prefix = String.slice(digest, 0, 2)
    outside = unique_tmp_path("vialkeeper-outside")
    File.mkdir_p!(outside)
    File.ln_s!(outside, Path.join([bundle.root, "blobs", prefix]))

    on_exit(fn -> File.rm_rf(outside) end)

    assert {:ok, writer} = FilesystemStore.begin_put(bundle.root, byte_size(payload), %{})
    assert :ok = FilesystemStore.write_chunk(writer, payload)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
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
    assert String.ends_with?(path, ".blob")
  end

  test "malformed digest cannot escape blob root", %{bundle: bundle} do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             FilesystemStore.open_read(bundle.root, "../../../etc/passwd")

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
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

  test "raw representation reuses the logical digest for the payload digest", %{bundle: bundle} do
    payload = <<0xFF, 0xD8, 0xFF, :crypto.strong_rand_bytes(32_768)::binary>>
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    assert {:ok, %{digest: ^digest, encoding: :raw}} = put_whole(bundle.root, payload)

    representation = File.read!(blob_path_for(bundle.root, digest))
    trailer_offset = byte_size(representation) - Representation.trailer_size()
    trailer = binary_part(representation, trailer_offset, Representation.trailer_size())

    assert {:ok, descriptor} = Representation.parse_trailer(trailer)
    assert descriptor.logical_digest == digest
    assert descriptor.payload_sha256 == digest
    assert descriptor.logical_length == byte_size(payload)
    assert descriptor.payload_length == byte_size(payload)
    assert {:ok, reader} = FilesystemStore.open_read(bundle.root, digest)
    assert collect_reader(reader) == payload
  end

  test "known compressed container signatures select raw representation", %{bundle: bundle} do
    signatures = [
      <<0xFF, 0xD8, 0xFF, 0>>,
      <<0x89, "PNG\r\n", 0x1A, "\n", 0>>,
      <<"GIF89a", 0>>,
      <<"PK", 3, 4, 0>>,
      <<0x1F, 0x8B, 0>>,
      <<0x28, 0xB5, 0x2F, 0xFD, 0>>,
      <<0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0>>,
      <<"Rar!", 0x1A, 0x07, 0>>,
      <<"%PDF-1.7", 0>>,
      <<"RIFF", 0, 0, 0, 0, "WEBP", 0>>,
      <<0, 0, 0, 24, "ftyp", "isom">>
    ]

    Enum.each(signatures, fn signature ->
      assert Compression.already_compressed?(signature)
      payload = signature <> :crypto.strong_rand_bytes(300_000)
      assert {:ok, %{encoding: :raw}} = put_whole(bundle.root, payload)
    end)

    refute Compression.already_compressed?("plain text")
  end

  @tag :compressed
  test "compressed read returns exact original bytes when worthwhile", %{bundle: bundle} do
    payload = compressible_payload()
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    assert {:ok, %{encoding: encoding}} = FilesystemStore.stat(bundle.root, digest)
    assert encoding == :zstd

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

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "corruption is detected on verify", %{bundle: bundle} do
    payload = "integrity-check"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)

    path = blob_path_for(bundle.root, digest)
    File.write!(path, "corrupt")

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "non-blob files are ignored and only digest.blob is accepted", %{bundle: bundle} do
    digest = String.duplicate("a", 64)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle.root, "blobs", prefix])
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, digest <> ".raw"), "raw")
    File.write!(Path.join(dir, digest <> ".zst"), "zst")
    File.write!(Path.join(dir, "not-a-digest.blob"), "x")

    refute FilesystemStore.exists?(bundle.root, digest)

    assert {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} =
             FilesystemStore.stat(bundle.root, digest)

    assert {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} =
             FilesystemStore.verify(bundle.root, digest, 3)

    assert {:ok, []} = FilesystemStore.list_digests(bundle.root)
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

  test "malformed blob is integrity_violation not not-found", %{bundle: bundle} do
    payload = "malformed-stat"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)
    path = blob_path_for(bundle.root, digest)
    File.write!(path, "too-short")

    assert FilesystemStore.exists?(bundle.root, digest)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.stat(bundle.root, digest)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "payload bit flip is detected physically", %{bundle: bundle} do
    payload = "payload-bit-flip"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)
    path = blob_path_for(bundle.root, digest)
    bytes = File.read!(path)

    flipped =
      <<:erlang.bxor(:binary.at(bytes, 0), 1), binary_part(bytes, 1, byte_size(bytes) - 1)::binary>>

    File.write!(path, flipped)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             put_whole(bundle.root, payload)
  end

  test "trailer bit flip is detected structurally", %{bundle: bundle} do
    payload = "trailer-bit-flip"
    assert {:ok, %{digest: digest, logical_size: size}} = put_whole(bundle.root, payload)
    path = blob_path_for(bundle.root, digest)
    bytes = File.read!(path)
    offset = byte_size(bytes) - Representation.trailer_size()
    <<prefix::binary-size(^offset), byte, rest::binary>> = bytes
    File.write!(path, prefix <> <<:erlang.bxor(byte, 1)>> <> rest)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.stat(bundle.root, digest)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, digest, size)
  end

  test "file shorter than trailer is rejected", %{bundle: bundle} do
    digest = :crypto.hash(:sha256, "short") |> Base.encode16(case: :lower)
    dir = Path.join([bundle.root, "blobs", String.slice(digest, 0, 2)])
    File.mkdir_p!(dir)
    File.write!(blob_path_for(bundle.root, digest), :binary.copy(<<1>>, 40))

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.stat(bundle.root, digest)
  end

  test "representation reader returns encoded payload without decompressing", %{bundle: bundle} do
    payload = compressible_payload()
    assert {:ok, %{digest: digest, encoding: :zstd}} = put_whole(bundle.root, payload)
    path = blob_path_for(bundle.root, digest)
    file = File.read!(path)
    stored_payload = binary_part(file, 0, byte_size(file) - Representation.trailer_size())

    assert {:ok, {descriptor, reader}} =
             FilesystemStore.open_representation_read(bundle.root, digest)

    assert descriptor.encoding == :zstd
    assert collect_representation(reader) == stored_payload
    refute collect_representation_reopen(bundle.root, digest) == payload
  end

  test "representation installer writes canonical trailer and is idempotent", %{bundle: bundle} do
    payload = "rep-install"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: byte_size(payload),
      payload_length: byte_size(payload),
      payload_sha256: digest
    }

    assert {:ok, writer} = FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)
    assert :ok = FilesystemStore.write_representation_chunk(writer, payload)
    assert {:ok, first} = FilesystemStore.finish_put_representation(writer)
    assert first.encoding == :raw
    assert first.digest == digest
    refute first.deduplicated?

    assert {:ok, writer} = FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)
    assert :ok = FilesystemStore.write_representation_chunk(writer, payload)
    assert {:ok, second} = FilesystemStore.finish_put_representation(writer)
    assert second.deduplicated?

    assert {:ok, reader} = FilesystemStore.open_read(bundle.root, digest)
    assert collect_reader(reader) == payload
  end

  test "representation installer rejects truncated payload and digest mismatch", %{bundle: bundle} do
    payload = "full-payload"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: byte_size(payload),
      payload_length: byte_size(payload),
      payload_sha256: digest
    }

    assert {:ok, writer} = FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)
    assert :ok = FilesystemStore.write_representation_chunk(writer, "short")

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.finish_put_representation(writer)

    wrong = :crypto.hash(:sha256, "other") |> Base.encode16(case: :lower)

    wrong_descriptor = %{
      encoding: :raw,
      logical_digest: wrong,
      logical_length: byte_size(payload),
      payload_length: byte_size(payload),
      payload_sha256: wrong
    }

    assert {:ok, writer} =
             FilesystemStore.begin_put_representation(bundle.root, wrong_descriptor, 1_000)

    assert :ok = FilesystemStore.write_representation_chunk(writer, payload)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.finish_put_representation(writer)
  end

  test "representation installer rejects bytes beyond payload_length", %{bundle: bundle} do
    payload = "abc"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: 3,
      payload_length: 3,
      payload_sha256: digest
    }

    assert {:ok, writer} = FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)
    tmp_path = FilesystemStore.writer_tmp_path(writer)

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.write_representation_chunk(writer, "abcd")

    refute File.exists?(tmp_path)
  end

  test "representation installer rejects logical length above the maximum", %{bundle: bundle} do
    digest = :crypto.hash(:sha256, "x") |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: 10,
      payload_length: 10,
      payload_sha256: digest
    }

    assert {:error, %VialKeeper.Error{code: :payload_too_large}} =
             FilesystemStore.begin_put_representation(bundle.root, descriptor, 4)
  end

  test "abort_put_representation removes the temporary upload", %{bundle: bundle} do
    digest = :crypto.hash(:sha256, "abort") |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: 5,
      payload_length: 5,
      payload_sha256: digest
    }

    assert {:ok, writer} = FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)
    tmp_path = FilesystemStore.writer_tmp_path(writer)
    assert File.regular?(tmp_path)
    :ok = FilesystemStore.abort_put_representation(writer)
    refute File.exists?(tmp_path)
  end

  test "concurrent identical representation install leaves one valid blob", %{bundle: bundle} do
    payload = "same-rep"
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: byte_size(payload),
      payload_length: byte_size(payload),
      payload_sha256: digest
    }

    parent = self()

    workers =
      for _ <- 1..8 do
        Task.async(fn ->
          assert {:ok, writer} =
                   FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000)

          assert :ok = FilesystemStore.write_representation_chunk(writer, payload)
          send(parent, {:ready_to_install, self()})

          receive do
            :install -> FilesystemStore.finish_put_representation(writer)
          end
        end)
      end

    for _ <- workers, do: assert_receive({:ready_to_install, _pid}, 1_000)
    Enum.each(workers, &send(&1.pid, :install))

    results =
      Enum.map(workers, fn worker ->
        case Task.await(worker, 5_000) do
          {:ok, result} -> result
          {:error, reason} -> flunk("concurrent representation install failed: #{inspect(reason)}")
        end
      end)

    assert Enum.uniq(Enum.map(results, & &1.digest)) == [digest]
    assert FilesystemStore.verify(bundle.root, digest, byte_size(payload)) == :ok
    assert {:ok, [^digest]} = FilesystemStore.list_digests(bundle.root)
  end

  test "concurrent different representations keep the first valid file", %{bundle: bundle} do
    logical = "shared-logical"
    digest = :crypto.hash(:sha256, logical) |> Base.encode16(case: :lower)
    other = "other-payload-bytes"
    other_digest = :crypto.hash(:sha256, other) |> Base.encode16(case: :lower)

    raw_descriptor = %{
      encoding: :raw,
      logical_digest: digest,
      logical_length: byte_size(logical),
      payload_length: byte_size(logical),
      payload_sha256: digest
    }

    zstd_descriptor = %{
      encoding: :zstd,
      logical_digest: digest,
      logical_length: byte_size(logical),
      payload_length: byte_size(other),
      payload_sha256: other_digest
    }

    parent = self()

    raw_task =
      Task.async(fn ->
        assert {:ok, writer} =
                 FilesystemStore.begin_put_representation(bundle.root, raw_descriptor, 1_000)

        assert :ok = FilesystemStore.write_representation_chunk(writer, logical)
        send(parent, {:ready, self()})

        receive do
          :install -> FilesystemStore.finish_put_representation(writer)
        end
      end)

    zstd_task =
      Task.async(fn ->
        assert {:ok, writer} =
                 FilesystemStore.begin_put_representation(bundle.root, zstd_descriptor, 1_000)

        assert :ok = FilesystemStore.write_representation_chunk(writer, other)
        send(parent, {:ready, self()})

        receive do
          :install -> FilesystemStore.finish_put_representation(writer)
        end
      end)

    assert_receive {:ready, _}, 1_000
    assert_receive {:ready, _}, 1_000

    # Both writers hold open temporary files; installs are ordered so "first"
    # is deterministic and the loser's dedup path is actually exercised.
    send(raw_task.pid, :install)
    assert {:ok, raw_result} = Task.await(raw_task, 5_000)
    refute raw_result.deduplicated?

    path = blob_path_for(bundle.root, digest)
    installed_bytes = File.read!(path)

    send(zstd_task.pid, :install)
    assert {:ok, zstd_result} = Task.await(zstd_task, 5_000)
    assert zstd_result.deduplicated?

    assert File.read!(path) == installed_bytes

    assert {:ok, {descriptor, reader}} =
             FilesystemStore.open_representation_read(bundle.root, digest)

    payload = collect_representation(reader)
    assert descriptor.encoding == :raw
    assert payload == logical
    assert byte_size(installed_bytes) == descriptor.payload_length + Representation.trailer_size()
    assert FilesystemStore.verify(bundle.root, digest, byte_size(logical)) == :ok
  end

  @tag :compressed
  test "verify detects a valid physical payload with the wrong logical digest", %{bundle: bundle} do
    logical = compressible_payload()
    lying_digest = :crypto.hash(:sha256, "not-the-logical-bytes") |> Base.encode16(case: :lower)
    assert {:ok, ctx} = Compression.new_compression_context()
    assert :ok = Compression.set_level(ctx)
    assert {:ok, compressed} = Compression.compress_chunks(ctx, [logical], true)
    payload_digest = :crypto.hash(:sha256, compressed) |> Base.encode16(case: :lower)

    descriptor = %{
      encoding: :zstd,
      logical_digest: lying_digest,
      logical_length: byte_size(logical),
      payload_length: byte_size(compressed),
      payload_sha256: payload_digest
    }

    assert {:ok, writer} =
             FilesystemStore.begin_put_representation(bundle.root, descriptor, 1_000_000)

    assert :ok = FilesystemStore.write_representation_chunk(writer, compressed)
    assert {:ok, _} = FilesystemStore.finish_put_representation(writer)

    assert {:ok, {stored, reader}} =
             FilesystemStore.open_representation_read(bundle.root, lying_digest)

    assert stored.payload_sha256 == payload_digest
    assert collect_representation(reader) == compressed

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             FilesystemStore.verify(bundle.root, lying_digest, byte_size(logical))
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

  defp collect_representation_reopen(bundle_root, digest) do
    assert {:ok, {_descriptor, reader}} =
             FilesystemStore.open_representation_read(bundle_root, digest)

    collect_representation(reader)
  end

  defp collect_representation(reader) do
    collect_representation_loop(reader, <<>>)
  end

  defp collect_representation_loop(reader, acc) do
    case FilesystemStore.read_representation_chunks(reader) do
      {:ok, chunk} ->
        collect_representation_loop(reader, acc <> chunk)

      {:done, _} ->
        FilesystemStore.close_representation_read(reader)
        acc

      {:error, reason} ->
        flunk("representation read failed: #{inspect(reason)}")
    end
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
    Path.join([bundle_root, "blobs", String.slice(digest, 0, 2), digest <> ".blob"])
  end

  defp compressible_payload do
    :binary.copy(<<0>>, 300 * 1024)
  end

  defp unique_tmp_path(prefix) do
    suffix = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    Path.join(System.tmp_dir!(), "#{prefix}-#{suffix}")
  end
end
