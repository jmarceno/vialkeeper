defmodule VialKeeper.Attachments.FilesystemStore do
  @moduledoc """
  Version 1 filesystem attachment store for database bundles.

  Blobs live under `blobs/<prefix>/<digest>.blob` as encoded payload followed
  by a canonical 92-byte trailer. Temporary uploads use random names under
  `tmp/`.
  """

  @behaviour VialKeeper.Attachments.Store

  alias VialKeeper.Attachments.{Compression, StoreRef}
  alias VialKeeper.Attachments.Representation
  alias VialKeeper.DatabaseBundle
  alias VialKeeper.DurableFS
  alias VialKeeper.Error
  alias VialKeeper.Observability.Instrumentation.AttachmentStore
  alias VialKeeper.PathSafety

  @probe_prefix_bytes 256 * 1024
  @read_chunk_size 64 * 1024
  @digest_pattern ~r/^[0-9a-f]{64}$/
  @writer_key {:vialkeeper, :attachment_writer}
  @representation_writer_key {:vialkeeper, :attachment_representation_writer}
  @reader_key {:vialkeeper, :attachment_reader}
  @representation_reader_key {:vialkeeper, :attachment_representation_reader}

  @type writer :: {:writer, reference()}
  @type representation_writer :: {:representation_writer, reference()}
  @type reader :: {:reader, reference()}
  @type representation_reader :: {:representation_reader, reference()}

  @impl true
  def begin_put(store_ref, max_bytes, _options)
      when is_integer(max_bytes) and max_bytes > 0 do
    AttachmentStore.phase(:begin, fn -> do_begin_put(store_ref, max_bytes) end)
  end

  defp do_begin_put(store_ref, max_bytes) do
    with {:ok, ref} <- writable_ref(store_ref),
         {:ok, bundle} <- bundle_for(ref),
         {:ok, tmp_path} <- exclusive_tmp(bundle.tmp_path),
         {:ok, fd} <- File.open(tmp_path, [:write, :binary, :raw]) do
      writer_ref = make_ref()

      put_writer(
        writer_ref,
        %{
          store_ref: ref,
          tmp_path: tmp_path,
          fd: fd,
          hash_ctx: :crypto.hash_init(:sha256),
          payload_hash_ctx: nil,
          logical_size: 0,
          payload_length: 0,
          max_bytes: max_bytes,
          phase: :probing,
          probe_buffer: <<>>,
          compression_ctx: nil
        }
      )

      {:ok, {:writer, writer_ref}}
    end
  end

  @impl true
  def write_chunk({:writer, ref} = writer, chunk) when is_binary(chunk) do
    case update_writer(ref, &write_chunk_state(&1, chunk)) do
      :ok ->
        :ok

      {:error, %Error{} = error} ->
        abort_put(writer)
        {:error, error}

      {:error, reason} ->
        abort_put(writer)
        {:error, Error.internal_error("attachment write failed", %{reason: inspect(reason)})}
    end
  end

  def write_chunk(writer, _chunk) do
    abort_put(writer)
    {:error, Error.internal_error("attachment writer is in an invalid state")}
  end

  @impl true
  def finish_put({:writer, ref} = writer) do
    result =
      with {:ok, state} <- fetch_writer(ref),
           {:ok, state} <- maybe_decide_encoding(state),
           {:ok, state} <-
             AttachmentStore.phase(:compression_finalize, fn -> finalize_physical(state) end),
           {:ok, descriptor} <-
             AttachmentStore.phase(:digest_finalize, fn -> original_descriptor(state) end),
           {:ok, trailer} <- Representation.encode_trailer(descriptor),
           :ok <- AttachmentStore.phase(:trailer_write, fn -> write_trailer(state.fd, trailer) end),
           :ok <- AttachmentStore.phase(:file_sync, fn -> sync_file(state.fd) end),
           :ok <- AttachmentStore.phase(:file_close, fn -> close_fd(state.fd) end),
           {:ok, install_kind} <-
             AttachmentStore.phase(:cas_install, fn ->
               install_blob(state.store_ref, state.tmp_path, descriptor.logical_digest)
             end) do
        {:ok,
         %{
           digest: descriptor.logical_digest,
           logical_size: descriptor.logical_length,
           encoding: descriptor.encoding,
           deduplicated?: install_kind == :deduplicated
         }}
      end

    finish_writer_result(writer, ref, result, "attachment finish failed")
  end

  def finish_put(writer) do
    abort_put(writer)
    {:error, Error.internal_error("attachment writer is in an invalid state")}
  end

  @impl true
  def abort_put({:writer, ref}) do
    discard_writer(ref, @writer_key)
  end

  def abort_put(_), do: :ok

  @impl true
  def begin_put_representation(store_ref, descriptor, max_logical_bytes)
      when is_integer(max_logical_bytes) and max_logical_bytes > 0 do
    with {:ok, ref} <- writable_ref(store_ref),
         {:ok, descriptor} <- Representation.descriptor(descriptor),
         :ok <- enforce_max_logical_bytes(descriptor.logical_length, max_logical_bytes),
         {:ok, bundle} <- bundle_for(ref),
         {:ok, tmp_path} <- exclusive_tmp(bundle.tmp_path),
         {:ok, fd} <- File.open(tmp_path, [:write, :binary, :raw]) do
      writer_ref = make_ref()

      put_keyed(
        @representation_writer_key,
        writer_ref,
        %{
          store_ref: ref,
          tmp_path: tmp_path,
          fd: fd,
          descriptor: descriptor,
          payload_hash_ctx: :crypto.hash_init(:sha256),
          payload_length: 0
        }
      )

      {:ok, {:representation_writer, writer_ref}}
    end
  end

  @impl true
  def write_representation_chunk({:representation_writer, ref} = writer, chunk)
      when is_binary(chunk) do
    case update_keyed(@representation_writer_key, ref, &write_representation_state(&1, chunk)) do
      :ok ->
        :ok

      {:error, %Error{} = error} ->
        abort_put_representation(writer)
        {:error, error}

      {:error, reason} ->
        abort_put_representation(writer)

        {:error,
         Error.internal_error("attachment representation write failed", %{reason: inspect(reason)})}
    end
  end

  def write_representation_chunk(writer, _chunk) do
    abort_put_representation(writer)
    {:error, Error.internal_error("attachment representation writer is in an invalid state")}
  end

  @impl true
  def finish_put_representation({:representation_writer, ref} = writer) do
    result =
      with {:ok, state} <- fetch_keyed(@representation_writer_key, ref),
           :ok <- finalize_representation_payload(state),
           {:ok, trailer} <- Representation.encode_trailer(state.descriptor),
           :ok <- write_trailer(state.fd, trailer),
           :ok <- sync_file(state.fd),
           :ok <- close_fd(state.fd),
           {:ok, install_kind} <-
             install_blob(state.store_ref, state.tmp_path, state.descriptor.logical_digest) do
        {:ok,
         %{
           digest: state.descriptor.logical_digest,
           logical_size: state.descriptor.logical_length,
           encoding: state.descriptor.encoding,
           deduplicated?: install_kind == :deduplicated
         }}
      end

    case result do
      {:ok, _} = ok ->
        drop_keyed(@representation_writer_key, ref)
        ok

      {:error, %Error{} = error} ->
        abort_put_representation(writer)
        {:error, error}

      {:error, reason} ->
        abort_put_representation(writer)

        {:error,
         Error.internal_error("attachment representation finish failed", %{reason: inspect(reason)})}
    end
  end

  def finish_put_representation(writer) do
    abort_put_representation(writer)
    {:error, Error.internal_error("attachment representation writer is in an invalid state")}
  end

  @impl true
  def abort_put_representation({:representation_writer, ref}) do
    discard_writer(ref, @representation_writer_key)
  end

  def abort_put_representation(_), do: :ok

  @doc false
  @spec writer_tmp_path(writer() | representation_writer()) :: binary() | nil
  def writer_tmp_path({:writer, ref}) do
    writer_tmp_from_key(@writer_key, ref)
  end

  def writer_tmp_path({:representation_writer, ref}) do
    writer_tmp_from_key(@representation_writer_key, ref)
  end

  def writer_tmp_path(_), do: nil

  @impl true
  def exists?(store_ref, digest) do
    match?({:ok, _path}, resolve_blob_file(store_ref, digest))
  end

  @impl true
  def stat(store_ref, digest) do
    with {:ok, path} <- resolve_blob_file(store_ref, digest),
         {:ok, descriptor, size} <- read_validated_descriptor(path, digest) do
      {:ok, %{encoding: descriptor.encoding, physical_size: size}}
    end
  end

  @impl true
  def open_read(store_ref, digest) do
    with {:ok, path} <- resolve_blob_file(store_ref, digest),
         {:ok, descriptor, _size} <- read_validated_descriptor(path, digest),
         {:ok, fd, decompress_ctx} <- open_logical_components(path, descriptor) do
      ref = make_ref()

      put_reader(
        ref,
        %{
          fd: fd,
          encoding: descriptor.encoding,
          remaining_payload: descriptor.payload_length,
          decompress_ctx: decompress_ctx,
          pending: <<>>,
          done?: false,
          hash_ctx: :crypto.hash_init(:sha256),
          logical_size: 0,
          expected_size: descriptor.logical_length,
          expected_digest: descriptor.logical_digest
        }
      )

      {:ok, {:reader, ref}}
    else
      {:error, %Error{}} = error ->
        error

      {:error, reason} ->
        {:error, Error.internal_error("attachment open failed", %{reason: inspect(reason)})}
    end
  end

  @impl true
  def read_chunks({:reader, ref}) do
    case fetch_reader(ref) do
      {:ok, %{done?: true}} ->
        {:done, {:reader, ref}}

      {:ok, %{encoding: :raw} = reader} ->
        read_raw_chunk(ref, reader)

      {:ok, %{encoding: :zstd} = reader} ->
        read_compressed_chunk(ref, reader)

      {:error, %Error{}} = error ->
        error
    end
  end

  def read_chunks(_reader) do
    {:error, Error.internal_error("attachment reader is not active")}
  end

  @impl true
  def close_read({:reader, ref}) do
    close_keyed_reader(@reader_key, ref)
  end

  def close_read(_), do: :ok

  @impl true
  def open_representation_read(store_ref, digest) do
    with {:ok, path} <- resolve_blob_file(store_ref, digest),
         {:ok, descriptor, _size} <- read_validated_descriptor(path, digest),
         {:ok, fd} <- File.open(path, [:read, :binary, :raw]) do
      ref = make_ref()

      put_keyed(
        @representation_reader_key,
        ref,
        %{
          fd: fd,
          remaining_payload: descriptor.payload_length,
          done?: false
        }
      )

      {:ok, {descriptor, {:representation_reader, ref}}}
    else
      {:error, %Error{}} = error ->
        error

      {:error, reason} ->
        {:error,
         Error.internal_error("attachment representation open failed", %{reason: inspect(reason)})}
    end
  end

  @impl true
  def read_representation_chunks({:representation_reader, ref}) do
    case fetch_keyed(@representation_reader_key, ref) do
      {:ok, %{done?: true}} ->
        {:done, {:representation_reader, ref}}

      {:ok, reader} ->
        read_representation_payload(ref, reader)

      {:error, %Error{}} = error ->
        error
    end
  end

  def read_representation_chunks(_reader) do
    {:error, Error.internal_error("attachment representation reader is not active")}
  end

  @impl true
  def close_representation_read({:representation_reader, ref}) do
    close_keyed_reader(@representation_reader_key, ref)
  end

  def close_representation_read(_), do: :ok

  @impl true
  def list_digests(store_ref) do
    with {:ok, ref} <- readable_ref(store_ref) do
      digests =
        ref.blobs_path
        |> Path.join("**/*.blob")
        |> Path.wildcard()
        |> Enum.map(&digest_from_blob_path/1)
        |> Enum.filter(&is_binary/1)
        |> Enum.uniq()
        |> Enum.sort()

      {:ok, digests}
    end
  end

  @impl true
  def delete(store_ref, digest) do
    with {:ok, ref} <- writable_ref(store_ref),
         {:ok, path} <- resolve_blob_file(ref, digest) do
      safe_rm(path)
    end
  end

  @impl true
  def verify(store_ref, digest, expected_size)
      when is_integer(expected_size) and expected_size >= 0 do
    with {:ok, path} <- resolve_blob_file(store_ref, digest),
         {:ok, descriptor} <- validate_physical_blob(path, digest),
         :ok <- validate_expected_logical_size(descriptor, expected_size),
         {:ok, reader} <- open_read(store_ref, digest) do
      stream_verify(reader, expected_size, digest)
    end
  end

  @impl true
  def cleanup_tmp(store_ref, %DateTime{} = cutoff) do
    with {:ok, ref} <- writable_ref(store_ref),
         {:ok, bundle} <- bundle_for(ref) do
      cutoff_unix = DateTime.to_unix(cutoff)

      bundle.tmp_path
      |> File.ls()
      |> cleanup_tmp_entries(bundle.tmp_path, cutoff_unix)
    end
  end

  defp finish_writer_result(_writer, ref, {:ok, _} = ok, _message) do
    drop_writer(ref)
    ok
  end

  defp finish_writer_result(writer, _ref, {:error, %Error{} = error}, _message) do
    abort_put(writer)
    {:error, error}
  end

  defp finish_writer_result(writer, _ref, {:error, reason}, message) do
    abort_put(writer)
    {:error, Error.internal_error(message, %{reason: inspect(reason)})}
  end

  defp original_descriptor(state) do
    encoding = encoding_from_phase(state.phase)
    logical_digest = :crypto.hash_final(state.hash_ctx) |> Base.encode16(case: :lower)

    payload_digest =
      case encoding do
        :raw -> logical_digest
        :zstd -> :crypto.hash_final(state.payload_hash_ctx) |> Base.encode16(case: :lower)
      end

    Representation.descriptor(%{
      encoding: encoding,
      logical_digest: logical_digest,
      logical_length: state.logical_size,
      payload_length: state.payload_length,
      payload_sha256: payload_digest
    })
  end

  defp write_trailer(fd, trailer), do: :file.write(fd, trailer)

  defp enforce_max_logical_bytes(logical_length, max_logical_bytes)
       when logical_length <= max_logical_bytes,
       do: :ok

  defp enforce_max_logical_bytes(_logical_length, _max_logical_bytes) do
    {:error, Error.payload_too_large("attachment exceeds max_attachment_bytes")}
  end

  defp write_representation_state(state, chunk) do
    chunk_size = byte_size(chunk)
    new_length = state.payload_length + chunk_size

    cond do
      chunk_size == 0 ->
        {:ok, state}

      new_length > state.descriptor.payload_length ->
        {:error, Error.integrity_violation("attachment representation payload length mismatch")}

      true ->
        case :file.write(state.fd, chunk) do
          :ok ->
            {:ok,
             %{
               state
               | payload_length: new_length,
                 payload_hash_ctx: :crypto.hash_update(state.payload_hash_ctx, chunk)
             }}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp finalize_representation_payload(state) do
    digest = :crypto.hash_final(state.payload_hash_ctx) |> Base.encode16(case: :lower)

    cond do
      state.payload_length != state.descriptor.payload_length ->
        {:error, Error.integrity_violation("attachment representation payload length mismatch")}

      digest != state.descriptor.payload_sha256 ->
        {:error, Error.integrity_violation("attachment representation payload digest mismatch")}

      true ->
        :ok
    end
  end

  defp read_raw_chunk(ref, %{fd: fd, remaining_payload: remaining} = reader) do
    case read_payload_chunk(fd, remaining) do
      :eof ->
        put_reader(ref, %{reader | done?: true, remaining_payload: 0})
        close_read({:reader, ref})
        {:done, {:reader, ref}}

      {:ok, chunk, unread} ->
        put_reader(ref, %{reader | remaining_payload: unread})
        {:ok, chunk}

      {:error, %Error{} = error} ->
        close_read({:reader, ref})
        {:error, error}

      {:error, reason} ->
        close_read({:reader, ref})
        {:error, Error.internal_error("attachment read failed", %{reason: inspect(reason)})}
    end
  end

  defp read_compressed_chunk(ref, reader) do
    case read_decompressed_chunk(reader) do
      {:ok, reader, output} ->
        put_reader(ref, reader)
        return_compressed_chunk(ref, reader, output)

      {:error, %Error{} = error} ->
        close_read({:reader, ref})
        {:error, error}

      {:error, reason} ->
        close_read({:reader, ref})
        {:error, Error.internal_error("attachment decompress failed", %{reason: inspect(reason)})}
    end
  end

  defp return_compressed_chunk(_ref, _reader, output) when output != <<>>, do: {:ok, output}

  defp return_compressed_chunk(ref, %{done?: false}, <<>>),
    do: read_chunks({:reader, ref})

  defp return_compressed_chunk(ref, reader, <<>>) do
    case finish_compressed_reader(reader) do
      :ok ->
        close_read({:reader, ref})
        {:done, {:reader, ref}}

      {:error, %Error{} = error} ->
        close_read({:reader, ref})
        {:error, error}
    end
  end

  defp read_representation_payload(ref, %{fd: fd, remaining_payload: remaining} = reader) do
    case read_payload_chunk(fd, remaining) do
      :eof ->
        put_keyed(@representation_reader_key, ref, %{reader | done?: true, remaining_payload: 0})
        close_representation_read({:representation_reader, ref})
        {:done, {:representation_reader, ref}}

      {:ok, chunk, unread} ->
        put_keyed(@representation_reader_key, ref, %{reader | remaining_payload: unread})
        {:ok, chunk}

      {:error, %Error{} = error} ->
        close_representation_read({:representation_reader, ref})
        {:error, error}

      {:error, reason} ->
        close_representation_read({:representation_reader, ref})

        {:error,
         Error.internal_error("attachment representation read failed", %{reason: inspect(reason)})}
    end
  end

  defp read_payload_chunk(_fd, remaining) when remaining <= 0, do: :eof

  defp read_payload_chunk(fd, remaining) do
    to_read = min(@read_chunk_size, remaining)

    case :file.read(fd, to_read) do
      :eof ->
        {:error, Error.integrity_violation("attachment representation payload is truncated")}

      {:ok, chunk} ->
        {:ok, chunk, remaining - byte_size(chunk)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_reader(ref, state), do: put_keyed(@reader_key, ref, state)

  defp fetch_reader(ref), do: fetch_keyed(@reader_key, ref)

  defp close_keyed_reader(key, ref) do
    case fetch_keyed(key, ref) do
      {:ok, %{fd: fd}} -> close_fd(fd)
      _ -> :ok
    end

    drop_keyed(key, ref)
    :ok
  end

  defp cleanup_tmp_entries({:ok, entries}, tmp_path, cutoff_unix) do
    Enum.each(entries, &cleanup_tmp_entry(tmp_path, &1, cutoff_unix))
    :ok
  end

  defp cleanup_tmp_entries({:error, :enoent}, _tmp_path, _cutoff_unix), do: :ok

  defp cleanup_tmp_entries({:error, reason}, _tmp_path, _cutoff_unix) do
    {:error, Error.internal_error("tmp cleanup failed", %{reason: inspect(reason)})}
  end

  defp cleanup_tmp_entry(tmp_path, entry, cutoff_unix) do
    path = Path.join(tmp_path, entry)

    with true <- File.regular?(path),
         {:ok, mtime} <- file_mtime(path),
         true <- mtime < cutoff_unix do
      File.rm(path)
    else
      _ -> :ok
    end
  end

  defp put_writer(ref, state), do: put_keyed(@writer_key, ref, state)

  defp drop_writer(ref), do: drop_keyed(@writer_key, ref)

  defp fetch_writer(ref), do: fetch_keyed(@writer_key, ref)

  defp update_writer(ref, fun), do: update_keyed(@writer_key, ref, fun)

  defp put_keyed(key, ref, state), do: Process.put({key, ref}, state)

  defp drop_keyed(key, ref), do: Process.delete({key, ref})

  defp fetch_keyed(key, ref) do
    case Process.get({key, ref}) do
      nil -> {:error, Error.internal_error("attachment stream is not active")}
      state -> {:ok, state}
    end
  end

  defp update_keyed(key, ref, fun) do
    with {:ok, state} <- fetch_keyed(key, ref) do
      case fun.(state) do
        {:ok, new_state} ->
          put_keyed(key, ref, new_state)
          :ok

        {:error, _} = error ->
          error
      end
    end
  end

  defp discard_writer(ref, key) do
    state = fetch_keyed(key, ref)
    drop_keyed(key, ref)

    case state do
      {:ok, %{fd: fd, tmp_path: tmp_path}} ->
        close_fd(fd)
        _ = File.rm(tmp_path)
        :ok

      _ ->
        :ok
    end
  end

  defp writer_tmp_from_key(key, ref) do
    case fetch_keyed(key, ref) do
      {:ok, %{tmp_path: tmp_path}} -> tmp_path
      _ -> nil
    end
  end

  defp write_chunk_state(state, chunk) do
    with {:ok, state} <-
           AttachmentStore.phase(:logical_hash, fn -> add_logical_bytes(state, chunk) end) do
      AttachmentStore.phase(:payload_write, fn -> write_original_chunk(state, chunk) end)
    end
  end

  defp add_logical_bytes(%{logical_size: size, max_bytes: max} = state, chunk) do
    chunk_size = byte_size(chunk)
    new_size = size + chunk_size

    if new_size > max do
      {:error, Error.payload_too_large("attachment exceeds max_attachment_bytes")}
    else
      {:ok,
       %{
         state
         | logical_size: new_size,
           hash_ctx: :crypto.hash_update(state.hash_ctx, chunk)
       }}
    end
  end

  defp write_original_chunk(%{phase: :probing} = state, chunk) do
    remaining_probe = @probe_prefix_bytes - byte_size(state.probe_buffer)

    cond do
      remaining_probe <= 0 ->
        with {:ok, state} <- decide_encoding(state) do
          write_payload(state, chunk)
        end

      byte_size(chunk) <= remaining_probe ->
        {:ok, %{state | probe_buffer: state.probe_buffer <> chunk}}

      true ->
        probe_part = binary_part(chunk, 0, remaining_probe)
        rest = binary_part(chunk, remaining_probe, byte_size(chunk) - remaining_probe)

        with {:ok, state} <-
               decide_encoding(%{state | probe_buffer: state.probe_buffer <> probe_part}) do
          write_payload(state, rest)
        end
    end
  end

  defp write_original_chunk(state, chunk), do: write_payload(state, chunk)

  defp write_payload(%{phase: {:writing, :raw}, fd: fd} = state, chunk) do
    append_payload_bytes(state, fd, chunk)
  end

  defp write_payload(%{phase: {:writing, :zstd}, fd: fd, compression_ctx: ctx} = state, chunk) do
    case Compression.compress_chunk(ctx, chunk) do
      {:ok, output, ctx} ->
        case append_payload_bytes(state, fd, output) do
          {:ok, state} -> {:ok, %{state | compression_ctx: ctx}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_payload(_state, _chunk), do: {:error, :invalid_writer_phase}

  defp append_payload_bytes(state, fd, iodata) do
    binary = IO.iodata_to_binary(iodata)

    if binary == <<>> do
      {:ok, state}
    else
      case :file.write(fd, binary) do
        :ok ->
          {:ok,
           %{
             state
             | payload_length: state.payload_length + byte_size(binary),
               payload_hash_ctx: update_payload_hash(state, binary)
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp maybe_decide_encoding(%{phase: :probing} = state), do: decide_encoding(state)
  defp maybe_decide_encoding(state), do: {:ok, state}

  defp decide_encoding(%{probe_buffer: probe} = state) do
    encoding =
      AttachmentStore.phase(:compression_probe, fn -> encoding_for_probe(probe) end)

    with {:ok, state} <- start_writing(state, encoding),
         {:ok, state} <- write_payload(state, probe) do
      {:ok, %{state | probe_buffer: <<>>}}
    end
  end

  defp encoding_for_probe(<<>>), do: :raw

  defp encoding_for_probe(probe) do
    if Compression.already_compressed?(probe) do
      :raw
    else
      case Compression.probe(probe) do
        {:ok, result} -> if Compression.worthwhile?(result), do: :zstd, else: :raw
        {:error, _} -> :raw
      end
    end
  end

  defp start_writing(state, :raw) do
    {:ok, %{state | phase: {:writing, :raw}, compression_ctx: nil, payload_hash_ctx: nil}}
  end

  defp start_writing(state, :zstd) do
    with {:ok, ctx} <- Compression.new_compression_context(),
         :ok <- Compression.set_level(ctx) do
      {:ok,
       %{
         state
         | phase: {:writing, :zstd},
           compression_ctx: ctx,
           payload_hash_ctx: :crypto.hash_init(:sha256)
       }}
    end
  end

  defp update_payload_hash(%{phase: {:writing, :raw}}, _binary), do: nil

  defp update_payload_hash(%{payload_hash_ctx: hash_ctx}, binary),
    do: :crypto.hash_update(hash_ctx, binary)

  defp finalize_physical(%{phase: {:writing, :zstd}, fd: fd, compression_ctx: ctx} = state) do
    with {:ok, output, _ctx} <- Compression.finish_compression(ctx, <<>>) do
      append_payload_bytes(state, fd, output)
    end
  end

  defp finalize_physical(state), do: {:ok, state}

  defp encoding_from_phase({:writing, encoding}), do: encoding
  defp encoding_from_phase(:probing), do: :raw

  defp install_blob(store_ref, tmp_path, digest) do
    case bundle_for(store_ref) do
      {:ok, bundle} ->
        with :ok <- ensure_blob_directory(bundle, digest) do
          install_into_bundle(bundle, tmp_path, digest)
        end

      {:error, _} = error ->
        error
    end
  end

  defp install_into_bundle(bundle, tmp_path, digest) do
    dest = blob_path(bundle.blobs_path, digest)

    case blob_file_kind(dest) do
      :regular ->
        reuse_validated_blob(dest, tmp_path, digest)

      :symlink ->
        _ = File.rm(tmp_path)
        {:error, Error.integrity_violation("attachment blob representation is a symlink")}

      :missing ->
        install_new_blob(bundle, tmp_path, dest, digest)

      {:error, reason} ->
        _ = File.rm(tmp_path)
        {:error, reason}
    end
  end

  defp reuse_validated_blob(dest, tmp_path, digest) do
    result = validate_physical_blob(dest, digest)
    _ = File.rm(tmp_path)

    case result do
      {:ok, _descriptor} -> {:ok, :deduplicated}
      {:error, _} = error -> error
    end
  end

  defp install_new_blob(_bundle, tmp_path, dest, digest) do
    case atomic_install(tmp_path, dest) do
      :ok ->
        case DurableFS.sync_directory(Path.dirname(dest)) do
          :ok -> {:ok, :new}
          {:error, reason} -> {:error, reason}
        end

      {:error, :collision} ->
        _ = File.rm(tmp_path)
        reuse_after_collision(dest, digest)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reuse_after_collision(dest, digest) do
    case blob_file_kind(dest) do
      :regular ->
        case validate_physical_blob(dest, digest) do
          {:ok, _descriptor} -> {:ok, :deduplicated}
          {:error, _} = error -> error
        end

      :symlink ->
        {:error, Error.integrity_violation("attachment blob representation is a symlink")}

      :missing ->
        {:error, Error.attachment_blob_not_found("attachment blob not found")}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp atomic_install(tmp_path, dest) do
    case :file.make_link(tmp_path, dest) do
      :ok ->
        case File.rm(tmp_path) do
          :ok -> :ok
          {:error, :enoent} -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, :eexist} ->
        {:error, :collision}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_verify(reader, expected_size, expected_digest) do
    hash_ctx = :crypto.hash_init(:sha256)
    verify_loop(reader, hash_ctx, 0, expected_size, expected_digest)
  end

  defp verify_loop({:reader, _} = reader, hash_ctx, size, expected_size, expected_digest) do
    case read_chunks(reader) do
      {:ok, chunk} ->
        new_size = size + byte_size(chunk)

        if new_size > expected_size do
          close_read(reader)
          {:error, Error.integrity_violation("attachment size mismatch")}
        else
          verify_loop(
            reader,
            :crypto.hash_update(hash_ctx, chunk),
            new_size,
            expected_size,
            expected_digest
          )
        end

      {:done, _} ->
        digest = :crypto.hash_final(hash_ctx) |> Base.encode16(case: :lower)

        cond do
          size != expected_size ->
            {:error, Error.integrity_violation("attachment size mismatch")}

          digest != expected_digest ->
            {:error, Error.integrity_violation("attachment digest mismatch")}

          true ->
            :ok
        end

      {:error, %Error{}} = error ->
        error
    end
  end

  defp validate_expected_logical_size(%{logical_length: expected_size}, expected_size), do: :ok

  defp validate_expected_logical_size(_descriptor, _expected_size) do
    {:error, Error.integrity_violation("attachment size mismatch")}
  end

  defp read_decompressed_chunk(%{decompress_ctx: ctx, pending: pending} = reader) do
    case take_decompressed_output(ctx, pending) do
      {:ok, output, ctx, pending} when output != <<>> ->
        reader = %{reader | decompress_ctx: ctx, pending: pending}

        case track_decompressed_output(reader, output) do
          {:ok, reader} -> {:ok, reader, output}
          {:error, _} = error -> error
        end

      {:ok, <<>>, ctx, pending} ->
        fill_compressed_input(%{reader | decompress_ctx: ctx, pending: pending})

      {:error, reason} ->
        {:error, Error.integrity_violation("compressed attachment is corrupt: #{inspect(reason)}")}
    end
  end

  defp fill_compressed_input(%{remaining_payload: remaining} = reader) when remaining <= 0 do
    {:ok, %{reader | done?: true, remaining_payload: 0}, <<>>}
  end

  defp fill_compressed_input(%{fd: fd, remaining_payload: remaining} = reader) do
    case read_payload_chunk(fd, remaining) do
      :eof ->
        {:ok, %{reader | done?: true, remaining_payload: 0}, <<>>}

      {:ok, compressed, unread} ->
        read_decompressed_chunk(%{
          reader
          | remaining_payload: unread,
            pending: reader.pending <> compressed
        })

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp track_decompressed_output(
         %{logical_size: size, expected_size: expected_size, hash_ctx: hash_ctx} = reader,
         output
       ) do
    new_size = size + byte_size(output)

    if new_size > expected_size do
      {:error, Error.integrity_violation("compressed attachment size mismatch")}
    else
      {:ok, %{reader | logical_size: new_size, hash_ctx: :crypto.hash_update(hash_ctx, output)}}
    end
  end

  defp finish_compressed_reader(%{
         logical_size: logical_size,
         expected_size: expected_size,
         expected_digest: expected_digest,
         hash_ctx: hash_ctx
       }) do
    digest = :crypto.hash_final(hash_ctx) |> Base.encode16(case: :lower)

    cond do
      logical_size != expected_size ->
        {:error, Error.integrity_violation("compressed attachment size mismatch")}

      digest != expected_digest ->
        {:error, Error.integrity_violation("compressed attachment digest mismatch")}

      true ->
        :ok
    end
  end

  defp take_decompressed_output(ctx, <<>>), do: {:ok, <<>>, ctx, <<>>}

  defp take_decompressed_output(ctx, pending) do
    case Compression.decompress_chunk(ctx, pending) do
      {:ok, output, ctx} ->
        {:ok, IO.iodata_to_binary(output), ctx, <<>>}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp open_logical_components(path, descriptor) do
    with {:ok, fd} <- File.open(path, [:read, :binary, :raw]) do
      case maybe_open_decompressor(descriptor.encoding) do
        {:ok, decompress_ctx} ->
          {:ok, fd, decompress_ctx}

        {:error, reason} ->
          close_fd(fd)
          {:error, reason}
      end
    end
  end

  defp maybe_open_decompressor(:raw), do: {:ok, nil}

  defp maybe_open_decompressor(:zstd), do: Compression.new_decompression_context()

  defp resolve_blob_file(store_ref, digest) do
    with {:ok, digest} <- validate_digest(digest),
         {:ok, ref} <- readable_ref(store_ref),
         :ok <- ensure_blob_path_safe(ref, digest) do
      path = blob_path(ref.blobs_path, digest)

      case blob_file_kind(path) do
        :regular ->
          {:ok, path}

        :symlink ->
          {:error, Error.integrity_violation("attachment blob representation is a symlink")}

        :missing ->
          {:error, Error.attachment_blob_not_found("attachment blob not found")}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp blob_file_kind(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} ->
        :regular

      {:ok, %File.Stat{type: :symlink}} ->
        :symlink

      {:error, :enoent} ->
        :missing

      {:ok, _stat} ->
        {:error, Error.integrity_violation("attachment blob entry has an invalid type")}

      {:error, reason} ->
        {:error, Error.internal_error("cannot inspect attachment blob", %{reason: inspect(reason)})}
    end
  end

  defp read_validated_descriptor(path, digest) do
    with {:ok, %File.Stat{size: size}} <- File.stat(path),
         true <- size >= Representation.trailer_size(),
         {:ok, fd} <- File.open(path, [:read, :binary, :raw]) do
      result = read_trailer_descriptor(fd, size, digest)
      close_fd(fd)
      result
    else
      false ->
        {:error, Error.integrity_violation("attachment representation trailer is invalid")}

      {:error, reason} ->
        {:error, Error.internal_error("attachment open failed", %{reason: inspect(reason)})}
    end
  end

  defp read_trailer_descriptor(fd, size, digest) do
    trailer_size = Representation.trailer_size()

    with {:ok, trailer} <- :file.pread(fd, size - trailer_size, trailer_size),
         {:ok, descriptor} <- Representation.parse_trailer(trailer),
         :ok <- Representation.validate_file_size(size, descriptor),
         :ok <- Representation.validate_route_digest(digest, descriptor) do
      {:ok, descriptor, size}
    else
      {:error, %Error{}} = error ->
        error

      {:error, reason} ->
        {:error,
         Error.integrity_violation("attachment representation trailer is invalid", %{
           reason: inspect(reason)
         })}
    end
  end

  defp validate_physical_blob(path, digest) do
    with {:ok, descriptor, _size} <- read_validated_descriptor(path, digest),
         {:ok, fd} <- File.open(path, [:read, :binary, :raw]) do
      result = hash_payload(fd, descriptor)
      close_fd(fd)
      result
    end
  end

  defp hash_payload(fd, descriptor) do
    case hash_payload_loop(fd, descriptor.payload_length, :crypto.hash_init(:sha256)) do
      {:ok, digest} ->
        if digest == descriptor.payload_sha256 do
          {:ok, descriptor}
        else
          {:error, Error.integrity_violation("attachment representation payload digest mismatch")}
        end

      {:error, _} = error ->
        error
    end
  end

  defp hash_payload_loop(_fd, remaining, hash_ctx) when remaining <= 0 do
    {:ok, :crypto.hash_final(hash_ctx) |> Base.encode16(case: :lower)}
  end

  defp hash_payload_loop(fd, remaining, hash_ctx) do
    case read_payload_chunk(fd, remaining) do
      {:ok, chunk, unread} ->
        hash_payload_loop(fd, unread, :crypto.hash_update(hash_ctx, chunk))

      :eof ->
        {:error, Error.integrity_violation("attachment representation payload is truncated")}

      {:error, %Error{}} = error ->
        error

      {:error, reason} ->
        {:error, Error.internal_error("attachment read failed", %{reason: inspect(reason)})}
    end
  end

  defp bundle_for(%StoreRef{mode: :bundle_local, bundle_root: bundle_path}) do
    case DatabaseBundle.open(bundle_path) do
      {:ok, bundle} ->
        if PathSafety.within_root?(bundle.blobs_path, bundle.root) and
             PathSafety.within_root?(bundle.tmp_path, bundle.root) do
          {:ok, bundle}
        else
          {:error, Error.integrity_violation("bundle blob paths escape root")}
        end

      {:error, %Error{}} = error ->
        error
    end
  end

  defp bundle_for(%StoreRef{mode: :external_read_only}),
    do: {:error, Error.shadow_attachment_store_read_only("external attachment store is read-only")}

  defp bundle_for(bundle_path) when is_binary(bundle_path),
    do: bundle_for(StoreRef.bundle_local(bundle_path))

  defp bundle_for(_),
    do: {:error, Error.invalid_request("attachment store reference is invalid")}

  defp readable_ref(store_ref), do: StoreRef.normalize(store_ref)

  defp writable_ref(store_ref) do
    with {:ok, ref} <- StoreRef.normalize(store_ref),
         :ok <- StoreRef.ensure_writable(ref) do
      {:ok, ref}
    end
  end

  defp exclusive_tmp(tmp_path) do
    name = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    path = Path.join(tmp_path, name)

    case :file.write_file(path, <<>>, [:write, :exclusive, :raw, :binary]) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, reason}
    end
  end

  defp blob_path(blobs_path, digest),
    do: Path.join([blobs_path, prefix(digest), digest <> ".blob"])

  defp prefix(digest), do: String.slice(digest, 0, 2)

  defp ensure_blob_directory(bundle, digest) do
    directory = Path.join(bundle.blobs_path, prefix(digest))

    with :ok <- ensure_blob_path_safe(bundle, digest),
         :ok <- mkdir_blob_directory(directory) do
      ensure_blob_path_safe(bundle, digest)
    end
  end

  defp mkdir_blob_directory(directory) do
    case File.mkdir(directory) do
      :ok ->
        :ok

      {:error, :eexist} ->
        case File.lstat(directory) do
          {:ok, %File.Stat{type: :directory}} ->
            :ok

          {:ok, _stat} ->
            {:error, Error.integrity_violation("attachment blob prefix must be a directory")}

          {:error, :enoent} ->
            mkdir_blob_directory(directory)

          {:error, reason} ->
            {:error,
             Error.internal_error("cannot inspect blob directory", %{reason: inspect(reason)})}
        end

      {:error, reason} ->
        {:error, Error.internal_error("cannot create blob directory", %{reason: inspect(reason)})}
    end
  end

  defp ensure_blob_path_safe(%DatabaseBundle{} = bundle, digest),
    do: ensure_blob_path_safe(StoreRef.bundle_local(bundle), digest)

  defp ensure_blob_path_safe(%StoreRef{} = ref, digest) do
    directory = Path.join(ref.blobs_path, prefix(digest))

    with :ok <- validate_blob_root(ref, directory),
         :ok <- validate_blob_symlinks(directory) do
      inspect_blob_directory(directory)
    end
  end

  defp validate_blob_root(%StoreRef{} = ref, directory) do
    root = ref.bundle_root || ref.blobs_path

    if PathSafety.within_root?(directory, root) or StoreRef.within_allowed_root?(ref, directory),
      do: :ok,
      else: {:error, Error.integrity_violation("attachment blob path escapes root")}
  end

  defp validate_blob_symlinks(directory) do
    if PathSafety.no_symlink_components?(directory),
      do: :ok,
      else: {:error, Error.integrity_violation("attachment blob path contains a symlink")}
  end

  defp inspect_blob_directory(directory) do
    case File.lstat(directory) do
      {:ok, %File.Stat{type: :directory}} ->
        :ok

      {:ok, _stat} ->
        {:error, Error.integrity_violation("attachment blob prefix must be a directory")}

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, Error.internal_error("cannot inspect blob directory", %{reason: inspect(reason)})}
    end
  end

  defp validate_digest(digest) when is_binary(digest) do
    if valid_digest?(digest) do
      {:ok, digest}
    else
      {:error, Error.invalid_request("attachment digest must be lowercase SHA-256 hex")}
    end
  end

  defp valid_digest?(digest), do: byte_size(digest) == 64 and Regex.match?(@digest_pattern, digest)

  defp digest_from_blob_path(path) do
    digest =
      path
      |> Path.basename()
      |> String.replace_suffix(".blob", "")

    prefix = path |> Path.dirname() |> Path.basename()

    if valid_digest?(digest) and prefix(digest) == prefix do
      digest
    else
      nil
    end
  end

  defp file_mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> {:ok, mtime}
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_rm(path) do
    case File.rm(path) do
      :ok ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error, Error.internal_error("attachment delete failed", %{reason: inspect(reason)})}
    end
  end

  defp sync_file(fd), do: :file.sync(fd)

  defp close_fd(fd) when is_pid(fd) or is_reference(fd) do
    _ = File.close(fd)
    :ok
  end

  defp close_fd(_), do: :ok
end
