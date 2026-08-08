defmodule ElixirDB.Attachments.FilesystemStore do
  @moduledoc """
  Version 1 filesystem attachment store for database bundles.

  Blobs live under `blobs/<prefix>/<digest>.raw` or `.zst`. Temporary uploads
  use random names under `tmp/`.
  """

  @behaviour ElixirDB.Attachments.Store

  alias ElixirDB.Attachments.Compression
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.DurableFS
  alias ElixirDB.Error
  alias ElixirDB.PathSafety

  @probe_prefix_bytes 256 * 1024
  @read_chunk_size 64 * 1024
  @integrity_footer_magic <<0x50, 0x2A, 0x4D, 0x18>>
  @integrity_footer_payload_size 40
  @integrity_footer_size 48
  @digest_pattern ~r/^[0-9a-f]{64}$/
  @writer_key {:elixirdb, :attachment_writer}

  @type writer :: {:writer, reference()}

  @type writer_state :: %{
          bundle_path: binary(),
          tmp_path: binary(),
          fd: :file.io_device(),
          hash_ctx: term(),
          logical_size: non_neg_integer(),
          max_bytes: pos_integer(),
          phase: :probing | {:writing, :raw | :compressed},
          probe_buffer: binary(),
          compression_ctx: reference() | nil
        }

  @reader_key {:elixirdb, :attachment_reader}

  @type reader :: {:reader, reference()}

  @impl true
  def begin_put(bundle_path, max_bytes, _options)
      when is_binary(bundle_path) and is_integer(max_bytes) and max_bytes > 0 do
    with {:ok, bundle} <- bundle_for(bundle_path),
         {:ok, tmp_path} <- exclusive_tmp(bundle.tmp_path),
         {:ok, fd} <- File.open(tmp_path, [:write, :binary, :raw]) do
      ref = make_ref()

      put_writer(
        ref,
        %{
          bundle_path: bundle.root,
          tmp_path: tmp_path,
          fd: fd,
          hash_ctx: :crypto.hash_init(:sha256),
          logical_size: 0,
          max_bytes: max_bytes,
          phase: :probing,
          probe_buffer: <<>>,
          compression_ctx: nil
        }
      )

      {:ok, {:writer, ref}}
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
           {:ok, state} <- finalize_physical(state),
           phase = state.phase,
           encoding = encoding_from_phase(phase),
           {:ok, digest, logical_size} <- finalize_digest(state),
           :ok <- append_integrity_footer(state.fd, encoding, digest, logical_size),
           :ok <- sync_file(state.fd),
           :ok <- close_fd(state.fd),
           {:ok, install_kind} <-
             install(state.bundle_path, state.tmp_path, digest, logical_size, phase) do
        {:ok,
         %{
           digest: digest,
           logical_size: logical_size,
           encoding: encoding,
           deduplicated?: install_kind == :deduplicated
         }}
      end

    case result do
      {:ok, _} = ok ->
        drop_writer(ref)
        ok

      {:error, %Error{} = error} ->
        abort_put(writer)
        {:error, error}

      {:error, reason} ->
        abort_put(writer)
        {:error, Error.internal_error("attachment finish failed", %{reason: inspect(reason)})}
    end
  end

  def finish_put(writer) do
    abort_put(writer)
    {:error, Error.internal_error("attachment writer is in an invalid state")}
  end

  @impl true
  def abort_put({:writer, ref}) do
    state = fetch_writer(ref)

    drop_writer(ref)

    case state do
      {:ok, %{fd: fd, tmp_path: tmp_path}} ->
        close_fd(fd)
        _ = File.rm(tmp_path)
        :ok

      _ ->
        :ok
    end
  end

  def abort_put(_), do: :ok

  @doc false
  @spec writer_tmp_path(writer()) :: binary() | nil
  def writer_tmp_path({:writer, ref}) do
    case fetch_writer(ref) do
      {:ok, %{tmp_path: tmp_path}} -> tmp_path
      _ -> nil
    end
  end

  @impl true
  def exists?(bundle_path, digest) do
    match?({:ok, _, _}, resolve_representation(bundle_path, digest))
  end

  @impl true
  def stat(bundle_path, digest) do
    with {:ok, digest} <- validate_digest(digest),
         {:ok, path, encoding} <- resolve_representation(bundle_path, digest),
         {:ok, stat} <- file_stat(path) do
      {:ok, %{encoding: encoding, physical_size: stat.size}}
    end
  end

  @impl true
  def open_read(bundle_path, digest) do
    with {:ok, digest} <- validate_digest(digest),
         {:ok, path, encoding} <- resolve_representation(bundle_path, digest),
         {:ok, fd, integrity, decompress_ctx} <- open_read_components(path, encoding, digest) do
      ref = make_ref()

      put_reader(
        ref,
        %{
          fd: fd,
          encoding: encoding,
          decompress_ctx: decompress_ctx,
          pending: <<>>,
          done?: false,
          hash_ctx: :crypto.hash_init(:sha256),
          logical_size: 0,
          expected_size: integrity.logical_size,
          expected_digest: integrity.digest
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

      {:ok, %{encoding: :compressed} = reader} ->
        read_compressed_chunk(ref, reader)

      {:error, %Error{}} = error ->
        error
    end
  end

  def read_chunks(_reader) do
    {:error, Error.internal_error("attachment reader is not active")}
  end

  defp read_raw_chunk(ref, %{fd: fd} = reader) do
    case :file.read(fd, @read_chunk_size) do
      :eof ->
        put_reader(ref, %{reader | done?: true})
        close_read({:reader, ref})
        {:done, {:reader, ref}}

      {:ok, chunk} ->
        {:ok, chunk}

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

  @impl true
  def close_read({:reader, ref}) do
    case fetch_reader(ref) do
      {:ok, %{fd: fd}} -> close_fd(fd)
      _ -> :ok
    end

    drop_reader(ref)
    :ok
  end

  def close_read(_), do: :ok

  @impl true
  def list_digests(bundle_path) do
    with {:ok, bundle} <- bundle_for(bundle_path) do
      digests =
        bundle.blobs_path
        |> Path.join("**/*.{raw,zst}")
        |> Path.wildcard()
        |> Enum.map(&digest_from_blob_path/1)
        |> Enum.filter(&valid_digest?/1)
        |> Enum.uniq()
        |> Enum.sort()

      {:ok, digests}
    end
  end

  @impl true
  def delete(bundle_path, digest) do
    with {:ok, digest} <- validate_digest(digest),
         {:ok, path, _encoding} <- resolve_representation(bundle_path, digest) do
      safe_rm(path)
    end
  end

  @impl true
  def verify(bundle_path, digest, expected_size)
      when is_integer(expected_size) and expected_size >= 0 do
    with {:ok, digest} <- validate_digest(digest),
         :ok <- ensure_single_representation(bundle_path, digest),
         {:ok, reader} <- open_read(bundle_path, digest) do
      stream_verify(reader, expected_size, digest)
    end
  end

  @impl true
  def cleanup_tmp(bundle_path, %DateTime{} = cutoff) do
    with {:ok, bundle} <- bundle_for(bundle_path) do
      cutoff_unix = DateTime.to_unix(cutoff)

      bundle.tmp_path
      |> File.ls()
      |> cleanup_tmp_entries(bundle.tmp_path, cutoff_unix)
    end
  end

  defp put_reader(ref, state), do: Process.put(reader_key(ref), state)

  defp drop_reader(ref), do: Process.delete(reader_key(ref))

  defp fetch_reader(ref) do
    case Process.get(reader_key(ref)) do
      nil -> {:error, Error.internal_error("attachment reader is not active")}
      state -> {:ok, state}
    end
  end

  defp reader_key(ref), do: {@reader_key, ref}

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

  defp put_writer(ref, state), do: Process.put(writer_key(ref), state)

  defp drop_writer(ref), do: Process.delete(writer_key(ref))

  defp fetch_writer(ref) do
    case Process.get(writer_key(ref)) do
      nil -> {:error, Error.internal_error("attachment writer is not active")}
      state -> {:ok, state}
    end
  end

  defp update_writer(ref, fun) do
    with {:ok, state} <- fetch_writer(ref) do
      case fun.(state) do
        {:ok, new_state} ->
          put_writer(ref, new_state)
          :ok

        {:error, _} = error ->
          error
      end
    end
  end

  defp writer_key(ref), do: {@writer_key, ref}

  defp write_chunk_state(state, chunk) do
    with {:ok, state} <- add_logical_bytes(state, chunk) do
      write_original_chunk(state, chunk)
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
        decide_encoding(state)
        |> then(fn {:ok, state} -> write_payload(state, chunk) end)

      byte_size(chunk) <= remaining_probe ->
        {:ok, %{state | probe_buffer: state.probe_buffer <> chunk}}

      true ->
        probe_part = binary_part(chunk, 0, remaining_probe)
        rest = binary_part(chunk, remaining_probe, byte_size(chunk) - remaining_probe)

        decide_encoding(%{state | probe_buffer: state.probe_buffer <> probe_part})
        |> then(fn {:ok, state} -> write_payload(state, rest) end)
    end
  end

  defp write_original_chunk(state, chunk), do: write_payload(state, chunk)

  defp write_payload(%{phase: {:writing, :raw}, fd: fd} = state, chunk) do
    case :file.write(fd, chunk) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_payload(%{phase: {:writing, :compressed}, fd: fd, compression_ctx: ctx} = state, chunk) do
    case Compression.compress_chunk(ctx, chunk) do
      {:ok, output, ctx} ->
        case write_compressed_output(fd, output) do
          :ok -> {:ok, %{state | compression_ctx: ctx}}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_payload(_state, _chunk), do: {:error, :invalid_writer_phase}

  defp write_compressed_output(fd, output), do: :file.write(fd, output)

  defp maybe_decide_encoding(%{phase: :probing} = state), do: decide_encoding(state)
  defp maybe_decide_encoding(state), do: {:ok, state}

  defp decide_encoding(%{probe_buffer: probe} = state) do
    encoding = encoding_for_probe(probe)

    with {:ok, state} <- start_writing(state, encoding),
         {:ok, state} <- write_payload(state, probe) do
      {:ok, %{state | probe_buffer: <<>>}}
    end
  end

  defp encoding_for_probe(<<>>), do: :raw

  defp encoding_for_probe(probe) do
    case Compression.probe(probe) do
      {:ok, result} -> if Compression.worthwhile?(result), do: :compressed, else: :raw
      {:error, _} -> :raw
    end
  end

  defp start_writing(state, :raw) do
    {:ok, %{state | phase: {:writing, :raw}, compression_ctx: nil}}
  end

  defp start_writing(state, :compressed) do
    with {:ok, ctx} <- Compression.new_compression_context(),
         :ok <- Compression.set_level(ctx) do
      {:ok, %{state | phase: {:writing, :compressed}, compression_ctx: ctx}}
    end
  end

  defp finalize_physical(%{phase: {:writing, :compressed}, fd: fd, compression_ctx: ctx} = state) do
    with {:ok, output, _ctx} <- Compression.finish_compression(ctx, <<>>),
         :ok <- write_compressed_output(fd, output) do
      {:ok, state}
    end
  end

  defp finalize_physical(state), do: {:ok, state}

  defp finalize_digest(%{hash_ctx: hash_ctx, logical_size: logical_size}) do
    digest = :crypto.hash_final(hash_ctx) |> Base.encode16(case: :lower)
    {:ok, digest, logical_size}
  end

  defp append_integrity_footer(_fd, :raw, _digest, _logical_size), do: :ok

  defp append_integrity_footer(fd, :compressed, digest, logical_size) do
    case Base.decode16(digest, case: :lower) do
      {:ok, digest_bytes} ->
        footer =
          @integrity_footer_magic <>
            <<@integrity_footer_payload_size::little-unsigned-32>> <>
            digest_bytes <>
            <<logical_size::little-unsigned-64>>

        :file.write(fd, footer)

      :error ->
        {:error, :invalid_digest}
    end
  end

  defp read_integrity_footer(_fd, _path, :raw, _digest),
    do: {:ok, %{logical_size: nil, digest: nil}}

  defp read_integrity_footer(fd, path, :compressed, digest) do
    with {:ok, %File.Stat{size: size}} <- File.stat(path),
         true <- size >= @integrity_footer_size,
         {:ok, footer} <- :file.pread(fd, size - @integrity_footer_size, @integrity_footer_size),
         {:ok, logical_size} <- parse_integrity_footer(footer, digest) do
      {:ok, %{logical_size: logical_size, digest: digest}}
    else
      false ->
        {:error, Error.integrity_violation("compressed attachment integrity footer is missing")}

      {:error, %Error{}} = error ->
        error

      {:error, reason} ->
        {:error,
         Error.integrity_violation(
           "compressed attachment integrity footer is invalid",
           %{reason: inspect(reason)}
         )}

      _ ->
        {:error, Error.integrity_violation("compressed attachment integrity footer is invalid")}
    end
  end

  defp open_read_components(path, encoding, digest) do
    with {:ok, fd} <- File.open(path, [:read, :binary, :raw]) do
      open_read_components(fd, path, encoding, digest)
    end
  end

  defp open_read_components(fd, path, encoding, digest) do
    case read_integrity_footer(fd, path, encoding, digest) do
      {:ok, integrity} ->
        open_read_decompressor(fd, integrity, encoding)

      {:error, reason} ->
        close_fd(fd)
        {:error, reason}
    end
  end

  defp open_read_decompressor(fd, integrity, encoding) do
    case maybe_open_decompressor(encoding) do
      {:ok, decompress_ctx} ->
        {:ok, fd, integrity, decompress_ctx}

      {:error, reason} ->
        close_fd(fd)
        {:error, reason}
    end
  end

  defp parse_integrity_footer(
         <<magic::binary-size(4), payload_size::little-unsigned-32, digest_bytes::binary-size(32),
           logical_size::little-unsigned-64>>,
         digest
       ) do
    with true <- magic == @integrity_footer_magic,
         true <- payload_size == @integrity_footer_payload_size,
         {:ok, expected_digest} <- Base.decode16(digest, case: :lower),
         true <- digest_bytes == expected_digest do
      {:ok, logical_size}
    else
      _ -> {:error, Error.integrity_violation("compressed attachment digest footer mismatch")}
    end
  end

  defp parse_integrity_footer(_footer, _digest),
    do: {:error, Error.integrity_violation("compressed attachment integrity footer is invalid")}

  defp install(bundle_path, tmp_path, digest, logical_size, phase) do
    encoding = encoding_from_phase(phase)

    case bundle_for(bundle_path) do
      {:ok, bundle} ->
        with :ok <- ensure_blob_directory(bundle, digest) do
          install_into_bundle(bundle, tmp_path, digest, logical_size, encoding)
        end

      {:error, _} = error ->
        error
    end
  end

  defp encoding_from_phase({:writing, encoding}), do: encoding
  defp encoding_from_phase(:probing), do: :raw

  defp install_into_bundle(bundle, tmp_path, digest, logical_size, encoding) do
    case resolve_representation(bundle.root, digest) do
      {:ok, _existing_path, _existing_encoding} ->
        reuse_verified_blob(bundle.root, tmp_path, digest, logical_size)

      {:error, %Error{code: :attachment_blob_not_found}} ->
        install_new_blob(bundle, tmp_path, digest, logical_size, encoding)

      {:error, %Error{}} = error ->
        _ = File.rm(tmp_path)
        error
    end
  end

  defp reuse_verified_blob(bundle_root, tmp_path, digest, logical_size) do
    result = verify(bundle_root, digest, logical_size)
    _ = File.rm(tmp_path)

    case result do
      :ok -> {:ok, :deduplicated}
      {:error, _} = error -> error
    end
  end

  defp install_new_blob(bundle, tmp_path, digest, logical_size, encoding) do
    dest = blob_path(bundle.blobs_path, digest, encoding)

    case atomic_install(tmp_path, dest) do
      :ok ->
        case DurableFS.sync_directory(Path.dirname(dest)) do
          :ok -> {:ok, :new}
          {:error, reason} -> {:error, reason}
        end

      {:error, :collision} ->
        # Another writer won the race; reuse only after validating the winner.
        _ = File.rm(tmp_path)
        reuse_after_collision(bundle.root, digest, logical_size)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reuse_after_collision(bundle_root, digest, logical_size) do
    case ensure_single_representation(bundle_root, digest) do
      :ok ->
        case verify(bundle_root, digest, logical_size) do
          :ok -> {:ok, :deduplicated}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  defp atomic_install(tmp_path, dest) do
    # A rename is not an exclusive install on POSIX: a concurrent writer can
    # create `dest` after an existence check and the rename may replace it.
    # Linking the private temp inode is atomic and fails without replacement
    # when the destination already exists.
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

  defp read_decompressed_chunk(%{fd: fd, decompress_ctx: ctx, pending: pending} = reader) do
    case take_decompressed_output(ctx, pending) do
      {:ok, output, ctx, pending} when output != <<>> ->
        reader = %{reader | decompress_ctx: ctx, pending: pending}

        case track_decompressed_output(reader, output) do
          {:ok, reader} -> {:ok, reader, output}
          {:error, _} = error -> error
        end

      {:ok, <<>>, ctx, pending} ->
        case :file.read(fd, @read_chunk_size) do
          :eof ->
            {:ok, %{reader | decompress_ctx: ctx, pending: pending, done?: true}, <<>>}

          {:ok, compressed} ->
            read_decompressed_chunk(%{reader | decompress_ctx: ctx, pending: pending <> compressed})

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, Error.integrity_violation("compressed attachment is corrupt: #{inspect(reason)}")}
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

  defp maybe_open_decompressor(:raw), do: {:ok, nil}

  defp maybe_open_decompressor(:compressed), do: Compression.new_decompression_context()

  defp resolve_representation(bundle_path, digest) do
    with {:ok, digest} <- validate_digest(digest),
         {:ok, bundle} <- bundle_for(bundle_path),
         :ok <- ensure_blob_path_safe(bundle, digest) do
      raw = blob_path(bundle.blobs_path, digest, :raw)
      zst = blob_path(bundle.blobs_path, digest, :compressed)
      raw_symlink? = symlink_blob?(raw)
      zst_symlink? = symlink_blob?(zst)
      raw? = regular_blob?(raw)
      zst? = regular_blob?(zst)

      cond do
        raw_symlink? or zst_symlink? ->
          {:error, Error.integrity_violation("attachment blob representation is a symlink")}

        raw? and zst? ->
          {:error, Error.integrity_violation("attachment has multiple physical representations")}

        raw? ->
          {:ok, raw, :raw}

        zst? ->
          {:ok, zst, :compressed}

        true ->
          {:error, Error.attachment_blob_not_found("attachment blob not found")}
      end
    end
  end

  defp ensure_single_representation(bundle_path, digest) do
    case resolve_representation(bundle_path, digest) do
      {:error, %Error{code: :attachment_blob_not_found}} -> :ok
      {:error, %Error{code: :integrity_violation}} = error -> error
      {:ok, _, _} -> :ok
      {:error, %Error{}} = error -> error
    end
  end

  defp bundle_for(bundle_path) do
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

  defp exclusive_tmp(tmp_path) do
    name = :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
    path = Path.join(tmp_path, name)

    case :file.write_file(path, <<>>, [:write, :exclusive, :raw, :binary]) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, reason}
    end
  end

  defp blob_path(blobs_path, digest, :raw),
    do: Path.join([blobs_path, prefix(digest), digest <> ".raw"])

  defp blob_path(blobs_path, digest, :compressed),
    do: Path.join([blobs_path, prefix(digest), digest <> ".zst"])

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

  defp ensure_blob_path_safe(bundle, digest) do
    directory = Path.join(bundle.blobs_path, prefix(digest))

    cond do
      not PathSafety.within_root?(directory, bundle.root) ->
        {:error, Error.integrity_violation("attachment blob path escapes root")}

      not PathSafety.no_symlink_components?(directory) ->
        {:error, Error.integrity_violation("attachment blob path contains a symlink")}

      true ->
        case File.lstat(directory) do
          {:ok, %File.Stat{type: :directory}} ->
            :ok

          {:ok, _stat} ->
            {:error, Error.integrity_violation("attachment blob prefix must be a directory")}

          {:error, :enoent} ->
            :ok

          {:error, reason} ->
            {:error,
             Error.internal_error("cannot inspect blob directory", %{reason: inspect(reason)})}
        end
    end
  end

  defp regular_blob?(path) do
    match?({:ok, %File.Stat{type: :regular}}, File.lstat(path))
  end

  defp symlink_blob?(path) do
    match?({:ok, %File.Stat{type: :symlink}}, File.lstat(path))
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
    path
    |> Path.basename()
    |> String.replace_suffix(".raw", "")
    |> String.replace_suffix(".zst", "")
  end

  defp file_stat(path), do: File.stat(path)

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
