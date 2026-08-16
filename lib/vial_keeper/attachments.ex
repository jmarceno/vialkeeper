defmodule VialKeeper.Attachments do
  @moduledoc """
  Storage-neutral attachment orchestration for HTTP and local mutation paths.

  Coordinates `AttachmentCoordinator` guards, short owner metadata commands, and
  `FilesystemStore` byte I/O. Never holds database admission or `DatabaseOwner`
  while attachment bytes are transferred.
  """

  alias VialKeeper.Attachments.{FilesystemStore, Manifest, Representation, StoreRef, Ticket}
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Attachment, as: AttachmentInstr
  alias VialKeeper.Observability.Instrumentation.AttachmentUpload
  alias VialKeeper.Replication.BlobRepresentationStream
  alias VialKeeper.Runtime.{AttachmentCoordinator, DatabaseCatalog}
  alias VialKeeper.Shadow.ReadRouter

  @store FilesystemStore
  @release_on_raise [
    ArgumentError,
    ArithmeticError,
    BadMapError,
    CaseClauseError,
    ErlangError,
    FunctionClauseError,
    KeyError,
    MatchError,
    Protocol.UndefinedError,
    RuntimeError,
    UndefinedFunctionError,
    WithClauseError
  ]
  @pending_protection_hours 24
  @read_chunk_opts [length: 65_536, read_length: 65_536]
  @default_batch_concurrency 16

  @type guard :: reference() | nil
  @type chunk_source :: Plug.Conn.t() | Enumerable.t() | (-> term())
  @type batch_source :: %{required(:source) => chunk_source(), optional(:key) => term()}

  @doc "Returns the measured default concurrency for local attachment batches."
  @spec default_batch_concurrency() :: pos_integer()
  def default_batch_concurrency, do: @default_batch_concurrency

  @doc """
  Streams an upload into the database-local CAS and protects the digest.

  `source` may be a `Plug.Conn`, an enumerable of binaries, or a zero-arity
  function returning `{:ok, binary(), next_fun}`, `:done`, or `{:error, error}`.

  When `source` is a `Plug.Conn`, success returns `{:ok, data, conn}` with the
  connection whose request body was fully consumed; the caller must respond on
  that connection so keepalive body accounting stays correct.
  """
  @spec upload_stream(binary(), chunk_source(), keyword()) ::
          {:ok, map()} | {:ok, map(), Plug.Conn.t()} | {:error, VialKeeper.Error.t()}
  def upload_stream(uuid, source, opts \\ []) when is_binary(uuid) and is_list(opts) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    with :ok <- AttachmentUpload.phase(:open_check, fn -> ensure_open(uuid) end),
         :ok <-
           AttachmentUpload.phase(:writable_check, fn -> ensure_writable(uuid, admission_class) end),
         {:ok, bundle_root} <-
           AttachmentUpload.phase(:bundle_lookup, fn -> DatabaseCatalog.bundle_root(uuid) end),
         {:ok, write_guard, max_bytes} <-
           AttachmentUpload.phase(:coordinator_wait, fn ->
             AttachmentCoordinator.acquire_write(uuid)
           end) do
      try do
        AttachmentInstr.write(uuid, fn ->
          do_upload(uuid, bundle_root, source, max_bytes, opts, admission_class)
        end)
      after
        _ = AttachmentCoordinator.release(uuid, write_guard)
      end
      |> pop_source_conn()
    end
  end

  @doc """
  Streams a bounded collection of local/import sources and protects completed
  digests in metadata batches.

  Each item is `%{source: source, key: optional_key}` or `{key, source}`. The
  sources are never materialized as complete binaries by this function. A
  reference guard remains held from before physical writes through the final
  pending-protection transaction, so attachment GC cannot race the batch.
  """
  @spec upload_batch(binary(), [batch_source() | {term(), chunk_source()}], keyword()) ::
          {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def upload_batch(uuid, sources, opts \\ [])

  def upload_batch(uuid, sources, opts)
      when is_binary(uuid) and is_list(sources) and is_list(opts) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    with {:ok, normalized} <- normalize_batch_sources(sources),
         :ok <- AttachmentUpload.phase(:open_check, fn -> ensure_open(uuid) end),
         :ok <-
           AttachmentUpload.phase(:writable_check, fn -> ensure_writable(uuid, admission_class) end),
         {:ok, bundle_root} <-
           AttachmentUpload.phase(:bundle_lookup, fn -> DatabaseCatalog.bundle_root(uuid) end),
         {:ok, concurrency} <- batch_concurrency(uuid, opts),
         {:ok, reference_guard} <-
           AttachmentUpload.phase(:coordinator_wait, fn ->
             AttachmentCoordinator.acquire_reference(uuid)
           end) do
      try do
        with {:ok, physical} <-
               upload_physical_batch(uuid, bundle_root, normalized, concurrency, opts),
             {:ok, expiries} <-
               protect_uploaded_blobs(uuid, physical, admission_class, opts) do
          {:ok,
           Enum.map(physical, fn descriptor ->
             Map.put(descriptor, :expires_at, Map.fetch!(expiries, descriptor.blob))
           end)}
        end
      after
        _ = AttachmentCoordinator.release(uuid, reference_guard)
      end
    end
  end

  def upload_batch(_uuid, _sources, _opts),
    do: {:error, VialKeeper.Error.invalid_request("attachment batch must be a list")}

  @doc """
  Resolves an attachment ticket under a read guard and opens a store reader.

  The returned map includes a lazy `body` enumerable. Consuming or abandoning
  the enumerable releases the reader and read guard.
  """
  @spec open_stream(binary(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def open_stream(uuid, request) when is_binary(uuid) and is_map(request) do
    with :ok <- ensure_open(uuid),
         {:ok, ticket_request} <- normalize_get_request(request),
         {:ok, read_guard} <- AttachmentCoordinator.acquire_read(uuid) do
      read_handle = AttachmentInstr.start_read(uuid)

      try do
        case open_stream_under_guard(uuid, ticket_request, read_guard, read_handle) do
          {:ok, _} = ok ->
            ok

          {:error, %VialKeeper.Error{} = error} = err ->
            _ = AttachmentInstr.fail_read(read_handle, error)
            err
        end
      rescue
        exception in @release_on_raise ->
          _ = AttachmentCoordinator.release(uuid, read_guard)

          _ =
            AttachmentInstr.fail_read(
              read_handle,
              VialKeeper.Error.internal_error("attachment read failed")
            )

          reraise(exception, __STACKTRACE__)
      catch
        kind, reason ->
          _ = AttachmentCoordinator.release(uuid, read_guard)

          _ =
            AttachmentInstr.fail_read(
              read_handle,
              VialKeeper.Error.internal_error("attachment read failed")
            )

          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  @doc "Opens an attachment through the selected primary or eventual shadow route."
  @spec open_stream_with_meta(binary(), map(), keyword()) ::
          {:ok, map(), map()} | {:error, VialKeeper.Error.t()}
  def open_stream_with_meta(uuid, request, opts \\ []) when is_binary(uuid) and is_map(request) do
    ReadRouter.open_attachment(
      uuid,
      request,
      Keyword.put(opts, :primary, fn primary_request -> open_stream(uuid, primary_request) end)
    )
  end

  @doc "Resolves a ticket without opening the byte stream."
  @spec resolve_ticket(binary(), map()) ::
          {:ok, Ticket.t(), guard()} | {:error, VialKeeper.Error.t()}
  def resolve_ticket(uuid, request) when is_binary(uuid) and is_map(request) do
    with :ok <- ensure_open(uuid),
         {:ok, ticket_request} <- normalize_get_request(request),
         {:ok, read_guard} <- AttachmentCoordinator.acquire_read(uuid) do
      try do
        case resolve_ticket_command(uuid, ticket_request) do
          {:ok, ticket} ->
            {:ok, ticket, read_guard}

          {:error, _} = error ->
            _ = AttachmentCoordinator.release(uuid, read_guard)
            error
        end
      rescue
        exception in @release_on_raise ->
          _ = AttachmentCoordinator.release(uuid, read_guard)
          reraise(exception, __STACKTRACE__)
      end
    end
  end

  @doc """
  Validates client attachment references and derives authoritative lengths.

  Returns `{attachments_for_owner, reference_guard}`. The guard is `nil` when
  no reference protection is required (`%{}`). Callers MUST release a non-nil
  guard after the owner mutation completes or fails.
  """
  @spec resolve_manifest_for_mutation(binary(), :omitted | map()) ::
          {:ok, :omitted | Manifest.t(), guard()} | {:error, VialKeeper.Error.t()}
  def resolve_manifest_for_mutation(uuid, :omitted) when is_binary(uuid) do
    with :ok <- ensure_open(uuid),
         {:ok, guard} <- AttachmentCoordinator.acquire_reference(uuid) do
      {:ok, :omitted, guard}
    end
  end

  def resolve_manifest_for_mutation(uuid, attachments)
      when is_binary(uuid) and attachments == %{} do
    {:ok, %{}, nil}
  end

  def resolve_manifest_for_mutation(uuid, attachments)
      when is_binary(uuid) and is_map(attachments) do
    with :ok <- ensure_open(uuid),
         {:ok, references} <- Manifest.normalize_references(attachments),
         {:ok, guard} <- AttachmentCoordinator.acquire_reference(uuid) do
      try do
        case materialize_normalized(uuid, references) do
          {:ok, manifest} ->
            {:ok, manifest, guard}

          {:error, _} = error ->
            _ = AttachmentCoordinator.release(uuid, guard)
            error
        end
      rescue
        exception in @release_on_raise ->
          _ = AttachmentCoordinator.release(uuid, guard)
          reraise(exception, __STACKTRACE__)
      end
    end
  end

  def resolve_manifest_for_mutation(_uuid, _attachments),
    do: {:error, VialKeeper.Error.invalid_request("attachments must be an object")}

  @doc """
  Materializes client references into a complete manifest without acquiring a guard.

  Callers MUST already hold a reference guard for the database.
  """
  @spec materialize_references(binary(), map()) ::
          {:ok, Manifest.t()} | {:error, VialKeeper.Error.t()}
  def materialize_references(uuid, references) when is_binary(uuid) and is_map(references) do
    with {:ok, normalized} <- Manifest.normalize_references(references) do
      materialize_normalized(uuid, normalized)
    end
  end

  @doc "Releases an attachment guard token when present."
  @spec release_guard(binary(), guard()) :: :ok
  def release_guard(_uuid, nil), do: :ok

  def release_guard(uuid, guard) when is_binary(uuid) do
    case AttachmentCoordinator.release(uuid, guard) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  end

  @doc """
  Reclaims unreachable attachment blobs under the exclusive GC barrier.

  Short owner calls collect the live digest set and clean expired pending rows;
  physical deletes and temporary-file cleanup run after metadata work and
  outside owner admission. GC metadata work uses the maintenance admission
  class.
  """
  @spec gc(binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def gc(uuid) when is_binary(uuid) do
    with :ok <- ensure_open(uuid),
         {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, gc_token} <- AttachmentCoordinator.begin_gc(uuid) do
      case gc_token do
        :coalesced ->
          {:ok, %{deleted: 0, blobs_deleted: 0, bytes_deleted: 0, coalesced: true}}

        _ ->
          try do
            AttachmentInstr.gc(uuid, fn ->
              case run_gc(uuid, bundle_root, :maintenance) do
                {:ok, stats} ->
                  {:ok,
                   Map.merge(stats, %{
                     blobs_deleted: Map.get(stats, :deleted, 0),
                     bytes_deleted: Map.get(stats, :bytes_deleted, 0)
                   })}

                other ->
                  other
              end
            end)
          after
            _ = AttachmentCoordinator.end_gc(uuid, gc_token)
          end
      end
    end
  end

  @doc """
  Returns digests whose physical blob is not durably present in the local CAS.

  Preserves request order. Each digest must be lowercase SHA-256 hex.
  """
  @spec diff_blobs(binary(), [binary()], keyword()) ::
          {:ok, [binary()]} | {:error, VialKeeper.Error.t()}
  def diff_blobs(uuid, digests, opts \\ [])

  def diff_blobs(uuid, digests, opts) when is_binary(uuid) and is_list(digests) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    with :ok <- ensure_open(uuid),
         {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, validated} <- validate_digest_list(digests) do
      {:ok, missing_digests(uuid, bundle_root, validated, admission_class)}
    end
  end

  def diff_blobs(_uuid, _digests, _opts),
    do: {:error, VialKeeper.Error.invalid_request("blob digests must be a list")}

  @doc """
  Opens a lazy encoded-payload stream for a digest under a read guard.

  The returned `BlobRepresentationStream` body enumerable releases the reader
  and guard when consumed or abandoned. Payload bytes are not decompressed.
  """
  @spec open_blob_representation(binary(), binary(), keyword()) ::
          {:ok, BlobRepresentationStream.t()} | {:error, VialKeeper.Error.t()}
  def open_blob_representation(uuid, digest, opts \\ []) when is_binary(uuid) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    with :ok <- ensure_open(uuid),
         {:ok, digest} <- Manifest.validate_digest(digest),
         {:ok, read_guard} <- AttachmentCoordinator.acquire_read(uuid) do
      read_handle = AttachmentInstr.start_read(uuid)

      try do
        case open_blob_representation_under_guard(
               uuid,
               digest,
               read_guard,
               read_handle,
               admission_class
             ) do
          {:ok, _} = ok ->
            ok

          {:error, %VialKeeper.Error{} = error} = err ->
            _ = AttachmentInstr.fail_read(read_handle, error)
            err
        end
      rescue
        exception in @release_on_raise ->
          _ = AttachmentCoordinator.release(uuid, read_guard)

          _ =
            AttachmentInstr.fail_read(
              read_handle,
              VialKeeper.Error.internal_error("attachment read failed")
            )

          reraise(exception, __STACKTRACE__)
      catch
        kind, reason ->
          _ = AttachmentCoordinator.release(uuid, read_guard)

          _ =
            AttachmentInstr.fail_read(
              read_handle,
              VialKeeper.Error.internal_error("attachment read failed")
            )

          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  @doc """
  Installs an encoded representation through pending protection.

  Writes payload bytes with `begin_put_representation/3` and does not probe or
  recompress. `source` may be a `BlobRepresentationStream`, `Plug.Conn`,
  enumerable, or zero-arity chunk function.
  """
  @spec put_blob_representation(binary(), BlobRepresentationStream.t(), keyword()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def put_blob_representation(uuid, stream, opts \\ [])

  def put_blob_representation(uuid, %BlobRepresentationStream{} = stream, opts)
      when is_binary(uuid) do
    put_blob_representation(uuid, BlobRepresentationStream.descriptor(stream), stream.body, opts)
  end

  @doc """
  Installs an encoded blob representation streamed from `source`.

  When `source` is a `Plug.Conn`, success returns `{:ok, conn}` with the
  connection whose request body was fully consumed; the caller must respond on
  that connection so keepalive body accounting stays correct.
  """
  @spec put_blob_representation(binary(), map(), chunk_source(), keyword()) ::
          :ok | {:ok, Plug.Conn.t()} | {:error, VialKeeper.Error.t()}
  def put_blob_representation(uuid, descriptor, source, opts)
      when is_binary(uuid) and is_map(descriptor) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    with :ok <- ensure_open(uuid),
         :ok <- ensure_writable(uuid, admission_class),
         {:ok, descriptor} <- Representation.descriptor(descriptor),
         {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, write_guard, max_bytes} <- AttachmentCoordinator.acquire_write(uuid) do
      try do
        case AttachmentInstr.write(uuid, fn ->
               put_blob_representation_under_guard(
                 uuid,
                 bundle_root,
                 descriptor,
                 source,
                 max_bytes,
                 admission_class
               )
             end)
             |> pop_source_conn() do
          {:ok, _stats, %Plug.Conn{} = conn} -> {:ok, conn}
          {:ok, _stats} -> :ok
          {:error, _} = error -> error
        end
      after
        _ = AttachmentCoordinator.release(uuid, write_guard)
      end
    end
  end

  def put_blob_representation(_uuid, _descriptor, _source, _opts),
    do: {:error, VialKeeper.Error.invalid_request("invalid blob representation put")}

  defp put_blob_representation_under_guard(
         uuid,
         bundle_root,
         descriptor,
         source,
         max_bytes,
         admission_class
       ) do
    case @store.begin_put_representation(StoreRef.bundle_local(bundle_root), descriptor, max_bytes) do
      {:ok, writer} ->
        try do
          with {:ok, stream_chunks, final_conn} <-
                 normalize_chunk_result(write_all_representation_chunks(writer, source)),
               {:ok, finished} <- @store.finish_put_representation(writer),
               :ok <-
                 match_blob_identity(
                   descriptor.logical_digest,
                   descriptor.logical_length,
                   finished.digest,
                   finished.logical_size
                 ),
               {:ok, protected} <-
                 protect_uploaded_blob(
                   uuid,
                   descriptor.logical_digest,
                   finished.logical_size,
                   admission_class
                 ) do
            {:ok,
             protected
             |> Map.merge(%{
               stream_chunks: stream_chunks,
               encoding: finished.encoding,
               payload_length: descriptor.payload_length,
               deduplicated?: finished.deduplicated?
             })
             |> put_source_conn(final_conn)}
          else
            {:error, _} = error ->
              abort_representation_writer(writer)
              error
          end
        rescue
          exception in @release_on_raise ->
            abort_representation_writer(writer)
            reraise(exception, __STACKTRACE__)
        catch
          kind, reason ->
            abort_representation_writer(writer)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      {:error, _} = error ->
        error
    end
  end

  defp match_blob_identity(expected_digest, expected_length, computed, logical_size) do
    cond do
      computed != expected_digest ->
        {:error,
         VialKeeper.Error.integrity_violation("attachment digest does not match streamed bytes")}

      logical_size != expected_length ->
        {:error,
         VialKeeper.Error.integrity_violation("attachment length does not match streamed bytes")}

      true ->
        :ok
    end
  end

  defp do_upload(uuid, bundle_root, source, max_bytes, opts, admission_class) do
    with {:ok, physical} <-
           AttachmentUpload.phase(:physical_store, fn ->
             do_physical_upload(bundle_root, source, max_bytes, opts)
           end),
         {:ok, protected} <-
           AttachmentUpload.phase(:pending_protection, fn ->
             protect_uploaded_blob(uuid, physical.blob, physical.length, admission_class)
           end) do
      {:ok, Map.merge(physical, protected)}
    end
  end

  defp do_physical_upload(bundle_root, source, max_bytes, opts) do
    case @store.begin_put(StoreRef.bundle_local(bundle_root), max_bytes, Map.new(opts)) do
      {:ok, writer} ->
        try do
          with {:ok, stream_chunks, final_conn} <-
                 normalize_chunk_result(write_all_chunks(writer, source)),
               {:ok, finished} <- @store.finish_put(writer) do
            {:ok,
             %{
               blob: finished.digest,
               length: finished.logical_size,
               stream_chunks: stream_chunks,
               encoding: finished.encoding,
               deduplicated?: finished.deduplicated?
             }
             |> put_source_conn(final_conn)}
          else
            {:error, _} = error ->
              abort_writer(writer)
              error
          end
        rescue
          exception in @release_on_raise ->
            abort_writer(writer)
            reraise(exception, __STACKTRACE__)
        catch
          kind, reason ->
            abort_writer(writer)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end

      {:error, _} = error ->
        error
    end
  end

  defp validate_digest_list(digests) do
    digests
    |> Enum.reduce_while({:ok, []}, &append_validated_digest/2)
    |> reverse_digest_list()
  end

  defp append_validated_digest(digest, {:ok, acc}) do
    case Manifest.validate_digest(digest) do
      {:ok, validated} -> {:cont, {:ok, [validated | acc]}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp reverse_digest_list({:ok, reversed}), do: {:ok, Enum.reverse(reversed)}
  defp reverse_digest_list({:error, _} = error), do: error

  defp durable_blob?(uuid, bundle_root, digest, admission_class) do
    with {:ok, metadata} <-
           metadata_command(
             uuid,
             {:command, :resolve_blob_metadata, %{digest: digest}},
             admission_class
           ),
         length when is_integer(length) and length >= 0 <-
           MapAccess.get_first(metadata, [:logical_size, :length]),
         :ok <- @store.verify(StoreRef.bundle_local(bundle_root), digest, length) do
      true
    else
      _ -> false
    end
  end

  defp missing_digests(uuid, bundle_root, digests, admission_class) do
    Enum.reject(digests, &durable_blob?(uuid, bundle_root, &1, admission_class))
  end

  defp open_blob_representation_under_guard(uuid, digest, read_guard, read_handle, admission_class) do
    with {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         true <- @store.exists?(StoreRef.bundle_local(bundle_root), digest),
         {:ok, meta} <-
           metadata_command(
             uuid,
             {:command, :resolve_blob_metadata, %{digest: digest}},
             admission_class
           ),
         length when is_integer(length) and length >= 0 <-
           MapAccess.get_first(meta, [:logical_size, :length]),
         {:ok, {descriptor, reader}} <-
           @store.open_representation_read(StoreRef.bundle_local(bundle_root), digest),
         :ok <- Representation.validate_route_digest(digest, descriptor),
         read_handle <-
           AttachmentInstr.finish_read_open(read_handle,
             logical_bytes: length,
             payload_length: descriptor.payload_length
           ) do
      finish = once_representation_read_finish(reader, uuid, read_guard, read_handle)

      case BlobRepresentationStream.new(
             Map.put(descriptor, :body, blob_representation_body_stream(reader, finish))
           ) do
        {:ok, stream} ->
          {:ok, stream}

        {:error, _} = error ->
          finish.(outcome: :failed)
          error
      end
    else
      false ->
        _ = AttachmentCoordinator.release(uuid, read_guard)
        {:error, VialKeeper.Error.attachment_blob_not_found("attachment blob is not durable")}

      nil ->
        _ = AttachmentCoordinator.release(uuid, read_guard)
        {:error, VialKeeper.Error.attachment_blob_not_found("attachment blob metadata is invalid")}

      {:error, _} = error ->
        _ = AttachmentCoordinator.release(uuid, read_guard)
        error
    end
  end

  defp blob_body_stream(reader, finish) when is_function(finish, 1) do
    store_body_stream(reader, finish, &@store.read_chunks/1)
  end

  defp blob_representation_body_stream(reader, finish) when is_function(finish, 1) do
    store_body_stream(reader, finish, &@store.read_representation_chunks/1)
  end

  defp store_body_stream(reader, finish, read_fun)
       when is_function(finish, 1) and is_function(read_fun, 1) do
    Stream.resource(
      fn -> {:open, reader} end,
      fn
        {:open, current} ->
          case read_fun.(current) do
            {:ok, chunk} -> {[chunk], {:open, current}}
            {:done, current} -> {:halt, {:done, current}}
            {:error, %VialKeeper.Error{} = error} -> {[{:error, error}], {:failed, current}}
          end

        {:done, current} ->
          {:halt, {:done, current}}

        {:failed, current} ->
          {:halt, {:failed, current}}
      end,
      fn
        {:failed, _} -> finish.(outcome: :failed)
        _ -> finish.([])
      end
    )
  end

  defp once_read_finish(reader, uuid, read_guard, read_handle) do
    once_store_read_finish(reader, uuid, read_guard, read_handle, &@store.close_read/1)
  end

  defp once_representation_read_finish(reader, uuid, read_guard, read_handle) do
    once_store_read_finish(
      reader,
      uuid,
      read_guard,
      read_handle,
      &@store.close_representation_read/1
    )
  end

  defp once_store_read_finish(reader, uuid, read_guard, read_handle, close_fun)
       when is_function(close_fun, 1) do
    finished? = :atomics.new(1, signed: false)

    fn attrs ->
      case :atomics.compare_exchange(finished?, 1, 0, 1) do
        :ok ->
          _ = close_fun.(reader)
          _ = AttachmentCoordinator.release(uuid, read_guard)
          _ = AttachmentInstr.end_read(read_handle, attrs)
          :ok

        _ ->
          :ok
      end
    end
  end

  defp write_all_chunks(writer, source) do
    write_all_source_chunks(writer, source, &@store.write_chunk/2)
  end

  defp write_all_representation_chunks(writer, source) do
    write_all_source_chunks(writer, source, &@store.write_representation_chunk/2)
  end

  defp write_all_source_chunks(writer, %Plug.Conn{} = conn, write_fun) do
    write_conn_chunks(writer, conn, 0, write_fun)
  end

  defp write_all_source_chunks(writer, source, write_fun) when is_function(source, 0) do
    write_fun_chunks(writer, source, 0, write_fun)
  end

  defp write_all_source_chunks(writer, source, write_fun) do
    Enum.reduce_while(source, {:ok, 0}, fn
      chunk, {:ok, count} when is_binary(chunk) ->
        case write_fun.(writer, chunk) do
          :ok -> {:cont, {:ok, count + 1}}
          {:error, _} = error -> {:halt, error}
        end

      {:error, %VialKeeper.Error{}} = error, {:ok, _count} ->
        {:halt, error}

      other, {:ok, _count} ->
        {:halt,
         {:error,
          VialKeeper.Error.invalid_request("attachment chunk must be a binary", %{
            got: inspect(other)
          })}}
    end)
  end

  # Returns the final conn so HTTP callers respond with correct body
  # accounting; responding with a pre-read conn desynchronizes keepalive.
  defp write_conn_chunks(writer, conn, count, write_fun) do
    case Plug.Conn.read_body(conn, @read_chunk_opts) do
      {:ok, "", conn} ->
        {:ok, count, conn}

      {:ok, body, conn} ->
        with :ok <- write_fun.(writer, body) do
          {:ok, count + 1, conn}
        end

      {:more, body, conn} ->
        with :ok <- write_fun.(writer, body) do
          write_conn_chunks(writer, conn, count + 1, write_fun)
        end

      {:error, reason} ->
        {:error,
         VialKeeper.Error.invalid_request("attachment body could not be read", %{
           cause: inspect(reason)
         })}
    end
  end

  defp normalize_chunk_result({:ok, count}), do: {:ok, count, nil}
  defp normalize_chunk_result({:ok, count, conn}), do: {:ok, count, conn}
  defp normalize_chunk_result({:error, _} = error), do: error

  defp put_source_conn(data, %Plug.Conn{} = conn), do: Map.put(data, :source_conn, conn)
  defp put_source_conn(data, nil), do: data

  defp pop_source_conn({:ok, data}) when is_map(data) do
    case Map.pop(data, :source_conn) do
      {nil, data} -> {:ok, data}
      {%Plug.Conn{} = conn, data} -> {:ok, data, conn}
    end
  end

  defp pop_source_conn(other), do: other

  defp write_fun_chunks(writer, fun, count, write_fun) do
    case fun.() do
      {:ok, chunk, next} when is_binary(chunk) and is_function(next, 0) ->
        with :ok <- write_fun.(writer, chunk) do
          write_fun_chunks(writer, next, count + 1, write_fun)
        end

      :done ->
        {:ok, count}

      {:error, %VialKeeper.Error{}} = error ->
        error

      {:error, reason} ->
        {:error,
         VialKeeper.Error.invalid_request("attachment chunk source failed", %{
           cause: inspect(reason)
         })}

      other ->
        {:error,
         VialKeeper.Error.invalid_request("attachment chunk source returned an invalid value", %{
           got: inspect(other)
         })}
    end
  end

  defp protect_uploaded_blob(uuid, digest, logical_size, admission_class) do
    expires_at =
      DateTime.utc_now()
      |> DateTime.add(@pending_protection_hours * 3600, :second)
      |> DateTime.to_iso8601()

    case metadata_command(
           uuid,
           {:command, :protect_pending_blob,
            %{
              digest: digest,
              logical_size: logical_size,
              expires_at: expires_at
            }},
           admission_class
         ) do
      {:ok, protected} ->
        {:ok,
         %{
           blob: digest,
           length: logical_size,
           expires_at: MapAccess.get(protected, :expires_at, expires_at)
         }}

      {:error, _} = error ->
        error
    end
  end

  defp normalize_batch_sources([]),
    do: {:error, VialKeeper.Error.invalid_request("attachment batch must not be empty")}

  defp normalize_batch_sources(sources) do
    sources
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {source, index}, {:ok, acc} ->
      case normalize_batch_source(source, index) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _} = error -> error
    end
  end

  defp normalize_batch_source(%{source: %Plug.Conn{}}, _index),
    do: {:error, VialKeeper.Error.invalid_request("Plug.Conn uploads cannot be batched")}

  defp normalize_batch_source(%{source: source} = descriptor, index) do
    normalize_batch_source_value(source, Map.get(descriptor, :key, index))
  end

  defp normalize_batch_source({_key, %Plug.Conn{}}, _index),
    do: {:error, VialKeeper.Error.invalid_request("Plug.Conn uploads cannot be batched")}

  defp normalize_batch_source({key, source}, _index),
    do: normalize_batch_source_value(source, key)

  defp normalize_batch_source(source, index),
    do: normalize_batch_source_value(source, index)

  defp normalize_batch_source_value(source, key) when is_function(source, 0),
    do: {:ok, %{key: key, source: source}}

  defp normalize_batch_source_value(source, key) do
    if Enumerable.impl_for(source) do
      {:ok, %{key: key, source: source}}
    else
      {:error, VialKeeper.Error.invalid_request("attachment batch source is not enumerable")}
    end
  end

  defp batch_concurrency(uuid, opts) do
    with %{write_limit: limit} when is_integer(limit) and limit > 0 <-
           AttachmentCoordinator.status(uuid),
         requested =
           Keyword.get(opts, :max_concurrency, min(@default_batch_concurrency, limit)),
         true <- is_integer(requested) and requested > 0 and requested <= limit do
      {:ok, requested}
    else
      false ->
        {:error,
         VialKeeper.Error.invalid_request(
           "attachment batch max_concurrency exceeds the database write limit"
         )}

      {:error, _} = error ->
        error

      _ ->
        {:error, VialKeeper.Error.internal_error("attachment write limit is unavailable")}
    end
  end

  defp upload_physical_batch(uuid, bundle_root, sources, concurrency, opts) do
    sources
    # quality:reason batch upload streams CAS under coordinator slots; a timeout would abandon durable writes
    # reach:disable-next-line vial_keeper_unbounded_async_stream -- batch uploads must run to completion
    |> Task.async_stream(
      fn descriptor -> upload_one_batch_source(uuid, bundle_root, descriptor, opts) end,
      max_concurrency: concurrency,
      ordered: true,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, descriptor}}, {:ok, acc} ->
        {:cont, {:ok, [descriptor | acc]}}

      {:ok, {:error, _} = error}, _acc ->
        {:halt, error}

      {:exit, reason}, _acc ->
        {:halt,
         {:error,
          VialKeeper.Error.internal_error("attachment batch worker failed", %{
            cause: inspect(reason)
          })}}
    end)
    |> case do
      {:ok, descriptors} -> {:ok, Enum.reverse(descriptors)}
      {:error, _} = error -> error
    end
  end

  defp upload_one_batch_source(uuid, bundle_root, descriptor, opts) do
    with {:ok, write_guard, max_bytes} <-
           AttachmentUpload.phase(:coordinator_wait, fn ->
             AttachmentCoordinator.acquire_write(uuid)
           end) do
      try do
        AttachmentInstr.write(uuid, fn ->
          result =
            AttachmentUpload.phase(:physical_store, fn ->
              do_physical_upload(bundle_root, descriptor.source, max_bytes, opts)
            end)

          case result do
            {:ok, physical} -> {:ok, Map.put(physical, :key, descriptor.key)}
            {:error, _} = error -> error
          end
        end)
      after
        _ = AttachmentCoordinator.release(uuid, write_guard)
      end
    end
  end

  defp protect_uploaded_blobs(uuid, descriptors, admission_class, opts) do
    maximum = min(VialKeeper.Config.host_limits()[:max_bulk_operations] || 500, 500)

    batch_size =
      Keyword.get(
        opts,
        :protection_batch_size,
        maximum
      )

    if valid_protection_batch_size?(batch_size, maximum) do
      AttachmentUpload.phase(:pending_protection, fn ->
        protect_descriptor_batches(uuid, descriptors, batch_size, admission_class)
      end)
    else
      {:error,
       VialKeeper.Error.invalid_request(
         "attachment protection batch size exceeds the host bulk limit"
       )}
    end
  end

  defp valid_protection_batch_size?(batch_size, maximum)
       when is_integer(batch_size) and batch_size > 0 and batch_size <= maximum,
       do: true

  defp valid_protection_batch_size?(_batch_size, _maximum), do: false

  defp protect_descriptor_batches(uuid, descriptors, batch_size, admission_class) do
    descriptors
    |> Enum.uniq_by(& &1.blob)
    |> Enum.chunk_every(batch_size)
    |> Enum.reduce_while({:ok, %{}}, fn batch, acc ->
      protect_one_batch(uuid, admission_class, batch, acc)
    end)
  end

  defp protect_one_batch(uuid, admission_class, batch, {:ok, expiries}) do
    blobs = Enum.map(batch, &%{digest: &1.blob, logical_size: &1.length})

    case metadata_command(
           uuid,
           {:command, :protect_pending_blobs, %{blobs: blobs}},
           admission_class
         ) do
      {:ok, %{protected: protected}} ->
        {:cont, {:ok, merge_protection_expiries(expiries, protected)}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp merge_protection_expiries(expiries, protected) do
    Enum.reduce(protected, expiries, fn row, acc ->
      Map.put(acc, MapAccess.get(row, :digest), MapAccess.get(row, :expires_at))
    end)
  end

  defp open_stream_under_guard(uuid, ticket_request, read_guard, read_handle) do
    case resolve_ticket_command(uuid, ticket_request) do
      {:ok, ticket} ->
        case @store.open_read(StoreRef.bundle_local(ticket.bundle_path), ticket.blob_digest) do
          {:ok, reader} ->
            read_handle =
              AttachmentInstr.finish_read_open(read_handle, logical_bytes: ticket.logical_size)

            finish = once_read_finish(reader, uuid, read_guard, read_handle)

            {:ok,
             %{
               ticket: ticket,
               content_type: ticket.content_type,
               content_length: ticket.logical_size,
               etag: ~s("#{ticket.blob_digest}"),
               body: blob_body_stream(reader, finish),
               close: fn -> finish.([]) end
             }}

          {:error, _} = error ->
            _ = AttachmentCoordinator.release(uuid, read_guard)
            error
        end

      {:error, _} = error ->
        _ = AttachmentCoordinator.release(uuid, read_guard)
        error
    end
  end

  defp resolve_ticket_command(uuid, ticket_request) do
    with {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, raw} <-
           DatabaseCatalog.command(uuid, {:command, :resolve_attachment_ticket, ticket_request}) do
      case raw do
        %Ticket{} = ticket ->
          {:ok, ticket}

        map when is_map(map) ->
          Ticket.build(
            MapAccess.get(map, :database_uuid, uuid),
            MapAccess.get(map, :bundle_path, bundle_root),
            MapAccess.get(map, :blob_digest) || MapAccess.get(map, :digest),
            MapAccess.get_first(map, [:logical_size, :length]),
            MapAccess.get(map, :content_type),
            MapAccess.get(map, :document_id) || MapAccess.get(map, :id),
            MapAccess.get(map, :revision_id) || MapAccess.get(map, :revision),
            MapAccess.get(map, :attachment_name) || MapAccess.get(map, :name)
          )
      end
    end
  end

  defp materialize_normalized(uuid, references) do
    with {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, metadata_by_digest} <- resolve_digests(uuid, bundle_root, references) do
      Manifest.from_blob_metadata(references, metadata_by_digest)
    end
  end

  defp resolve_digests(uuid, bundle_root, references) do
    digests =
      references
      |> Map.values()
      |> Enum.map(& &1.digest)
      |> Enum.uniq()

    Enum.reduce_while(digests, {:ok, %{}}, fn digest, {:ok, acc} ->
      case resolve_one_digest(uuid, bundle_root, digest) do
        {:ok, metadata} -> {:cont, {:ok, Map.put(acc, digest, metadata)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp resolve_one_digest(uuid, bundle_root, digest) do
    with {:ok, meta} <-
           DatabaseCatalog.command(uuid, {:command, :resolve_blob_metadata, %{digest: digest}}),
         true <- @store.exists?(StoreRef.bundle_local(bundle_root), digest) do
      length = MapAccess.get_first(meta, [:logical_size, :length])

      if is_integer(length) and length >= 0 do
        {:ok, %{length: length, digest: digest}}
      else
        {:error, VialKeeper.Error.attachment_blob_not_found("attachment blob metadata is invalid")}
      end
    else
      false ->
        {:error, VialKeeper.Error.attachment_blob_not_found("attachment blob is not durable")}

      {:error, _} = error ->
        error
    end
  end

  defp normalize_get_request(request) do
    allowed = [:id, :revision, :name, "id", "revision", "name"]

    if Enum.all?(Map.keys(request), &(&1 in allowed)) do
      id = MapAccess.get(request, :id)
      name = MapAccess.get(request, :name)
      revision = MapAccess.get(request, :revision)

      with :ok <- require_string(id, "id"),
           :ok <- require_string(name, "name"),
           :ok <- optional_revision(revision),
           {:ok, validated_name} <- Manifest.validate_name(name) do
        {:ok, %{document_id: id, revision: revision, name: validated_name}}
      end
    else
      {:error, VialKeeper.Error.invalid_request("attachment get contains an unknown field")}
    end
  end

  defp require_string(value, _field) when is_binary(value) and value != "", do: :ok

  defp require_string(_, field),
    do: {:error, VialKeeper.Error.invalid_request("attachment #{field} must be a non-empty string")}

  defp optional_revision(nil), do: :ok
  defp optional_revision(value) when is_binary(value), do: :ok

  defp optional_revision(_),
    do: {:error, VialKeeper.Error.invalid_request("attachment revision must be a string or null")}

  defp ensure_open(uuid) do
    case DatabaseCatalog.open(uuid) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  defp ensure_writable(uuid, admission_class) do
    case DatabaseCatalog.command_as(uuid, admission_class, {:command, :identity, %{}}) do
      {:ok, %{database_kind: :derived}} ->
        {:error,
         VialKeeper.Error.derived_database_read_only(
           "derived databases do not accept attachment writes"
         )}

      {:ok, %{"database_kind" => "derived"}} ->
        {:error,
         VialKeeper.Error.derived_database_read_only(
           "derived databases do not accept attachment writes"
         )}

      {:ok, _identity} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defp abort_writer(writer) do
    @store.abort_put(writer)
  end

  defp abort_representation_writer(writer) do
    @store.abort_put_representation(writer)
  end

  defp run_gc(uuid, bundle_root, admission_class) do
    with {:ok, live} <- collect_live_digests(uuid, admission_class),
         {:ok, expired} <-
           metadata_command(
             uuid,
             {:command, :cleanup_expired_pending_blobs, %{}},
             admission_class
           ),
         {:ok, on_disk} <- @store.list_digests(StoreRef.bundle_local(bundle_root)),
         {:ok, {deleted, bytes_deleted}} <- delete_unreachable(bundle_root, on_disk, live),
         :ok <- @store.cleanup_tmp(StoreRef.bundle_local(bundle_root), tmp_cleanup_cutoff()) do
      {:ok,
       %{
         deleted: length(deleted),
         deleted_digests: deleted,
         live_count: MapSet.size(live),
         expired_pending_removed: MapAccess.get(expired, :removed, 0),
         blobs_deleted: length(deleted),
         bytes_deleted: bytes_deleted
       }}
    end
  end

  defp collect_live_digests(uuid, admission_class),
    do: collect_live_digests(uuid, admission_class, nil, MapSet.new())

  defp collect_live_digests(uuid, admission_class, after_digest, acc) do
    request =
      if is_binary(after_digest) do
        %{after_digest: after_digest}
      else
        %{}
      end

    case metadata_command(
           uuid,
           {:command, :list_live_attachment_digests, request},
           admission_class
         ) do
      {:ok, page} ->
        digests = MapAccess.get(page, :digests) || []
        next = MapAccess.get(page, :next_after_digest)
        acc = Enum.reduce(digests, acc, &MapSet.put(&2, &1))

        if is_binary(next) do
          collect_live_digests(uuid, admission_class, next, acc)
        else
          {:ok, acc}
        end

      {:error, _} = error ->
        error
    end
  end

  defp delete_unreachable(bundle_root, on_disk, live) do
    unreachable = Enum.reject(on_disk, &MapSet.member?(live, &1))

    unreachable
    |> Enum.reduce_while({:ok, {[], 0}}, fn digest, {:ok, {deleted, bytes_deleted}} ->
      invoke_gc_hook({:before_delete, digest})

      with {:ok, %{physical_size: physical_size}} <-
             @store.stat(StoreRef.bundle_local(bundle_root), digest),
           :ok <- @store.delete(StoreRef.bundle_local(bundle_root), digest) do
        {:cont, {:ok, {[digest | deleted], bytes_deleted + physical_size}}}
      else
        {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} ->
          {:cont, {:ok, {deleted, bytes_deleted}}}

        {:error, %VialKeeper.Error{} = error} ->
          {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, {deleted, bytes_deleted}} -> {:ok, {Enum.reverse(deleted), bytes_deleted}}
      {:error, _} = error -> error
    end
  end

  defp tmp_cleanup_cutoff do
    DateTime.utc_now() |> DateTime.add(-@pending_protection_hours * 3600, :second)
  end

  defp invoke_gc_hook(event) do
    case Application.get_env(:vial_keeper, :attachment_gc_hook) do
      fun when is_function(fun, 1) -> fun.(event)
      _ -> :ok
    end
  end

  defp metadata_command(uuid, command, :foreground) do
    DatabaseCatalog.command(uuid, command)
  end

  defp metadata_command(uuid, command, admission_class) do
    DatabaseCatalog.command_as(uuid, admission_class, command)
  end
end
