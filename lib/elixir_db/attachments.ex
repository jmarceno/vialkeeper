defmodule ElixirDB.Attachments do
  @moduledoc """
  Storage-neutral attachment orchestration for HTTP and local mutation paths.

  Coordinates `AttachmentCoordinator` guards, short owner metadata commands, and
  `FilesystemStore` byte I/O. Never holds database admission or `DatabaseOwner`
  while attachment bytes are transferred.
  """

  alias ElixirDB.Attachments.{FilesystemStore, Manifest, Ticket}
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}

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

  @type guard :: reference() | nil
  @type chunk_source :: Plug.Conn.t() | Enumerable.t() | (-> term())

  @doc """
  Streams an upload into the database-local CAS and protects the digest.

  `source` may be a `Plug.Conn`, an enumerable of binaries, or a zero-arity
  function returning `{:ok, binary(), next_fun}`, `:done`, or `{:error, error}`.
  """
  @spec upload_stream(binary(), chunk_source(), keyword()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def upload_stream(uuid, source, opts \\ []) when is_binary(uuid) and is_list(opts) do
    with :ok <- ensure_open(uuid),
         {:ok, bundle_root} <- DatabaseCatalog.bundle_root(uuid),
         {:ok, write_guard, max_bytes} <- AttachmentCoordinator.acquire_write(uuid) do
      try do
        do_upload(uuid, bundle_root, source, max_bytes, opts)
      after
        _ = AttachmentCoordinator.release(uuid, write_guard)
      end
    end
  end

  @doc """
  Resolves an attachment ticket under a read guard and opens a store reader.

  The returned map includes a lazy `body` enumerable. Consuming or abandoning
  the enumerable releases the reader and read guard.
  """
  @spec open_stream(binary(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def open_stream(uuid, request) when is_binary(uuid) and is_map(request) do
    with :ok <- ensure_open(uuid),
         {:ok, ticket_request} <- normalize_get_request(request),
         {:ok, read_guard} <- AttachmentCoordinator.acquire_read(uuid) do
      try do
        open_stream_under_guard(uuid, ticket_request, read_guard)
      rescue
        exception in @release_on_raise ->
          _ = AttachmentCoordinator.release(uuid, read_guard)
          reraise(exception, __STACKTRACE__)
      catch
        kind, reason ->
          _ = AttachmentCoordinator.release(uuid, read_guard)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    end
  end

  @doc "Resolves a ticket without opening the byte stream."
  @spec resolve_ticket(binary(), map()) ::
          {:ok, Ticket.t(), guard()} | {:error, ElixirDB.Error.t()}
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
          {:ok, :omitted | Manifest.t(), guard()} | {:error, ElixirDB.Error.t()}
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
    do: {:error, ElixirDB.Error.invalid_request("attachments must be an object")}

  @doc """
  Materializes client references into a complete manifest without acquiring a guard.

  Callers MUST already hold a reference guard for the database.
  """
  @spec materialize_references(binary(), map()) ::
          {:ok, Manifest.t()} | {:error, ElixirDB.Error.t()}
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

  @doc "Attachment GC placeholder for a later wave."
  @spec gc(binary()) :: {:error, ElixirDB.Error.t()}
  def gc(_uuid),
    do: {:error, ElixirDB.Error.internal_error("attachment gc is not implemented")}

  defp do_upload(uuid, bundle_root, source, max_bytes, opts) do
    case @store.begin_put(bundle_root, max_bytes, Map.new(opts)) do
      {:ok, writer} ->
        try do
          with :ok <- write_all_chunks(writer, source),
               {:ok, %{digest: digest, logical_size: logical_size}} <- @store.finish_put(writer) do
            protect_uploaded_blob(uuid, digest, logical_size)
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

  defp write_all_chunks(writer, %Plug.Conn{} = conn) do
    write_conn_chunks(writer, conn)
  end

  defp write_all_chunks(writer, source) when is_function(source, 0) do
    write_fun_chunks(writer, source)
  end

  defp write_all_chunks(writer, source) do
    Enum.reduce_while(source, :ok, fn
      chunk, :ok when is_binary(chunk) ->
        case @store.write_chunk(writer, chunk) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end

      other, :ok ->
        {:halt,
         {:error,
          ElixirDB.Error.invalid_request("attachment chunk must be a binary", %{
            got: inspect(other)
          })}}
    end)
  end

  defp write_conn_chunks(writer, conn) do
    case Plug.Conn.read_body(conn, @read_chunk_opts) do
      {:ok, body, _conn} ->
        if body == "", do: :ok, else: @store.write_chunk(writer, body)

      {:more, body, conn} ->
        with :ok <- @store.write_chunk(writer, body) do
          write_conn_chunks(writer, conn)
        end

      {:error, reason} ->
        {:error,
         ElixirDB.Error.invalid_request("attachment body could not be read", %{
           cause: inspect(reason)
         })}
    end
  end

  defp write_fun_chunks(writer, fun) do
    case fun.() do
      {:ok, chunk, next} when is_binary(chunk) and is_function(next, 0) ->
        with :ok <- @store.write_chunk(writer, chunk) do
          write_fun_chunks(writer, next)
        end

      :done ->
        :ok

      {:error, %ElixirDB.Error{}} = error ->
        error

      {:error, reason} ->
        {:error,
         ElixirDB.Error.invalid_request("attachment chunk source failed", %{
           cause: inspect(reason)
         })}

      other ->
        {:error,
         ElixirDB.Error.invalid_request("attachment chunk source returned an invalid value", %{
           got: inspect(other)
         })}
    end
  end

  defp protect_uploaded_blob(uuid, digest, logical_size) do
    expires_at =
      DateTime.utc_now()
      |> DateTime.add(@pending_protection_hours * 3600, :second)
      |> DateTime.to_iso8601()

    case DatabaseCatalog.command(
           uuid,
           {:command, :protect_pending_blob,
            %{
              digest: digest,
              logical_size: logical_size,
              expires_at: expires_at
            }}
         ) do
      {:ok, _} ->
        {:ok, %{blob: digest, length: logical_size, expires_at: expires_at}}

      {:error, _} = error ->
        error
    end
  end

  defp open_stream_under_guard(uuid, ticket_request, read_guard) do
    case resolve_ticket_command(uuid, ticket_request) do
      {:ok, ticket} ->
        case @store.open_read(ticket.bundle_path, ticket.blob_digest) do
          {:ok, reader} ->
            {:ok, stream_result(uuid, ticket, reader, read_guard)}

          {:error, _} = error ->
            _ = AttachmentCoordinator.release(uuid, read_guard)
            error
        end

      {:error, _} = error ->
        _ = AttachmentCoordinator.release(uuid, read_guard)
        error
    end
  end

  defp stream_result(uuid, ticket, reader, read_guard) do
    cleanup = fn ->
      _ = @store.close_read(reader)
      _ = AttachmentCoordinator.release(uuid, read_guard)
      :ok
    end

    body =
      Stream.resource(
        fn -> {:open, reader} end,
        fn
          {:open, current} ->
            case @store.read_chunks(current) do
              {:ok, chunk} -> {[chunk], {:open, current}}
              {:done, current} -> {:halt, {:done, current}}
              {:error, %ElixirDB.Error{} = error} -> {[error: error], {:failed, current}}
            end

          {:done, current} ->
            {:halt, {:done, current}}

          {:failed, current} ->
            {:halt, {:failed, current}}
        end,
        fn
          {:open, current} ->
            _ = @store.close_read(current)
            _ = AttachmentCoordinator.release(uuid, read_guard)

          {:done, current} ->
            _ = @store.close_read(current)
            _ = AttachmentCoordinator.release(uuid, read_guard)

          {:failed, current} ->
            _ = @store.close_read(current)
            _ = AttachmentCoordinator.release(uuid, read_guard)

          _ ->
            _ = AttachmentCoordinator.release(uuid, read_guard)
        end
      )

    %{
      ticket: ticket,
      content_type: ticket.content_type,
      content_length: ticket.logical_size,
      etag: ~s("#{ticket.blob_digest}"),
      body: body,
      close: cleanup
    }
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
            MapAccess.get(map, :logical_size) || MapAccess.get(map, :length),
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
         true <- @store.exists?(bundle_root, digest) do
      length = MapAccess.get(meta, :logical_size) || MapAccess.get(meta, :length)

      if is_integer(length) and length >= 0 do
        {:ok, %{length: length, digest: digest}}
      else
        {:error, ElixirDB.Error.attachment_blob_not_found("attachment blob metadata is invalid")}
      end
    else
      false ->
        {:error, ElixirDB.Error.attachment_blob_not_found("attachment blob is not durable")}

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
      {:error, ElixirDB.Error.invalid_request("attachment get contains an unknown field")}
    end
  end

  defp require_string(value, _field) when is_binary(value) and value != "", do: :ok

  defp require_string(_, field),
    do: {:error, ElixirDB.Error.invalid_request("attachment #{field} must be a non-empty string")}

  defp optional_revision(nil), do: :ok
  defp optional_revision(value) when is_binary(value), do: :ok

  defp optional_revision(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment revision must be a string or null")}

  defp ensure_open(uuid) do
    case DatabaseCatalog.open(uuid) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  defp abort_writer(writer) do
    @store.abort_put(writer)
  end
end
