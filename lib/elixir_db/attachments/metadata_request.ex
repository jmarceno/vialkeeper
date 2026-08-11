defmodule ElixirDB.Attachments.MetadataRequest do
  @moduledoc """
  Shared request decoding for attachment metadata operations.

  Pure helpers used by Memory and SQLite attachment-metadata ports so digest,
  paging, and revision-resolution rules stay identical across backends.
  """

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.MapAccess

  @default_live_digest_page_size 4096
  @digest_pattern ~r/^[0-9a-f]{64}$/

  @doc "Validates `digest` or `blob` from a metadata request."
  @spec request_digest(map()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def request_digest(request) when is_map(request) do
    digest = MapAccess.get(request, :digest) || MapAccess.get(request, :blob)
    Manifest.validate_digest(digest)
  end

  @doc "Validates non-negative `logical_size` or `length` from a request."
  @spec request_logical_size(map()) :: {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def request_logical_size(request) when is_map(request) do
    size = MapAccess.get_first(request, [:logical_size, :length])

    if is_integer(size) and size >= 0,
      do: {:ok, size},
      else: {:error, ElixirDB.Error.invalid_request("logical_size must be a non-negative integer")}
  end

  @doc "Validates every digest in a list."
  @spec validate_digest_list([term()]) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_digest_list(digests) when is_list(digests) do
    Enum.reduce_while(digests, :ok, fn digest, :ok ->
      case Manifest.validate_digest(digest) do
        {:ok, _} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  @doc """
  Resolves a live-digest page limit capped by `max_page_size`.

  Defaults to `#{@default_live_digest_page_size}` when `max_page_size` is omitted.
  """
  @spec page_limit(term()) :: pos_integer()
  def page_limit(limit), do: page_limit(limit, @default_live_digest_page_size)

  @spec page_limit(term(), pos_integer()) :: pos_integer()
  def page_limit(nil, max_page_size) when is_integer(max_page_size) and max_page_size > 0,
    do: max_page_size

  def page_limit(limit, max_page_size)
      when is_integer(limit) and limit > 0 and is_integer(max_page_size) and max_page_size > 0,
      do: min(limit, max_page_size)

  def page_limit(_limit, max_page_size) when is_integer(max_page_size) and max_page_size > 0,
    do: max_page_size

  @doc "Validates an optional `after_digest` cursor."
  @spec validate_after_digest(term()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_after_digest(nil), do: :ok

  def validate_after_digest(digest) when is_binary(digest) do
    if Regex.match?(@digest_pattern, digest),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("after_digest must be lowercase SHA-256 hex")}
  end

  def validate_after_digest(_),
    do: {:error, ElixirDB.Error.invalid_request("after_digest must be lowercase SHA-256 hex")}

  @doc "Resolves optional `now` override to a second-truncated UTC DateTime."
  @spec cleanup_now(map()) :: {:ok, DateTime.t()} | {:error, ElixirDB.Error.t()}
  def cleanup_now(request) when is_map(request) do
    case MapAccess.get(request, :now) do
      nil ->
        {:ok, DateTime.utc_now() |> DateTime.truncate(:second)}

      %DateTime{} = dt ->
        {:ok, DateTime.truncate(dt, :second)}

      value when is_binary(value) ->
        case DateTime.from_iso8601(value) do
          {:ok, dt, _} -> {:ok, DateTime.truncate(dt, :second)}
          _ -> {:error, ElixirDB.Error.invalid_request("now must be an RFC3339 timestamp")}
        end

      _ ->
        {:error, ElixirDB.Error.invalid_request("now must be an RFC3339 timestamp")}
    end
  end

  @doc """
  Resolves a revision id from a document fact and optional request revision.

  `nil` revision selects the document winning revision when present.
  """
  @spec resolve_revision_id(map(), binary() | nil) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def resolve_revision_id(%{winning_revision: nil}, nil),
    do: {:error, ElixirDB.Error.document_not_found("document has no winning revision")}

  def resolve_revision_id(%{winning_revision: winning}, nil) when is_binary(winning),
    do: {:ok, winning}

  def resolve_revision_id(_doc, revision_id) when is_binary(revision_id) and revision_id != "",
    do: {:ok, revision_id}

  def resolve_revision_id(_doc, _revision_id),
    do: {:error, ElixirDB.Error.invalid_request("revision must be a string or null")}

  @doc """
  Collects digests to remove from a pending-protection request.

  Prefers `:digests` list; otherwise a single `:digest`/`:blob`. Invalid single
  digests become an empty list so callers can still validate an empty drop.
  """
  @spec pending_digests_from_request(map()) :: [binary()]
  def pending_digests_from_request(request) when is_map(request) do
    case MapAccess.get(request, :digests) do
      list when is_list(list) ->
        list

      _ ->
        case request_digest(request) do
          {:ok, digest} -> [digest]
          {:error, _} -> []
        end
    end
  end
end
