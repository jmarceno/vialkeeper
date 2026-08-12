defmodule ElixirDB.Attachments.Orchestration do
  @moduledoc """
  Shared attachment-metadata orchestration helpers.

  Owns pending TTL, live-digest union/paging, expiry selection, and
  reachability checks over caller-supplied fact lists. Backends only load and
  persist metadata rows.
  """

  alias ElixirDB.Attachments.MetadataRequest
  alias ElixirDB.MapAccess

  @pending_ttl_seconds 86_400
  @live_digest_page_size 4096

  @doc "Pending-blob protection lifetime in seconds."
  @spec pending_ttl_seconds() :: pos_integer()
  def pending_ttl_seconds, do: @pending_ttl_seconds

  @doc "Default live-digest page size shared by all backends."
  @spec live_digest_page_size() :: pos_integer()
  def live_digest_page_size, do: @live_digest_page_size

  @doc "Builds a pending-blob metadata row renewing TTL from `now`."
  @spec pending_row(binary(), non_neg_integer(), DateTime.t()) :: map()
  def pending_row(digest, logical_size, %DateTime{} = now)
      when is_binary(digest) and is_integer(logical_size) and logical_size >= 0 do
    now = DateTime.truncate(now, :second)
    expires_at = DateTime.add(now, @pending_ttl_seconds, :second)
    now_iso = DateTime.to_iso8601(now)
    expires_iso = DateTime.to_iso8601(expires_at)

    %{
      digest: digest,
      logical_size: logical_size,
      length: logical_size,
      expires_at: expires_iso,
      updated_at: now_iso
    }
  end

  @type pending_snapshot :: %{
          required(:digest) => binary(),
          required(:logical_size) => non_neg_integer(),
          required(:expires_at) => binary(),
          required(:updated_at) => binary()
        }

  @doc "Builds the normalized pending-blob shape used by ports and integrity snapshots."
  @spec pending_snapshot(binary(), non_neg_integer(), binary(), binary()) :: pending_snapshot()
  def pending_snapshot(digest, logical_size, expires_at, updated_at) do
    %{
      digest: digest,
      logical_size: logical_size,
      expires_at: expires_at,
      updated_at: updated_at
    }
  end

  @doc "True when pending metadata expires strictly after `now`."
  @spec pending_unexpired?(map(), DateTime.t()) :: boolean()
  def pending_unexpired?(meta, %DateTime{} = now) when is_map(meta) do
    case MapAccess.get(meta, :expires_at) do
      expires when is_binary(expires) ->
        case DateTime.from_iso8601(expires) do
          {:ok, expires_dt, _} -> DateTime.compare(expires_dt, now) == :gt
          _ -> false
        end

      _ ->
        false
    end
  end

  @doc "Union of retained digests and unexpired pending digests, sorted."
  @spec union_live_digests([binary()], [map()], DateTime.t()) :: [binary()]
  def union_live_digests(retained, pending_rows, %DateTime{} = now)
      when is_list(retained) and is_list(pending_rows) do
    pending =
      pending_rows
      |> Enum.filter(&pending_unexpired?(&1, now))
      |> Enum.map(&digest_of/1)
      |> Enum.filter(&is_binary/1)

    retained
    |> Enum.filter(&is_binary/1)
    |> Kernel.++(pending)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc "Pages a sorted digest list with an optional exclusive after-cursor."
  @spec page_digests([binary()], binary() | nil, term()) ::
          {:ok, %{digests: [binary()], next_after_digest: binary() | nil}}
          | {:error, ElixirDB.Error.t()}
  def page_digests(digests, after_digest, limit) when is_list(digests) do
    with :ok <- MetadataRequest.validate_after_digest(after_digest) do
      page_limit = MetadataRequest.page_limit(limit, @live_digest_page_size)
      window = drop_after(digests, after_digest)
      page = Enum.take(window, page_limit)

      next_after =
        if length(window) > page_limit do
          List.last(page)
        else
          nil
        end

      {:ok, %{digests: page, next_after_digest: next_after}}
    end
  end

  @doc "Pending rows whose expiry is at or before `now`."
  @spec expired_pending_digests([map()], DateTime.t()) :: [binary()]
  def expired_pending_digests(pending_rows, %DateTime{} = now) when is_list(pending_rows) do
    pending_rows
    |> Enum.reject(&pending_unexpired?(&1, now))
    |> Enum.map(&digest_of/1)
    |> Enum.filter(&is_binary/1)
  end

  @doc "Keeps only unexpired pending rows."
  @spec keep_unexpired_pending([map()], DateTime.t()) :: [map()]
  def keep_unexpired_pending(pending_rows, %DateTime{} = now) when is_list(pending_rows) do
    Enum.filter(pending_rows, &pending_unexpired?(&1, now))
  end

  @doc """
  Ensures every manifest digest is reachable via `lookup_fun`.

  `lookup_fun` receives a digest and returns `{:ok, size}`, `:ok`, or
  `{:error, error}`.
  """
  @spec ensure_reachable(map(), (binary() -> :ok | {:ok, term()} | {:error, ElixirDB.Error.t()})) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_reachable(manifest, lookup_fun)
      when is_map(manifest) and is_function(lookup_fun, 1) do
    if map_size(manifest) == 0 do
      :ok
    else
      check_digests(manifest_digests(manifest), lookup_fun)
    end
  end

  @doc "Digests referenced by a revision attachment manifest."
  @spec manifest_digests(map()) :: [binary()]
  def manifest_digests(manifest) when is_map(manifest) do
    manifest
    |> Map.values()
    |> Enum.map(&digest_of/1)
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  defp check_digests(digests, lookup_fun) do
    Enum.reduce_while(digests, :ok, fn digest, :ok ->
      case lookup_fun.(digest) do
        :ok -> {:cont, :ok}
        {:ok, _} -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp digest_of(entry) when is_map(entry),
    do: MapAccess.get(entry, :digest) || MapAccess.get(entry, "digest")

  defp digest_of(digest) when is_binary(digest), do: digest
  defp digest_of(_), do: nil

  defp drop_after(digests, nil), do: digests

  defp drop_after(digests, after_digest) when is_binary(after_digest) do
    Enum.drop_while(digests, &(&1 <= after_digest))
  end
end
