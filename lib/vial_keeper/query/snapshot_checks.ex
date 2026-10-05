defmodule VialKeeper.Query.SnapshotChecks do
  @moduledoc """
  Public query checks that depend on the database's current identity.

  The storage layer runs them inside the query's own snapshot, against the
  identity read in that snapshot, so a query needs one trip to its database
  and a bookmark is validated against exactly the state the page is read from.
  """

  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Query.BookmarkCodec

  @default_limit 50
  @default_max_limit 500

  @doc """
  Checks a normalized public query against `identity` and resumes its bookmark.

  Returns the request with `:after_id`, `:after_ordering` and
  `:bookmark_payload` set when it carries a bookmark.

  `changed_after?` reports whether any change row exists above a visible
  sequence. It is only called for a bookmark read in an earlier run.
  """
  @spec admit(map(), map(), (non_neg_integer() -> {:ok, boolean()} | {:error, Error.t()})) ::
          {:ok, map()} | {:error, Error.t()}
  def admit(request, identity, changed_after?)
      when is_map(request) and is_map(identity) and is_function(changed_after?, 1) do
    with :ok <- check_limit(request, identity) do
      resume_bookmark(request, identity, changed_after?)
    end
  end

  @doc "Rejects a page limit above the database's configured maximum."
  @spec check_limit(map(), map()) :: :ok | {:error, Error.t()}
  def check_limit(request, identity) when is_map(request) and is_map(identity) do
    limit = MapAccess.get(request, :limit) || @default_limit

    max =
      get_in(identity, [:config, "queries", "max_limit"]) || @default_max_limit

    if limit <= max,
      do: :ok,
      else: {:error, Error.resource_limit("query limit exceeds the database configuration")}
  end

  defp resume_bookmark(request, identity, changed_after?) do
    case MapAccess.get(request, :bookmark) do
      nil ->
        {:ok, request}

      bookmark ->
        with {:ok, decoded} <-
               BookmarkCodec.decode(bookmark, %{"query_fingerprint" => request.fingerprint}),
             :ok <- check_bookmark_sequence(decoded, identity, changed_after?) do
          {:ok,
           request
           |> Map.put(:after_id, decoded.last_id)
           |> Map.put(:after_ordering, decoded.ordering_key)
           |> Map.put(:bookmark_payload, decoded)}
        end
    end
  end

  # A bookmark holds the data version its page was read at; any committed
  # document write since then makes it stale. A bookmark from an earlier run
  # (its version is below this run's base) stays current while no change row
  # exists above the visible sequence it was read at and compaction has not
  # moved the floor past it.
  defp check_bookmark_sequence(%{sequence: sequence} = bookmark, identity, changed_after?) do
    case MapAccess.get(identity, :data_version) do
      ^sequence -> :ok
      _current -> check_earlier_run(bookmark, identity, changed_after?)
    end
  end

  defp check_earlier_run(%{sequence: sequence, visible: visible}, identity, changed_after?)
       when is_integer(visible) do
    base = MapAccess.get(identity, :data_version_base)
    floor = MapAccess.get(identity, :retention_floor_sequence) || 0

    if is_integer(base) and sequence < base and floor <= visible do
      case changed_after?.(visible) do
        {:ok, false} -> :ok
        {:ok, true} -> stale()
        {:error, _reason} = error -> error
      end
    else
      stale()
    end
  end

  defp check_earlier_run(_bookmark, _identity, _changed_after?), do: stale()

  defp stale, do: {:error, Error.bookmark_stale("bookmark sequence is no longer current")}
end
