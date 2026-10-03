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
  """
  @spec admit(map(), map()) :: {:ok, map()} | {:error, Error.t()}
  def admit(request, identity) when is_map(request) and is_map(identity) do
    with :ok <- check_limit(request, identity) do
      resume_bookmark(request, identity)
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

  defp resume_bookmark(request, identity) do
    case MapAccess.get(request, :bookmark) do
      nil ->
        {:ok, request}

      bookmark ->
        with {:ok, decoded} <-
               BookmarkCodec.decode(bookmark, %{"query_fingerprint" => request.fingerprint}),
             :ok <- check_bookmark_sequence(decoded.sequence, identity) do
          {:ok,
           request
           |> Map.put(:after_id, decoded.last_id)
           |> Map.put(:after_ordering, decoded.ordering_key)
           |> Map.put(:bookmark_payload, decoded)}
        end
    end
  end

  defp check_bookmark_sequence(sequence, identity) do
    case MapAccess.get(identity, :current_sequence) do
      ^sequence -> :ok
      _current -> {:error, Error.bookmark_stale("bookmark sequence is no longer current")}
    end
  end
end
