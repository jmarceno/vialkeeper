defmodule VialKeeper.Search.Engine do
  @moduledoc """
  In-memory inverted index over unicode_words_v1 posting lists.

  Tables are owned by `VialKeeper.Search.Owner`. Rank is a numeric score where
  smaller values sort first, matching previous BM25 page order (more matching
  occurrences come first).
  """

  alias VialKeeper.Search.Tokens

  @type tables :: %{
          meta: :ets.tid(),
          docs: :ets.tid(),
          postings: :ets.tid(),
          tokens: :ets.tid()
        }

  @type hit :: %{id: binary(), rank: float()}

  @spec new_tables() :: tables()
  def new_tables do
    %{
      meta: :ets.new(__MODULE__.Meta, [:set, :public]),
      docs: :ets.new(__MODULE__.Docs, [:set, :public]),
      postings: :ets.new(__MODULE__.Postings, [:set, :public]),
      tokens: :ets.new(__MODULE__.Tokens, [:ordered_set, :public])
    }
  end

  @spec put_index(tables(), map()) :: :ok
  def put_index(tables, definition) when is_map(tables) and is_map(definition) do
    index_id = index_id!(definition)
    :ets.insert(tables.meta, {index_id, definition})
    :ok
  end

  @spec drop_index(tables(), binary()) :: :ok
  def drop_index(tables, index_id) when is_map(tables) and is_binary(index_id) do
    docs = :ets.match_object(tables.docs, {{index_id, :_}, :_})

    Enum.each(docs, fn {{^index_id, doc_id}, tokens} ->
      remove_postings(tables, index_id, doc_id, tokens)
      :ets.delete(tables.docs, {index_id, doc_id})
    end)

    :ets.match_delete(tables.tokens, {{index_id, :_}})
    :ets.delete(tables.meta, index_id)
    :ok
  end

  @spec refresh(tables(), binary(), map() | nil, boolean()) :: :ok
  def refresh(tables, document_id, body, deleted)
      when is_map(tables) and is_binary(document_id) and is_boolean(deleted) do
    Enum.each(:ets.tab2list(tables.meta), fn {index_id, definition} ->
      refresh_index(tables, index_id, definition, document_id, body, deleted)
    end)

    :ok
  end

  @spec search(tables(), binary(), binary(), binary()) ::
          {:ok, [hit()]} | {:error, VialKeeper.Error.t()}
  def search(tables, index_id, text, mode)
      when is_map(tables) and is_binary(index_id) and is_binary(text) and is_binary(mode) do
    case :ets.lookup(tables.meta, index_id) do
      [{^index_id, definition}] ->
        query = Tokens.query(text, definition)

        if query == [] do
          {:error, VialKeeper.Error.invalid_request("full-text search requires at least one term")}
        else
          {:ok, search_query(tables, index_id, query, mode)}
        end

      [] ->
        {:error, VialKeeper.Error.index_not_found("index not found", %{index: index_id})}
    end
  end

  @spec dump(tables()) :: map()
  def dump(tables) when is_map(tables) do
    %{
      meta: :ets.tab2list(tables.meta),
      docs: :ets.tab2list(tables.docs)
    }
  end

  @spec load(tables(), map()) :: :ok
  def load(tables, %{meta: meta, docs: docs})
      when is_map(tables) and is_list(meta) and is_list(docs) do
    Enum.each(meta, &:ets.insert(tables.meta, &1))

    Enum.each(docs, fn {{index_id, doc_id}, tokens} = entry ->
      :ets.insert(tables.docs, entry)
      add_postings(tables, index_id, doc_id, tokens)
    end)

    :ok
  end

  def load(_tables, _other), do: :ok

  defp refresh_index(tables, index_id, definition, document_id, body, deleted) do
    old =
      case :ets.lookup(tables.docs, {index_id, document_id}) do
        [{_, tokens}] -> tokens
        [] -> []
      end

    remove_postings(tables, index_id, document_id, old)
    :ets.delete(tables.docs, {index_id, document_id})

    tokens = if deleted, do: [], else: Tokens.stream(body, definition)

    if tokens != [] do
      :ets.insert(tables.docs, {{index_id, document_id}, tokens})
      add_postings(tables, index_id, document_id, tokens)
    end

    :ok
  end

  defp add_postings(tables, index_id, document_id, tokens) do
    tokens
    |> Enum.with_index()
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.each(fn {token, positions} ->
      :ets.insert(tables.tokens, {{index_id, token}})
      key = {index_id, token}
      postings = posting_map(tables, key)

      :ets.insert(
        tables.postings,
        {key, Map.put(postings, document_id, {length(positions), positions})}
      )
    end)
  end

  defp remove_postings(tables, index_id, document_id, tokens) do
    Enum.each(Enum.uniq(tokens), &remove_token_posting(tables, index_id, document_id, &1))
  end

  defp remove_token_posting(tables, index_id, document_id, token) do
    key = {index_id, token}

    case :ets.lookup(tables.postings, key) do
      [{^key, postings}] ->
        write_or_drop_postings(tables, key, index_id, token, Map.delete(postings, document_id))

      [] ->
        :ok
    end
  end

  defp write_or_drop_postings(tables, key, index_id, token, updated) do
    if updated == %{} do
      :ets.delete(tables.postings, key)
      maybe_drop_token(tables, index_id, token)
    else
      :ets.insert(tables.postings, {key, updated})
    end
  end

  defp maybe_drop_token(tables, index_id, token) do
    case :ets.lookup(tables.postings, {index_id, token}) do
      [] -> :ets.delete(tables.tokens, {index_id, token})
      _ -> :ok
    end
  end

  defp posting_map(tables, key) do
    case :ets.lookup(tables.postings, key) do
      [{^key, postings}] -> postings
      [] -> %{}
    end
  end

  defp search_query(tables, index_id, query, "any") do
    query
    |> Enum.reduce(%{}, fn token, acc ->
      merge_hits(acc, exact_postings(tables, index_id, token))
    end)
    |> hits()
  end

  defp search_query(tables, index_id, query, "phrase") do
    tables
    |> exact_postings(index_id, hd(query))
    |> Enum.reduce(%{}, fn {document_id, {tf, positions}}, acc ->
      if phrase_at?(tables, index_id, document_id, query, positions),
        do: Map.put(acc, document_id, tf),
        else: acc
    end)
    |> hits()
  end

  defp search_query(tables, index_id, query, "prefix") do
    query
    |> Enum.map(&prefix_doc_scores(tables, index_id, &1))
    |> intersect_scores()
    |> hits()
  end

  defp search_query(tables, index_id, query, _mode) do
    query
    |> Enum.map(&exact_doc_scores(tables, index_id, &1))
    |> intersect_scores()
    |> hits()
  end

  defp exact_postings(tables, index_id, token), do: posting_map(tables, {index_id, token})

  defp exact_doc_scores(tables, index_id, token) do
    Map.new(exact_postings(tables, index_id, token), fn {document_id, {tf, _positions}} ->
      {document_id, tf}
    end)
  end

  defp prefix_doc_scores(tables, index_id, prefix) do
    prefix
    |> prefix_tokens(tables, index_id)
    |> Enum.reduce(%{}, fn token, acc ->
      merge_hits(acc, exact_postings(tables, index_id, token))
    end)
  end

  defp prefix_tokens(prefix, tables, index_id) do
    start = {index_id, prefix}

    acc =
      case :ets.lookup(tables.tokens, start) do
        [{^start}] -> [prefix]
        _ -> []
      end

    collect_prefix_tokens(tables.tokens, start, index_id, prefix, acc)
  end

  defp collect_prefix_tokens(table, key, index_id, prefix, acc) do
    case :ets.next(table, key) do
      {^index_id, token} = next ->
        if String.starts_with?(token, prefix) do
          collect_prefix_tokens(table, next, index_id, prefix, [token | acc])
        else
          Enum.reverse(acc)
        end

      _ ->
        Enum.reverse(acc)
    end
  end

  defp merge_hits(acc, postings) do
    Enum.reduce(postings, acc, fn {document_id, {tf, _positions}}, acc ->
      Map.update(acc, document_id, tf, &(&1 + tf))
    end)
  end

  defp intersect_scores([]), do: %{}
  defp intersect_scores([first | rest]), do: Enum.reduce(rest, first, &intersect_pair/2)

  defp intersect_pair(left, right) do
    Map.new(for {id, left_tf} <- left, Map.has_key?(right, id), do: {id, left_tf + right[id]})
  end

  defp phrase_at?(tables, index_id, document_id, [_first | rest], start_positions) do
    rest
    |> Enum.with_index(1)
    |> Enum.all?(&phrase_token_at?(tables, index_id, document_id, start_positions, &1))
  end

  defp phrase_token_at?(tables, index_id, document_id, start_positions, {token, offset}) do
    case Map.get(exact_postings(tables, index_id, token), document_id) do
      {_tf, positions} ->
        Enum.any?(start_positions, fn start -> (start + offset) in positions end)

      nil ->
        false
    end
  end

  defp hits(scores) do
    scores
    |> Enum.map(fn {id, tf} -> %{id: id, rank: -1.0 * tf} end)
    |> Enum.sort_by(fn %{id: id, rank: rank} -> {rank, id} end)
  end

  defp index_id!(definition) do
    case VialKeeper.MapAccess.get(definition, :index_id) do
      id when is_binary(id) -> id
      _ -> raise ArgumentError, "full-text index definition requires index_id"
    end
  end
end
