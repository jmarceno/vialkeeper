defmodule VialKeeper.Query.FullText do
  @moduledoc "Storage-neutral unicode_words_v1 tokenization and search."

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess

  @spec tokens(binary(), atom()) :: [binary()]
  def tokens(text, diacritics \\ :preserve) when is_binary(text) do
    text
    |> String.to_charlist()
    |> Enum.reduce({[], []}, fn grapheme, {tokens, current} ->
      grapheme = <<grapheme::utf8>>

      if token_grapheme?(grapheme) do
        {tokens, [normalize_grapheme(grapheme, diacritics) | current]}
      else
        flush(tokens, current)
      end
    end)
    |> then(fn {tokens, current} -> Enum.reverse(elem(flush(tokens, current), 0)) end)
  end

  @spec matches?(map(), map(), binary()) :: boolean()
  def matches?(body, definition, text) do
    fields = MapAccess.get(definition, :fields, [])

    tokenization = MapAccess.get(definition, :tokenization, %{})

    diacritics =
      if MapAccess.get(tokenization, :diacritics) in ["remove", :remove],
        do: :remove,
        else: :preserve

    query = tokens(text, diacritics)

    values =
      Enum.flat_map(fields, fn field ->
        path = if is_binary(field), do: field, else: MapAccess.get(field, :path)

        case Pointer.get(body, path) do
          {:ok, value} when is_binary(value) -> tokens(value, diacritics)
          _ -> []
        end
      end)

    case query do
      [] ->
        false

      _ ->
        query_matches?(query, values, MapAccess.get(definition, :mode, "all"))
    end
  end

  defp query_matches?(query, values, "phrase"), do: phrase?(values, query)

  defp query_matches?(query, values, "prefix") do
    Enum.all?(query, fn query_token ->
      Enum.any?(values, &String.starts_with?(&1, query_token))
    end)
  end

  defp query_matches?(query, values, mode) do
    initial = mode == "all"
    Enum.reduce_while(query, initial, &reduce_query_token(&1, &2, values, mode))
  end

  defp reduce_query_token(token, _matched, values, "any") do
    if token in values, do: {:halt, true}, else: {:cont, false}
  end

  defp reduce_query_token(token, matched, values, _mode) do
    if token in values, do: {:cont, matched}, else: {:halt, false}
  end

  defp phrase?(values, query) do
    values |> Enum.chunk_every(length(query), 1, :discard) |> Enum.any?(&(&1 == query))
  end

  defp flush(tokens, []), do: {tokens, []}

  defp flush(tokens, current),
    do: {[current |> Enum.reverse() |> IO.iodata_to_binary() | tokens], []}

  defp token_grapheme?(grapheme) do
    Regex.match?(~r/^[\p{L}\p{N}\p{Co}]$/u, grapheme)
  end

  defp normalize_grapheme(grapheme, :preserve), do: String.downcase(grapheme)

  defp normalize_grapheme(grapheme, :remove),
    do: grapheme |> String.normalize(:nfd) |> String.replace(~r/\p{M}/u, "") |> String.downcase()
end
