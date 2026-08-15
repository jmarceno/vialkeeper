defmodule VialKeeper.Query.FullText do
  @moduledoc """
  Storage-neutral unicode_words_v1 tokenization and search.

  Tokenization walks UTF-8 code points. ASCII letters and digits are classified
  without Unicode regex so large English bodies stay cheap; non-ASCII code
  points keep the same `\\p{L}\\p{N}\\p{Co}` rules as QUERY-015.
  """

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess

  @token_char_regex ~r/^[\p{L}\p{N}\p{Co}]$/u
  @mark_regex ~r/\p{M}/u
  @compile {:inline, ascii_token_char?: 1, ascii_downcase_byte: 1}

  @spec tokens(binary(), atom()) :: [binary()]
  def tokens(text, diacritics \\ :preserve) when is_binary(text) do
    text
    |> tokenize(diacritics, [], [], [])
    |> Enum.reverse()
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

  defp tokenize(<<>>, _diacritics, tokens, parts, ascii) do
    flush_token(tokens, parts, ascii)
  end

  defp tokenize(<<byte, rest::binary>>, diacritics, tokens, parts, ascii) when byte <= 127 do
    if ascii_token_char?(byte) do
      tokenize(rest, diacritics, tokens, parts, [ascii_downcase_byte(byte) | ascii])
    else
      tokenize(rest, diacritics, flush_token(tokens, parts, ascii), [], [])
    end
  end

  defp tokenize(<<cp::utf8, rest::binary>>, diacritics, tokens, parts, ascii) do
    grapheme = <<cp::utf8>>

    if token_grapheme?(grapheme) do
      parts = [normalize_grapheme(grapheme, diacritics) | take_ascii(parts, ascii)]
      tokenize(rest, diacritics, tokens, parts, [])
    else
      tokenize(rest, diacritics, flush_token(tokens, parts, ascii), [], [])
    end
  end

  defp tokenize(_invalid_utf8, _diacritics, _tokens, _parts, _ascii) do
    raise ArgumentError, "invalid UTF-8 string"
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

  defp flush_token(tokens, [], []), do: tokens

  defp flush_token(tokens, [], ascii), do: [ascii_token(ascii) | tokens]

  defp flush_token(tokens, parts, ascii) do
    [IO.iodata_to_binary(Enum.reverse(take_ascii(parts, ascii))) | tokens]
  end

  defp take_ascii(parts, []), do: parts
  defp take_ascii(parts, ascii), do: [ascii_token(ascii) | parts]

  defp ascii_token(ascii), do: ascii |> Enum.reverse() |> :erlang.list_to_binary()

  defp ascii_token_char?(byte) when byte >= ?0 and byte <= ?9, do: true
  defp ascii_token_char?(byte) when byte >= ?A and byte <= ?Z, do: true
  defp ascii_token_char?(byte) when byte >= ?a and byte <= ?z, do: true
  defp ascii_token_char?(_byte), do: false

  defp ascii_downcase_byte(byte) when byte >= ?A and byte <= ?Z, do: byte + 32
  defp ascii_downcase_byte(byte), do: byte

  defp token_grapheme?(grapheme), do: Regex.match?(@token_char_regex, grapheme)

  defp normalize_grapheme(grapheme, :preserve), do: String.downcase(grapheme)

  defp normalize_grapheme(grapheme, :remove) do
    grapheme
    |> String.normalize(:nfd)
    |> String.replace(@mark_regex, "")
    |> String.downcase()
  end
end
