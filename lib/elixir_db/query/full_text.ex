defmodule ElixirDB.Query.FullText do
  @moduledoc "Storage-neutral unicode_words_v1 tokenization and search."

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
    fields = definition[:fields] || definition["fields"] || []

    tokenization = definition[:tokenization] || definition["tokenization"] || %{}

    diacritics =
      if tokenization[:diacritics] in ["remove", :remove] or
           tokenization["diacritics"] in ["remove", :remove],
         do: :remove,
         else: :preserve

    query = tokens(text, diacritics)

    values =
      Enum.flat_map(fields, fn field ->
        path = if is_binary(field), do: field, else: field["path"] || field[:path]

        case ElixirDB.JSON.Pointer.get(body, path) do
          {:ok, value} when is_binary(value) -> tokens(value, diacritics)
          _ -> []
        end
      end)

    if query == [] do
      false
    else
      case definition[:mode] || definition["mode"] || "all" do
        "any" -> Enum.any?(query, &(&1 in values))
        "phrase" -> phrase?(values, query)
        _ -> Enum.all?(query, &(&1 in values))
      end
    end
  end

  defp phrase?(values, query) do
    query != [] and
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
