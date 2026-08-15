defmodule VialKeeper.Search.Tokens do
  @moduledoc """
  Extracts unicode_words_v1 token streams from a document body for one index.
  """

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess
  alias VialKeeper.Query.FullText

  @spec stream(map() | nil, map()) :: [binary()]
  def stream(body, definition) when is_map(definition) do
    diacritics = diacritics(definition)

    definition
    |> field_paths()
    |> Enum.flat_map(&field_tokens(body, &1, diacritics))
  end

  def stream(_body, _definition), do: []

  @spec query(binary(), map()) :: [binary()]
  def query(text, definition) when is_binary(text) and is_map(definition) do
    FullText.tokens(text, diacritics(definition))
  end

  defp field_paths(definition) do
    definition
    |> MapAccess.get(:fields, [])
    |> Enum.map(fn
      field when is_binary(field) -> field
      field -> MapAccess.get(field, :path)
    end)
    |> Enum.filter(&is_binary/1)
  end

  defp field_tokens(body, path, diacritics) when is_map(body) and is_binary(path) do
    case Pointer.get(body, path) do
      {:ok, value} when is_binary(value) -> FullText.tokens(value, diacritics)
      _ -> []
    end
  end

  defp field_tokens(_body, _path, _diacritics), do: []

  defp diacritics(definition) do
    tokenization = MapAccess.get(definition, :tokenization, %{})

    if MapAccess.get(tokenization, :diacritics) in ["remove", :remove],
      do: :remove,
      else: :preserve
  end
end
