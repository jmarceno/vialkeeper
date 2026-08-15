defmodule VialKeeper.Query.Ordering do
  @moduledoc "Shared storage-neutral ordering semantics for query results."

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.MapAccess

  @type comparison :: :lt | :eq | :gt

  @doc "Builds the opaque ordering key used by ordinary query bookmarks."
  def ordering_key(nil, _sort), do: nil

  def ordering_key(document, sort) do
    values =
      Enum.map(sort, fn field ->
        case Pointer.get(MapAccess.get(document, :body), MapAccess.get(field, :path)) do
          {:ok, value} -> %{"present" => true, "value" => value}
          :missing -> %{"present" => false}
        end
      end)

    %{"sort" => values, "id" => MapAccess.get(document, :id)}
    |> maybe_put_rank(document, sort)
  end

  @doc "Compares two documents using only their requested sort values."
  @spec compare_sort_values(map(), map(), list()) :: comparison()
  def compare_sort_values(_left, _right, []), do: :eq

  def compare_sort_values(left, right, sort) do
    left = ordering_key(left, sort) |> Map.put("id", "")
    right = ordering_key(right, sort) |> Map.put("id", "")
    compare_ordering_keys(left, right, sort)
  end

  @doc "Sorts documents while computing each document's ordering key once."
  @spec sort_documents([map()], list()) :: [map()]
  def sort_documents(documents, sort) when is_list(documents) and is_list(sort) do
    documents
    |> Enum.map(fn document -> {ordering_key(document, sort), document} end)
    |> Enum.sort(fn {left_key, _left_document}, {right_key, _right_document} ->
      compare_ordering_keys(left_key, right_key, sort) == :lt
    end)
    |> Enum.map(&elem(&1, 1))
  end

  @doc "Compares two documents using ordinary query ordering semantics."
  @spec compare_documents(map(), map(), list()) :: comparison()
  def compare_documents(left, right, sort) do
    compare_ordering_keys(ordering_key(left, sort), ordering_key(right, sort), sort)
  end

  @doc "Compares a document or ordering key with an ordinary query cursor."
  @spec compare_cursor(map(), map(), list()) :: comparison()
  def compare_cursor(document_or_key, cursor, sort) when is_map(cursor) do
    left =
      if ordering_key?(document_or_key),
        do: normalize_ordering_key(document_or_key),
        else: ordering_key(document_or_key, sort)

    compare_ordering_keys(left, normalize_ordering_key(cursor), sort)
  end

  defp maybe_put_rank(key, document, []) do
    case MapAccess.get(document, :rank) do
      rank when is_number(rank) -> Map.put(key, "rank", rank)
      _ -> key
    end
  end

  defp maybe_put_rank(key, _document, _sort), do: key

  defp ordering_key?(%{"sort" => sort, "id" => id}) when is_list(sort) and is_binary(id), do: true
  defp ordering_key?(%{sort: sort, id: id}) when is_list(sort) and is_binary(id), do: true
  defp ordering_key?(_), do: false

  defp normalize_ordering_key(key) do
    rank = MapAccess.get(key, :rank)

    key = %{
      "sort" => MapAccess.get(key, :sort, []),
      "id" => MapAccess.get(key, :id)
    }

    case rank do
      rank when is_number(rank) -> Map.put(key, "rank", rank)
      _ -> key
    end
  end

  defp compare_ordering_keys(left, right, []) do
    case {left["rank"], right["rank"]} do
      {left_rank, right_rank} when is_number(left_rank) and is_number(right_rank) ->
        case compare_values({:ok, left_rank}, {:ok, right_rank}) do
          :eq -> compare_ids(left["id"], right["id"])
          comparison -> comparison
        end

      _ ->
        compare_ids(left["id"], right["id"])
    end
  end

  defp compare_ordering_keys(left, right, [field | rest]) do
    comparison = compare_values(first_value(left), first_value(right))

    case comparison do
      :eq -> compare_ordering_keys(drop_value(left), drop_value(right), rest)
      :lt -> apply_direction(:lt, field)
      :gt -> apply_direction(:gt, field)
    end
  end

  defp first_value(%{"sort" => [value | _]}), do: ordering_value(value)
  defp first_value(_), do: :missing

  defp drop_value(value) do
    dropped = %{"sort" => Enum.drop(value["sort"] || [], 1), "id" => value["id"]}

    if is_number(value["rank"]), do: Map.put(dropped, "rank", value["rank"]), else: dropped
  end

  defp ordering_value(%{"present" => true, "value" => value}), do: {:ok, value}
  defp ordering_value(_), do: :missing

  defp apply_direction(result, field),
    do: if(MapAccess.get(field, :direction, "asc") == "asc", do: result, else: invert(result))

  defp invert(:lt), do: :gt
  defp invert(:gt), do: :lt
  defp compare_ids(left, right) when left == right, do: :eq
  defp compare_ids(left, right) when left < right, do: :lt
  defp compare_ids(_, _), do: :gt
  defp compare_values(:missing, :missing), do: :eq
  defp compare_values(:missing, _), do: :gt
  defp compare_values(_, :missing), do: :lt
  defp compare_values({:ok, left}, {:ok, right}) when left == right, do: :eq
  defp compare_values({:ok, left}, {:ok, right}) when left < right, do: :lt
  defp compare_values({:ok, _}, {:ok, _}), do: :gt
end
