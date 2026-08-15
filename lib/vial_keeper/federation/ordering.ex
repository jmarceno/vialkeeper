defmodule VialKeeper.Federation.Ordering do
  @moduledoc "Global ordering helpers that add source identity to query ordering."

  alias VialKeeper.MapAccess
  alias VialKeeper.Query.Ordering, as: QueryOrdering

  @type entry :: {binary(), map()} | %{source_uuid: binary(), document: map()}

  @doc "Builds the sort key stored in a federation bookmark."
  @spec ordering_key(map(), list()) :: [term()]
  def ordering_key(document, sort) when is_map(document) and is_list(sort) do
    key = QueryOrdering.ordering_key(document, sort)
    Map.get(key, "sort", []) ++ [MapAccess.get(document, :id)]
  end

  @doc "Compares two source-qualified documents using deterministic global ordering."
  @spec compare_documents(entry(), entry(), list()) :: :lt | :eq | :gt
  def compare_documents(left, right, sort) when is_list(sort) do
    {left_source, left_document} = entry_parts(left)
    {right_source, right_document} = entry_parts(right)

    case QueryOrdering.compare_sort_values(left_document, right_document, sort) do
      :eq ->
        case compare_binary(left_source, right_source) do
          :eq ->
            compare_binary(MapAccess.get(left_document, :id), MapAccess.get(right_document, :id))

          comparison ->
            comparison
        end

      comparison ->
        comparison
    end
  end

  @doc "Compares a source-qualified document with a decoded federation cursor."
  @spec compare_cursor(entry(), map(), list()) :: :lt | :eq | :gt
  def compare_cursor(entry, cursor, sort) when is_map(cursor) and is_list(sort) do
    {source_uuid, document} = entry_parts(entry)
    ordering_key = MapAccess.get(cursor, :ordering_key, [])
    cursor_source = MapAccess.get(cursor, :last_source_uuid)
    cursor_id = MapAccess.get(cursor, :last_document_id)

    left_key = QueryOrdering.ordering_key(document, sort) |> Map.put("id", "")
    right_key = %{"sort" => cursor_sort_values(ordering_key), "id" => ""}

    case QueryOrdering.compare_cursor(left_key, right_key, sort) do
      :eq ->
        case compare_binary(source_uuid, cursor_source) do
          :eq -> compare_binary(MapAccess.get(document, :id), cursor_id)
          comparison -> comparison
        end

      comparison ->
        comparison
    end
  end

  defp entry_parts({source_uuid, document}) when is_binary(source_uuid) and is_map(document),
    do: {source_uuid, document}

  defp entry_parts(%{source_uuid: source_uuid, document: document})
       when is_binary(source_uuid) and is_map(document),
       do: {source_uuid, document}

  defp entry_parts(%{"source_database_uuid" => source_uuid, "document" => document})
       when is_binary(source_uuid) and is_map(document),
       do: {source_uuid, document}

  defp entry_parts(%{source_database_uuid: source_uuid, document: document})
       when is_binary(source_uuid) and is_map(document),
       do: {source_uuid, document}

  defp entry_parts(_entry), do: raise(ArgumentError, "source-qualified document is invalid")

  defp cursor_sort_values(ordering_key) when is_list(ordering_key) do
    case List.pop_at(ordering_key, -1) do
      {nil, []} -> []
      {_document_id, sort_values} -> Enum.map(sort_values, &cursor_sort_value/1)
    end
  end

  defp cursor_sort_values(_ordering_key), do: []

  defp cursor_sort_value(%{"present" => present} = value) when is_boolean(present), do: value

  defp cursor_sort_value(%{present: present} = value) when is_boolean(present),
    do: stringify_key(value)

  defp cursor_sort_value(%{"missing" => true}), do: %{"present" => false}
  defp cursor_sort_value(%{missing: true}), do: %{"present" => false}
  defp cursor_sort_value(value), do: %{"present" => true, "value" => value}

  defp stringify_key(value) do
    %{"present" => MapAccess.get(value, :present), "value" => MapAccess.get(value, :value)}
    |> maybe_drop_value()
  end

  defp maybe_drop_value(%{"present" => false} = value), do: value
  defp maybe_drop_value(value), do: value

  defp compare_binary(left, right) when left == right, do: :eq
  defp compare_binary(left, right) when left < right, do: :lt
  defp compare_binary(_left, _right), do: :gt
end
