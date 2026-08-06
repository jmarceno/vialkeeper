defmodule ElixirDB.Query.Planner do
  @moduledoc """
  Storage-neutral index selection (`QUERY-009`).

  Selection priority:

  1. Explicit requested index (fail with `invalid_index_hint` if missing/incompatible)
  2. Longest equality-compatible field prefix
  3. Compatible range field
  4. Compatible sort fields
  5. Stable logical index ID tie-break
  """

  @spec select_index([map()], map()) :: {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def select_index(indexes, request) when is_list(indexes) and is_map(request) do
    case requested_index_name(request) do
      nil ->
        selected =
          indexes
          |> Enum.filter(&compatible_index?(&1, request))
          |> Enum.sort_by(&selection_key(&1, request))
          |> List.first()

        {:ok, selected}

      name ->
        case Enum.find(indexes, &(&1["name"] == name or &1["index_id"] == name)) do
          nil ->
            {:error,
             ElixirDB.Error.invalid_index_hint("requested index does not exist", %{index: name})}

          index ->
            if compatible_index?(index, request),
              do: {:ok, index},
              else:
                {:error,
                 ElixirDB.Error.invalid_index_hint(
                   "requested index is incompatible with the query",
                   %{index: name}
                 )}
        end
    end
  end

  @doc false
  @spec selection_key(map(), map()) :: {integer(), integer(), integer(), binary()}
  def selection_key(index, request) do
    {eq, range, sort} = score_components(index, request)
    {-eq, -range, -sort, index["index_id"] || ""}
  end

  @doc false
  @spec score_components(map(), map()) :: {non_neg_integer(), 0 | 1, non_neg_integer()}
  def score_components(%{"type" => "full_text"}, request) do
    if is_nil(get(request, :search)), do: {0, 0, 0}, else: {1_000_000, 0, 0}
  end

  def score_components(%{"type" => "structured", "fields" => fields}, request) do
    selector = get(request, :selector) || %{}
    sort = get(request, :sort) || []
    eq = equality_prefix_length(fields, selector)
    range = if compatible_range_field?(fields, selector, eq), do: 1, else: 0
    sort_count = compatible_sort_count(fields, selector, sort)
    {eq, range, sort_count}
  end

  def score_components(_, _), do: {0, 0, 0}

  defp requested_index_name(request) do
    get(request, :index) ||
      get(get(request, :search) || %{}, :index)
  end

  defp compatible_index?(%{"type" => "full_text"} = index, request) do
    not is_nil(get(request, :search)) and
      search_index_name(request) in [index["name"], index["index_id"]]
  end

  defp compatible_index?(%{"type" => "structured", "fields" => fields}, request) do
    selector = get(request, :selector) || %{}
    sort = get(request, :sort) || []
    paths = Enum.map(fields, & &1["path"])
    selector_paths = selector_paths(selector)

    Enum.any?(selector_paths, &(&1 in paths)) or
      compatible_sort_count(fields, selector, sort) > 0
  end

  defp compatible_index?(_, _), do: false

  defp search_index_name(request) do
    search = get(request, :search) || %{}
    get(search, :index)
  end

  defp equality_prefix_length(fields, selector) do
    equality_paths = MapSet.new(equality_selector_paths(selector))

    fields
    |> Enum.take_while(fn field -> MapSet.member?(equality_paths, field["path"]) end)
    |> length()
  end

  defp compatible_range_field?(fields, selector, equality_prefix_len) do
    case Enum.at(fields, equality_prefix_len) do
      nil -> false
      field -> range_condition?(selector_condition(selector, field["path"]))
    end
  end

  defp compatible_sort_count(_fields, _selector, []), do: 0

  defp compatible_sort_count(fields, selector, sort) do
    equality_paths = MapSet.new(equality_selector_paths(selector))
    remaining = Enum.drop_while(fields, &MapSet.member?(equality_paths, &1["path"]))
    prefix = Enum.take(remaining, length(sort))

    paths_match = Enum.map(prefix, & &1["path"]) == Enum.map(sort, &get(&1, :path))
    directions = Enum.map(prefix, & &1["direction"])
    requested = Enum.map(sort, &get(&1, :direction))
    same = requested == directions
    inverse = requested == Enum.map(directions, &invert_direction/1)

    if paths_match and (same or inverse), do: length(sort), else: 0
  end

  defp invert_direction("asc"), do: "desc"
  defp invert_direction("desc"), do: "asc"
  defp invert_direction(other), do: other

  defp selector_paths(selector) do
    Enum.flat_map(selector, fn
      {"$and", clauses} when is_list(clauses) -> Enum.flat_map(clauses, &selector_paths/1)
      {path, _} when is_binary(path) -> [path]
      _ -> []
    end)
  end

  defp equality_selector_paths(selector) do
    Enum.flat_map(selector, fn
      {"$and", clauses} when is_list(clauses) ->
        Enum.flat_map(clauses, &equality_selector_paths/1)

      {path, value} when is_binary(path) ->
        if equality_condition?(value), do: [path], else: []

      _ ->
        []
    end)
  end

  defp selector_condition(selector, path) do
    Enum.find_value(selector, fn
      {"$and", clauses} when is_list(clauses) ->
        Enum.find_value(clauses, &selector_condition(&1, path))

      {^path, value} ->
        value

      _ ->
        nil
    end)
  end

  defp equality_condition?(value) when is_map(value) do
    keys = Map.keys(value)

    keys in [["$eq"], [:"$eq"]] or Map.has_key?(value, "$in") or Map.has_key?(value, :"$in")
  end

  defp equality_condition?(_), do: true

  defp range_condition?(value) when is_map(value) do
    Map.has_key?(value, "$gt") or Map.has_key?(value, :"$gt") or
      Map.has_key?(value, "$gte") or Map.has_key?(value, :"$gte") or
      Map.has_key?(value, "$lt") or Map.has_key?(value, :"$lt") or
      Map.has_key?(value, "$lte") or Map.has_key?(value, :"$lte")
  end

  defp range_condition?(_), do: false

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp get(_, _key), do: nil
end
