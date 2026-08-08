defmodule ElixirDB.Query.Planner do
  @moduledoc """
  Storage-neutral candidate planning (`QUERY-009`).

  The authoritative planner consumes the normalized predicate tree and emits
  one complete candidate plan. `select_index/2` remains below as a small
  migration seam for Wave 1 callers and tests.

  Single-index selection priority:

  1. Explicit requested index (fail with `invalid_index_hint` if missing/incompatible)
  2. Longest equality-compatible field prefix
  3. Compatible range field
  4. Compatible sort fields
  5. Stable logical index ID tie-break
  """

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.{Plan, Predicate}

  @type candidate :: %{
          index: map(),
          scan: map(),
          equality_prefix: non_neg_integer(),
          range: 0 | 1,
          sort: non_neg_integer()
        }

  @doc """
  Builds the authoritative storage-neutral candidate plan.

  Planning is deliberately conservative: a structured scan contains only
  positive constraints that are guaranteed to be necessary for every matching
  document. Negative and complex predicates remain final-evaluator work.
  """
  @spec plan([map()], map()) :: {:ok, Plan.t()} | {:error, ElixirDB.Error.t()}
  def plan(indexes, request) when is_list(indexes) and is_map(request) do
    indexes = Enum.sort_by(indexes, &index_sort_key/1)

    cond do
      get(request, :search) ->
        plan_full_text(indexes, request)

      is_binary(get(request, :index)) ->
        plan_explicit_structured(indexes, request)

      true ->
        plan_predicate(indexes, get(request, :predicate), request)
    end
  end

  def plan(_indexes, _request),
    do: {:error, ElixirDB.Error.invalid_request("query planning requires an object")}

  defp plan_full_text(indexes, request) do
    search = get(request, :search) || %{}
    requested = get(search, :index) || get(request, :index)

    case Enum.find(indexes, &full_text_index?(&1, requested)) do
      nil ->
        {:error,
         ElixirDB.Error.invalid_index_hint(
           "requested full-text index does not exist or is incompatible",
           %{index: requested}
         )}

      index ->
        index_binding = candidate_binding(index)
        scan = %{"index_id" => index_binding.index_id, "type" => "full_text", "branch" => 0}

        build_plan(:full_text, [scan], [index_binding], false)
    end
  end

  defp plan_explicit_structured(indexes, request) do
    requested = get(request, :index)

    case Enum.find(indexes, &index_named?(&1, requested)) do
      nil ->
        {:error,
         ElixirDB.Error.invalid_index_hint("requested index does not exist", %{index: requested})}

      %{"type" => "structured"} = index ->
        case best_candidate([index], get(request, :predicate), request) do
          nil ->
            invalid_hint(requested)

          candidate ->
            build_single_plan(candidate)
        end

      _index ->
        invalid_hint(requested)
    end
  end

  defp plan_predicate(indexes, predicate, request) do
    case predicate do
      {:or, branches} ->
        plan_or(indexes, branches, request)

      {:and, children} ->
        plan_conjunction(indexes, children, request)

      _ ->
        case best_candidate(indexes, predicate, request) do
          nil -> build_bounded_plan()
          candidate -> build_single_plan(candidate)
        end
    end
  end

  defp plan_conjunction(indexes, children, request) do
    case best_candidate(indexes, {:and, children}, request) do
      nil ->
        case Enum.filter(children, &match?({:or, _}, &1)) do
          [{:or, branches}] -> plan_or(indexes, branches, request)
          _ -> build_bounded_plan()
        end

      candidate ->
        build_single_plan(candidate)
    end
  end

  defp plan_or(indexes, branches, request) when is_list(branches) do
    candidates =
      Enum.map(branches, fn branch ->
        best_candidate(indexes, branch, request)
      end)

    if Enum.all?(candidates, &match?(%{}, &1)) and unique_candidates?(candidates) do
      scans =
        candidates
        |> Enum.with_index()
        |> Enum.map(fn {candidate, branch} ->
          Map.put(candidate.scan, "branch", branch)
        end)

      bindings =
        candidates
        |> Enum.map(&candidate_binding(&1.index))
        |> Enum.uniq_by(& &1.index_id)

      sort_compatible? = Enum.all?(candidates, &(&1.sort > 0))
      build_plan(:union, scans, bindings, sort_compatible?)
    else
      build_bounded_plan()
    end
  end

  defp unique_candidates?(candidates) do
    candidates
    |> Enum.map(&index_id(&1.index))
    |> Enum.uniq()
    |> length() == length(candidates)
  end

  defp best_candidate(indexes, predicate, request) do
    constraints = mandatory_constraints(predicate)

    indexes
    |> Enum.filter(&structured_index?/1)
    |> Enum.map(&candidate_for_index(&1, constraints, request))
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(&candidate_sort_key/1)
    |> List.first()
  end

  defp candidate_for_index(index, constraints, request) do
    fields = get(index, :fields) || []

    case scan_constraints(fields, constraints) do
      {[], _equality_prefix, _range} ->
        nil

      {selected, equality_prefix, range} ->
        sort = compatible_sort_count_for_constraints(fields, selected, get(request, :sort) || [])

        %{
          index: index,
          scan: %{
            "index_id" => index_id(index),
            "type" => "structured",
            "constraints" => selected
          },
          equality_prefix: equality_prefix,
          range: range,
          sort: sort
        }
    end
  end

  defp scan_constraints(fields, constraints) do
    Enum.reduce_while(fields, {[], 0, 0}, &scan_field(&1, &2, constraints))
  end

  defp scan_field(field, {selected, equality_prefix, range}, constraints) do
    path = get(field, :path)
    type = get(field, :type)
    field_constraints = Enum.filter(constraints, &(Map.get(&1, "path") == path))

    case equality_constraint(field_constraints, type) do
      nil -> scan_range(field_constraints, selected, equality_prefix, type)
      equality -> {:cont, {selected ++ equality, equality_prefix + 1, range}}
    end
  end

  defp scan_range(constraints, selected, equality_prefix, type) do
    case range_constraints(constraints, type) do
      [] -> {:halt, {selected, equality_prefix, 0}}
      ranges -> {:halt, {selected ++ ranges, equality_prefix, 1}}
    end
  end

  defp equality_constraint(constraints, type) do
    case Enum.find(
           constraints,
           &(&1["operator"] == "$eq" and compatible_scalar?(&1["value"], type))
         ) do
      nil ->
        in_equality_constraint(constraints, type)

      equality ->
        [equality]
    end
  end

  defp in_equality_constraint(constraints, type) do
    case Enum.find(constraints, &(&1["operator"] == "$in")) do
      %{"value" => values} ->
        values = Enum.filter(values, &compatible_scalar?(&1, type))

        case values do
          [] -> nil
          _ -> [%{"path" => hd(constraints)["path"], "operator" => "$in", "value" => values}]
        end

      nil ->
        nil
    end
  end

  defp range_constraints(constraints, type) do
    constraints
    |> Enum.filter(fn constraint ->
      constraint["operator"] in ["$gt", "$gte", "$lt", "$lte"] and
        compatible_scalar?(constraint["value"], type)
    end)
    |> Kernel.++(
      Enum.filter(constraints, fn constraint ->
        constraint["operator"] == "$beginsWith" and type == "string"
      end)
    )
  end

  defp mandatory_constraints({:field, path, predicates}) do
    Enum.flat_map(predicates, fn
      {:eq, value} ->
        if Predicate.scalar?(value),
          do: [%{"path" => path, "operator" => "$eq", "value" => value}],
          else: []

      {:in, values} ->
        [%{"path" => path, "operator" => "$in", "value" => values}]

      {operator, value} when operator in [:gt, :gte, :lt, :lte] ->
        [%{"path" => path, "operator" => "$" <> Atom.to_string(operator), "value" => value}]

      {:begins_with, value} ->
        [%{"path" => path, "operator" => "$beginsWith", "value" => value}]

      _ ->
        []
    end)
  end

  defp mandatory_constraints({:and, children}),
    do: Enum.flat_map(children, &mandatory_constraints/1)

  defp mandatory_constraints(_), do: []

  defp compatible_scalar?(value, "string"), do: is_binary(value)
  defp compatible_scalar?(value, "number"), do: is_number(value) and not is_boolean(value)
  defp compatible_scalar?(value, "boolean"), do: is_boolean(value)
  defp compatible_scalar?(nil, "null"), do: true
  defp compatible_scalar?(_value, _type), do: false

  defp compatible_sort_count_for_constraints(fields, constraints, sort) do
    equality_paths =
      constraints
      |> Enum.filter(&(&1["operator"] in ["$eq", "$in"]))
      |> Enum.map(& &1["path"])
      |> MapSet.new()

    remaining = Enum.drop_while(fields, &MapSet.member?(equality_paths, get(&1, :path)))
    prefix = Enum.take(remaining, length(sort))

    paths_match = Enum.map(prefix, &get(&1, :path)) == Enum.map(sort, &get(&1, :path))
    directions = Enum.map(prefix, &get(&1, :direction))
    requested = Enum.map(sort, &get(&1, :direction))
    same = requested == directions
    inverse = requested == Enum.map(directions, &invert_direction/1)

    if paths_match and (same or inverse), do: length(sort), else: 0
  end

  defp build_single_plan(candidate) do
    index_binding = candidate_binding(candidate.index)
    build_plan(:single, [candidate.scan], [index_binding], candidate.sort > 0)
  end

  defp build_bounded_plan do
    build_plan(:bounded_scan, [], [], false)
  end

  defp build_plan(kind, scans, bindings, sort_compatible?) do
    attrs =
      %{kind: kind}
      |> Map.put(:scans, scans)
      |> Map.put(:selected_indexes, bindings)
      |> Map.put(:sort_compatible?, sort_compatible?)
      |> Map.put(:pagination, pagination_for(kind))

    Plan.new(attrs)
  end

  defp candidate_binding(index) do
    %{index_id: index_id(index), definition_digest: definition_digest(index)}
  end

  defp definition_digest(index) do
    case get(index, :definition_digest) do
      digest when is_binary(digest) and byte_size(digest) == 64 -> digest
      _ -> fallback_digest(index)
    end
  end

  defp fallback_digest(index) do
    index
    |> Map.drop(["_metadata", "lifecycle_state", :_metadata, :lifecycle_state])
    |> Canonical.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp invalid_hint(index),
    do:
      {:error,
       ElixirDB.Error.invalid_index_hint("requested index is incompatible with the query", %{
         index: index
       })}

  defp structured_index?(index), do: get(index, :type) == "structured"

  defp full_text_index?(index, requested),
    do: get(index, :type) == "full_text" and index_named?(index, requested)

  defp index_named?(index, requested), do: requested in [get(index, :name), get(index, :index_id)]
  defp index_id(index), do: get(index, :index_id) || get(index, :name)

  defp index_sort_key(index), do: {index_id(index) || "", definition_digest(index)}

  defp candidate_sort_key(candidate) do
    {-candidate.equality_prefix, -candidate.range, -candidate.sort, index_id(candidate.index) || ""}
  end

  defp pagination_for(:single), do: :indexed
  defp pagination_for(:union), do: :union
  defp pagination_for(:bounded_scan), do: :bounded_scan
  defp pagination_for(:full_text), do: :full_text

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
            requested_index_result(index, request, name)
        end
    end
  end

  defp requested_index_result(index, request, name) when is_map(index) do
    if compatible_index?(index, request) do
      {:ok, index}
    else
      {:error,
       ElixirDB.Error.invalid_index_hint(
         "requested index is incompatible with the query",
         %{index: name}
       )}
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

    Enum.reduce_while(fields, 0, fn field, length ->
      if MapSet.member?(equality_paths, field["path"]),
        do: {:cont, length + 1},
        else: {:halt, length}
    end)
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
