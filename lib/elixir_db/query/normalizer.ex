defmodule ElixirDB.Query.Normalizer do
  @moduledoc "Validates and canonicalizes the storage-neutral query request."

  alias ElixirDB.JSON.{Canonical, Pointer, Stringify}
  alias ElixirDB.Query.Predicate
  alias ElixirDB.Query.Regex, as: QueryRegex

  @known [:selector, :sort, :fields, :limit, :bookmark, :index, :search]
  @max_nodes 256
  @max_depth 32
  @max_boolean_children 64
  @safe_integer 9_007_199_254_740_991

  @spec normalize(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def normalize(request) when is_map(request) do
    with :ok <- validate_key_collisions(request),
         :ok <- known_fields(request),
         {:ok, selector, predicate} <-
           normalize_selector(value_or_default(get(request, :selector), %{})),
         {:ok, sort} <- normalize_sort(value_or_default(get(request, :sort), [])),
         {:ok, fields} <- normalize_fields(get(request, :fields)),
         {:ok, index} <- normalize_index_name(get(request, :index)),
         {:ok, search} <- normalize_search(get(request, :search)),
         {:ok, fingerprint_json} <-
           Canonical.encode(%{
             "selector" => selector,
             "sort" => Enum.map(sort, &Stringify.keys/1),
             "fields" => fields,
             "index" => index,
             "search" => if(search, do: Stringify.keys(search), else: nil)
           }) do
      {:ok,
       %{
         selector: selector,
         predicate: predicate,
         sort: sort,
         fields: fields,
         limit: get(request, :limit),
         bookmark: get(request, :bookmark),
         index: index,
         search: search,
         fingerprint: :crypto.hash(:sha256, fingerprint_json) |> Base.encode16(case: :lower)
       }}
    end
  end

  def normalize(_), do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  defp known_fields(request) do
    allowed = MapSet.new(@known ++ Enum.map(@known, &Atom.to_string/1))

    if Enum.all?(Map.keys(request), &MapSet.member?(allowed, &1)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("query contains an unknown field")}
  end

  defp normalize_selector(selector) when is_map(selector) do
    compile_selector(selector, %{nodes: 0}, 1)
    |> case do
      {:ok, normalized, predicate, _state} ->
        case Predicate.introspect(predicate) do
          %{node_count: node_count, depth: depth}
          when node_count <= @max_nodes and depth <= @max_depth ->
            {:ok, normalized, predicate}

          %{depth: depth} when depth > @max_depth ->
            {:error,
             ElixirDB.Error.resource_limit("query predicate depth exceeds the configured limit")}

          _ ->
            {:error, ElixirDB.Error.resource_limit("query predicate is too complex")}
        end

      {:error, _} = error ->
        error
    end
  end

  defp normalize_selector(_),
    do: {:error, ElixirDB.Error.invalid_request("selector must be an object")}

  defp compile_selector(selector, state, depth) when is_map(selector) do
    with {:ok, state} <- add_node(state, depth),
         {:ok, entries, state} <- compile_selector_entries(selector, state, depth) do
      case entries do
        [] ->
          {:ok, %{}, :match_all, state}

        [{_normalized, predicate}] ->
          {:ok, normalized_map(entries), predicate, state}

        entries ->
          {:ok, normalized_map(entries), {:and, Enum.map(entries, &elem(&1, 1))}, state}
      end
    end
  end

  defp compile_selector(_, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("selector must be an object")}

  defp normalized_map(entries) do
    Map.new(entries, fn {{key, value}, _predicate} -> {key, value} end)
  end

  defp compile_selector_entries(selector, state, depth) do
    Enum.reduce_while(selector, {:ok, [], state}, fn {key, value}, {:ok, acc, state} ->
      case compile_selector_entry(stringify_key(key), value, state, depth) do
        {:ok, normalized, predicate, state} ->
          {:cont, {:ok, [{normalized, predicate} | acc], state}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, entries, state} -> {:ok, Enum.reverse(entries), state}
      error -> error
    end)
  end

  defp compile_selector_entry("$and", value, state, depth),
    do: compile_boolean("$and", value, state, depth)

  defp compile_selector_entry("$or", value, state, depth),
    do: compile_boolean("$or", value, state, depth)

  defp compile_selector_entry("$nor", value, state, depth),
    do: compile_boolean("$nor", value, state, depth)

  defp compile_selector_entry("$not", value, state, depth) do
    with true <- is_map(value),
         {:ok, state} <- add_node(state, depth),
         {:ok, normalized, predicate, state} <- compile_selector(value, state, depth + 1) do
      {:ok, {"$not", normalized}, {:not, predicate}, state}
    else
      false -> {:error, ElixirDB.Error.invalid_request("$not requires one selector object")}
      {:error, _} = error -> error
    end
  end

  defp compile_selector_entry(key, value, state, depth) when is_binary(key) do
    if String.starts_with?(key, "$") do
      {:error, ElixirDB.Error.invalid_request("unsupported selector operator")}
    else
      compile_field_entry(key, value, state, depth)
    end
  end

  defp compile_selector_entry(_key, _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("selector field must be a string")}

  defp compile_boolean(operator, value, state, depth)
       when operator in ["$and", "$or", "$nor"] do
    cond do
      not is_list(value) or value == [] ->
        {:error, ElixirDB.Error.invalid_request("#{operator} must be a non-empty array")}

      length(value) > @max_boolean_children ->
        {:error, ElixirDB.Error.resource_limit("#{operator} has too many child selectors")}

      true ->
        with {:ok, state} <- add_node(state, depth),
             {:ok, children, state} <- compile_selector_children(value, state, depth + 1) do
          predicates = Enum.map(children, &elem(&1, 1))
          normalized = Enum.map(children, &elem(&1, 0))

          predicate = boolean_predicate(operator, predicates)
          {:ok, {operator, normalized}, predicate, state}
        end
    end
  end

  defp compile_selector_children(values, state, depth) do
    Enum.reduce_while(values, {:ok, [], state}, fn value, {:ok, acc, state} ->
      case compile_selector(value, state, depth) do
        {:ok, normalized, predicate, state} ->
          {:cont, {:ok, [{normalized, predicate} | acc], state}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, children, state} -> {:ok, Enum.reverse(children), state}
      error -> error
    end)
  end

  defp compile_field_entry(path, value, state, depth) do
    with {:ok, [_ | _]} <- Pointer.parse(path),
         {:ok, normalized, predicates, state} <- compile_condition(value, state, depth + 1),
         {:ok, state} <- add_node(state, depth) do
      {:ok, {path, normalized}, {:field, path, predicates}, state}
    else
      {:ok, []} ->
        {:error, ElixirDB.Error.invalid_request("selector paths must not be empty")}

      {:error, _} = error ->
        error

      _ ->
        {:error, ElixirDB.Error.invalid_request("selector path is invalid")}
    end
  end

  defp compile_condition(condition, state, depth) when is_map(condition) do
    keys = Enum.map(Map.keys(condition), &stringify_key/1)
    dollar_keys = Enum.filter(keys, &(is_binary(&1) and String.starts_with?(&1, "$")))

    cond do
      keys == [] ->
        with {:ok, value} <- normalize_json(condition),
             {:ok, state} <- add_node(state, depth) do
          {:ok, value, [{:eq, value}], state}
        end

      length(dollar_keys) == length(keys) ->
        compile_operator_object(condition, state, depth)

      dollar_keys == [] ->
        with {:ok, value} <- normalize_json(condition),
             {:ok, state} <- add_node(state, depth) do
          {:ok, value, [{:eq, value}], state}
        end

      true ->
        {:error,
         ElixirDB.Error.invalid_request(
           "selector condition cannot mix operators and literal fields"
         )}
    end
  end

  defp compile_condition(value, state, depth) do
    with {:ok, value} <- normalize_json(value),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, [{:eq, value}], state}
    end
  end

  defp compile_operator_object(condition, state, depth) do
    Enum.reduce_while(condition, {:ok, [], %{}, state}, fn {key, value},
                                                           {:ok, predicates, normalized, state} ->
      key = stringify_key(key)

      compile_operator_entry(key, value, predicates, normalized, state, depth)
    end)
    |> then(fn
      {:ok, predicates, normalized, state} -> {:ok, normalized, predicates, state}
      error -> error
    end)
  end

  defp compile_operator_entry(key, _value, _predicates, normalized, _state, _depth)
       when is_map_key(normalized, key) do
    {:halt,
     {:error, ElixirDB.Error.invalid_request("selector operator keys must be unique strings")}}
  end

  defp compile_operator_entry(key, value, predicates, normalized, state, depth) do
    case compile_operator(key, value, state, depth) do
      {:ok, normalized_value, predicate, state} ->
        {:cont, {:ok, predicates ++ [predicate], Map.put(normalized, key, normalized_value), state}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  defp compile_operator("$eq", value, state, depth),
    do: compile_json_operator(value, {:eq, value}, state, depth)

  defp compile_operator("$ne", value, state, depth),
    do: compile_json_operator(value, {:ne, value}, state, depth)

  defp compile_operator("$gt", value, state, depth),
    do: compile_ordered_operator(:gt, value, "$gt", state, depth)

  defp compile_operator("$gte", value, state, depth),
    do: compile_ordered_operator(:gte, value, "$gte", state, depth)

  defp compile_operator("$lt", value, state, depth),
    do: compile_ordered_operator(:lt, value, "$lt", state, depth)

  defp compile_operator("$lte", value, state, depth),
    do: compile_ordered_operator(:lte, value, "$lte", state, depth)

  defp compile_operator("$in", values, state, depth),
    do: compile_membership_operator(:in, values, "$in", state, depth)

  defp compile_operator("$nin", values, state, depth),
    do: compile_membership_operator(:nin, values, "$nin", state, depth)

  defp compile_operator("$all", values, state, depth),
    do: compile_membership_operator(:all, values, "$all", state, depth)

  defp compile_operator("$exists", value, state, depth) when is_boolean(value) do
    with {:ok, state} <- add_node(state, depth), do: {:ok, value, {:exists, value}, state}
  end

  defp compile_operator("$exists", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$exists requires a boolean")}

  defp compile_operator("$type", value, state, depth)
       when value in ["null", "boolean", "number", "string", "array", "object"] do
    with {:ok, atom} <- json_type_atom(value),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, {:type, atom}, state}
    end
  end

  defp compile_operator("$type", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$type requires a supported JSON type")}

  defp compile_operator("$beginsWith", value, state, depth) when is_binary(value) do
    with :ok <- validate_non_empty_utf8(value, "$beginsWith"),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, {:begins_with, value}, state}
    end
  end

  defp compile_operator("$beginsWith", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$beginsWith requires a non-empty UTF-8 string")}

  defp compile_operator("$regex", value, state, depth) when is_binary(value) do
    with {:ok, regex} <- QueryRegex.compile(value),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, {:regex, regex}, state}
    end
  end

  defp compile_operator("$regex", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$regex requires a string")}

  defp compile_operator("$elemMatch", value, state, depth) when is_map(value) do
    with {:ok, state} <- add_node(state, depth),
         {:ok, normalized, predicate, state} <- compile_selector(value, state, depth + 1) do
      {:ok, normalized, {:elem_match, predicate}, state}
    end
  end

  defp compile_operator("$elemMatch", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$elemMatch requires a selector object")}

  defp compile_operator("$size", value, state, depth)
       when is_integer(value) and value >= 0 and value <= @safe_integer do
    with {:ok, state} <- add_node(state, depth), do: {:ok, value, {:size, value}, state}
  end

  defp compile_operator("$size", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$size requires a non-negative integer")}

  defp compile_operator("$mod", [divisor, remainder], state, depth) do
    with {:ok, [divisor, remainder]} <- normalize_mod_values([divisor, remainder]),
         false <- divisor == 0,
         {:ok, state} <- add_node(state, depth) do
      {:ok, [divisor, remainder], {:mod, divisor, remainder}, state}
    else
      true -> {:error, ElixirDB.Error.invalid_request("$mod divisor must not be zero")}
      {:error, _} = error -> error
    end
  end

  defp compile_operator("$mod", _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("$mod requires two safe integers")}

  defp compile_operator(_operator, _value, _state, _depth),
    do: {:error, ElixirDB.Error.invalid_request("unsupported selector operator")}

  defp compile_json_operator(value, predicate, state, depth) do
    with {:ok, value} <- normalize_json(value),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, put_predicate_value(predicate, value), state}
    end
  end

  defp put_predicate_value({operator, _old}, value), do: {operator, value}

  defp ordered_value(value, _operator) when is_number(value) and not is_boolean(value),
    do: {:ok, value}

  defp ordered_value(value, _operator) when is_binary(value), do: {:ok, value}

  defp ordered_value(_value, operator),
    do: {:error, ElixirDB.Error.invalid_request("#{operator} requires a number or string")}

  defp validate_scalar_list(operator, values) when is_list(values) and values != [] do
    max = ElixirDB.Config.host_limits()[:max_query_results] || 500

    cond do
      length(values) > max ->
        {:error, ElixirDB.Error.resource_limit("#{operator} values exceed the configured limit")}

      Enum.all?(values, &Predicate.scalar?/1) ->
        :ok

      true ->
        {:error, ElixirDB.Error.invalid_request("#{operator} requires non-empty scalar values")}
    end
  end

  defp validate_scalar_list(operator, _values),
    do: {:error, ElixirDB.Error.invalid_request("#{operator} requires a non-empty array")}

  defp normalize_mod_values(values) do
    if Enum.all?(values, &safe_integer?/1),
      do: {:ok, values},
      else: {:error, ElixirDB.Error.invalid_request("$mod requires two safe integers")}
  end

  defp safe_integer?(value), do: is_integer(value) and abs(value) <= @safe_integer

  defp validate_non_empty_utf8(value, label) do
    if value != "" and String.valid?(value),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("#{label} requires a non-empty UTF-8 string")}
  end

  defp normalize_json(value) do
    normalized = Stringify.keys(value)

    if valid_json?(normalized),
      do: {:ok, normalized},
      else: {:error, ElixirDB.Error.invalid_request("selector values must be valid JSON")}
  rescue
    Protocol.UndefinedError ->
      {:error, ElixirDB.Error.invalid_request("selector values must be valid JSON")}
  end

  defp valid_json?(nil), do: true
  defp valid_json?(value) when is_boolean(value), do: true
  defp valid_json?(value) when is_integer(value), do: abs(value) <= @safe_integer
  defp valid_json?(value) when is_float(value), do: is_float(value)
  defp valid_json?(value) when is_binary(value), do: String.valid?(value)
  defp valid_json?(value) when is_list(value), do: Enum.all?(value, &valid_json?/1)

  defp valid_json?(value) when is_map(value),
    do: Enum.all?(value, fn {key, child} -> is_binary(key) and valid_json?(child) end)

  defp valid_json?(_), do: false

  defp add_node(_state, depth) when depth > @max_depth,
    do:
      {:error, ElixirDB.Error.resource_limit("query predicate depth exceeds the configured limit")}

  defp add_node(state, _depth), do: {:ok, state}

  defp compile_ordered_operator(tag, value, operator, state, depth) do
    with {:ok, value} <- ordered_value(value, operator),
         {:ok, state} <- add_node(state, depth) do
      {:ok, value, {tag, value}, state}
    end
  end

  defp compile_membership_operator(tag, values, operator, state, depth) do
    with :ok <- validate_scalar_list(operator, values),
         {:ok, values} <- normalize_json(values),
         {:ok, state} <- add_node(state, depth) do
      {:ok, values, {tag, values}, state}
    end
  end

  defp json_type_atom("null"), do: {:ok, :null}
  defp json_type_atom("boolean"), do: {:ok, :boolean}
  defp json_type_atom("number"), do: {:ok, :number}
  defp json_type_atom("string"), do: {:ok, :string}
  defp json_type_atom("array"), do: {:ok, :array}
  defp json_type_atom("object"), do: {:ok, :object}

  defp json_type_atom(_),
    do: {:error, ElixirDB.Error.invalid_request("$type requires a supported JSON type")}

  defp boolean_predicate("$nor", predicates), do: {:not, {:or, predicates}}
  defp boolean_predicate(operator, predicates), do: {operator_atom(operator), predicates}

  defp operator_atom("$and"), do: :and
  defp operator_atom("$or"), do: :or
  defp operator_atom("$nor"), do: :or

  defp normalize_sort(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      path = get(value, :path)
      direction = value_or_default(get(value, :direction), "asc")

      with true <- is_map(value),
           true <- Map.keys(value) -- [:path, :direction, "path", "direction"] == [],
           true <- is_binary(path),
           {:ok, [_ | _]} <- Pointer.parse(path),
           true <- direction in ["asc", "desc", :asc, :desc] do
        {:cont, {:ok, [%{path: path, direction: to_string(direction)} | acc]}}
      else
        false ->
          {:halt, {:error, ElixirDB.Error.invalid_request("sort direction must be asc or desc")}}

        _ ->
          {:halt,
           {:error, ElixirDB.Error.invalid_request("sort fields require non-empty JSON Pointers")}}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_sort(_), do: {:error, ElixirDB.Error.invalid_request("sort must be an array")}

  defp normalize_fields(nil), do: {:ok, nil}

  defp normalize_fields(values) when is_list(values) do
    if values == [] do
      {:error, ElixirDB.Error.invalid_request("fields must be a non-empty array")}
    else
      normalize_field_list(values)
    end
  end

  defp normalize_fields(_), do: {:error, ElixirDB.Error.invalid_request("fields must be an array")}

  defp normalize_field_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn path, {:ok, acc} ->
      with true <- is_binary(path),
           {:ok, [_ | _]} <- Pointer.parse(path) do
        {:cont, {:ok, [path | acc]}}
      else
        _ ->
          {:halt,
           {:error, ElixirDB.Error.invalid_request("fields require non-empty JSON Pointers")}}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_index_name(nil), do: {:ok, nil}
  defp normalize_index_name(value) when is_binary(value) and value != "", do: {:ok, value}

  defp normalize_index_name(_),
    do: {:error, ElixirDB.Error.invalid_request("index must be a string")}

  defp normalize_search(nil), do: {:ok, nil}

  defp normalize_search(value) when is_map(value) do
    index = get(value, :index)
    text = get(value, :text)
    mode = value_or_default(get(value, :mode), "all")

    with true <- Map.keys(value) -- [:index, :text, :mode, "index", "text", "mode"] == [],
         true <- is_binary(index) and index != "",
         true <- is_binary(text) and text != "",
         true <- mode in ["all", "any", "phrase", "prefix"] do
      {:ok, %{index: index, text: text, mode: mode}}
    else
      _ ->
        {:error,
         ElixirDB.Error.invalid_request("search requires index, text, and a supported mode")}
    end
  end

  defp normalize_search(_), do: {:error, ElixirDB.Error.invalid_request("search must be an object")}

  defp stringify_key(key) when is_atom(key), do: Atom.to_string(key)
  defp stringify_key(key), do: key

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp get(_, _key), do: nil

  defp value_or_default(nil, default), do: default
  defp value_or_default(value, _default), do: value

  @doc "Validates that nested object keys remain unique after stringification."
  def validate_key_collisions(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, MapSet.new()}, fn {key, child}, {:ok, seen} ->
      with {:ok, string_key} <- key_as_string(key),
           false <- MapSet.member?(seen, string_key),
           :ok <- validate_key_collisions(child) do
        {:cont, {:ok, MapSet.put(seen, string_key)}}
      else
        true ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("object keys must be unique after stringification")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, _seen} -> :ok
      {:error, _} = error -> error
    end)
  end

  def validate_key_collisions(value) when is_list(value) do
    Enum.reduce_while(value, :ok, fn child, :ok ->
      case validate_key_collisions(child) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  def validate_key_collisions(_value), do: :ok

  defp key_as_string(key) when is_binary(key), do: {:ok, key}
  defp key_as_string(key) when is_atom(key), do: {:ok, Atom.to_string(key)}
  defp key_as_string(_key), do: {:error, ElixirDB.Error.invalid_request("object key is invalid")}
end
