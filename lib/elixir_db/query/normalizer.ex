defmodule ElixirDB.Query.Normalizer do
  @moduledoc "Validates and canonicalizes the storage-neutral query request."

  alias ElixirDB.JSON.{Canonical, Pointer, Stringify}

  @known [:selector, :sort, :fields, :limit, :bookmark, :index, :search]

  @spec normalize(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def normalize(request) when is_map(request) do
    with :ok <- known_fields(request),
         {:ok, selector} <- normalize_selector(get(request, :selector) || %{}),
         {:ok, sort} <- normalize_sort(get(request, :sort) || []),
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
    Enum.reduce_while(selector, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      normalize_selector_entry(stringify_key(key), value, acc)
    end)
  end

  defp normalize_selector(_),
    do: {:error, ElixirDB.Error.invalid_request("selector must be an object")}

  defp normalize_selector_entry("$and", value, acc),
    do: normalize_and_entry(value, acc)

  defp normalize_selector_entry(key, value, acc) when is_binary(key),
    do: normalize_path_entry(key, value, acc)

  defp normalize_selector_entry(_key, _value, _acc),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("selector field must be a string")}}

  defp normalize_and_entry(value, acc) when is_list(value) and value != [] do
    case normalize_selector_list(value) do
      {:ok, clauses} -> {:cont, {:ok, Map.put(acc, "$and", clauses)}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp normalize_and_entry(_value, _acc),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("$and must be a non-empty array")}}

  defp normalize_path_entry(key, value, acc) do
    with {:ok, [_ | _]} <- Pointer.parse(key),
         {:ok, condition} <- normalize_condition(value) do
      {:cont, {:ok, Map.put(acc, key, condition)}}
    else
      {:ok, []} ->
        {:halt, {:error, ElixirDB.Error.invalid_request("selector paths must not be empty")}}

      {:error, error} ->
        {:halt, {:error, error}}

      _ ->
        {:halt, {:error, ElixirDB.Error.invalid_request("selector path is invalid")}}
    end
  end

  defp normalize_selector_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case normalize_selector(value) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_condition(condition) when is_map(condition) do
    Enum.reduce_while(condition, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      normalize_condition_entry(stringify_key(key), value, acc)
    end)
    |> case do
      {:ok, values} when map_size(values) > 0 ->
        {:ok, values}

      {:ok, _} ->
        {:error, ElixirDB.Error.invalid_request("selector operator object must not be empty")}

      error ->
        error
    end
  end

  defp normalize_condition(value) do
    if scalar?(value),
      do: {:ok, value},
      else: {:error, ElixirDB.Error.invalid_request("selector values must be JSON scalars")}
  end

  defp normalize_condition_entry(key, _value, _acc) when not is_binary(key),
    do: invalid_operator_keys()

  defp normalize_condition_entry(key, _value, acc) when is_map_key(acc, key),
    do: invalid_operator_keys()

  defp normalize_condition_entry(key, value, acc)
       when key in ["$eq", "$gt", "$gte", "$lt", "$lte", "$exists", "$in"] do
    case validate_condition_value(key, value) do
      :ok -> {:cont, {:ok, Map.put(acc, key, Stringify.keys(value))}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp normalize_condition_entry(_key, _value, _acc),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("unsupported selector operator")}}

  defp invalid_operator_keys do
    {:halt,
     {:error, ElixirDB.Error.invalid_request("selector operator keys must be unique strings")}}
  end

  defp validate_condition_value("$exists", value) when is_boolean(value), do: :ok

  defp validate_condition_value("$exists", _),
    do: {:error, ElixirDB.Error.invalid_request("$exists requires a boolean")}

  defp validate_condition_value("$in", values) when is_list(values) and values != [] do
    max = ElixirDB.Config.host_limits()[:max_query_results] || 500

    if length(values) <= max and Enum.all?(values, &scalar?/1),
      do: :ok,
      else: {:error, ElixirDB.Error.resource_limit("$in values exceed the configured limit")}
  end

  defp validate_condition_value("$in", _),
    do: {:error, ElixirDB.Error.invalid_request("$in requires a non-empty array")}

  defp validate_condition_value(_operator, value) when is_map(value) or is_list(value),
    do: {:error, ElixirDB.Error.invalid_request("comparison values must be scalars")}

  defp validate_condition_value(_operator, value),
    do:
      if(scalar?(value),
        do: :ok,
        else: {:error, ElixirDB.Error.invalid_request("selector value is invalid")}
      )

  defp normalize_sort(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      path = get(value, :path)
      direction = get(value, :direction) || "asc"

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
    mode = get(value, :mode) || "all"

    with true <- Map.keys(value) -- [:index, :text, :mode, "index", "text", "mode"] == [],
         true <- is_binary(index) and index != "",
         true <- is_binary(text) and text != "",
         true <- mode in ["all", "any", "phrase"] do
      {:ok, %{index: index, text: text, mode: mode}}
    else
      _ ->
        {:error,
         ElixirDB.Error.invalid_request("search requires index, text, and a supported mode")}
    end
  end

  defp normalize_search(_), do: {:error, ElixirDB.Error.invalid_request("search must be an object")}

  defp scalar?(value),
    do: is_nil(value) or is_boolean(value) or is_number(value) or is_binary(value)

  defp stringify_key(key) when is_atom(key), do: Atom.to_string(key)
  defp stringify_key(key), do: key

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp get(_, _key), do: nil
end
