defmodule ElixirDB.Query do
  @moduledoc "Query service and logical index lifecycle."

  alias ElixirDB.JSON.{Canonical, Pointer}
  alias ElixirDB.MapAccess
  alias ElixirDB.Query.{BookmarkCodec, Normalizer, Plan, Planner, Prepared}
  alias ElixirDB.Runtime.DatabaseCatalog

  def create_index(uuid, definition) do
    with {:ok, normalized} <- normalize_index(definition) do
      DatabaseCatalog.command(uuid, {:command, :create_index, normalized})
    end
  end

  def list_indexes(uuid), do: DatabaseCatalog.command(uuid, {:command, :list_indexes, %{}})

  def delete_index(uuid, index_id),
    do: DatabaseCatalog.command(uuid, {:command, :delete_index, index_id})

  def rebuild_index(uuid, index_id),
    do: DatabaseCatalog.command(uuid, {:command, :rebuild_index, index_id})

  def execute(uuid, request) do
    with {:ok, normalized} <- Normalizer.normalize(request),
         :ok <- validate_query(normalized),
         {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}),
         :ok <- validate_database_query(normalized, identity),
         {:ok, indexes} <- DatabaseCatalog.command(uuid, {:command, :list_indexes, %{}}),
         {:ok, prepared} <- prepare_bookmark(normalized, identity, indexes),
         {:ok, result} <- DatabaseCatalog.command(uuid, {:command, :query, Prepared.wrap(prepared)}) do
      add_bookmark(result, prepared, identity)
    end
  end

  def explain(uuid, request) do
    with {:ok, normalized} <- Normalizer.normalize(request),
         :ok <- validate_query(normalized),
         {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}),
         :ok <- validate_database_query(normalized, identity) do
      DatabaseCatalog.command(uuid, {:command, :explain_query, Prepared.wrap(normalized)})
    end
  end

  defp normalize_index(definition) when is_map(definition) do
    with :ok <- known_index_fields(definition),
         {:ok, name} <- required_string(definition, :name, "index name"),
         {:ok, type} <- normalize_index_type(get(definition, :type)),
         {:ok, fields} <- normalize_index_fields(get(definition, :fields), type),
         {:ok, tokenization} <- normalize_tokenization(get(definition, :tokenization), type),
         logical <-
           %{
             "name" => name,
             "type" => type,
             "fields" => fields
           }
           |> maybe_put_tokenization(tokenization),
         {:ok, json} <- Canonical.encode(logical) do
      {:ok,
       Map.put(
         logical,
         "definition_digest",
         :crypto.hash(:sha256, json) |> Base.encode16(case: :lower)
       )}
    end
  end

  defp normalize_index(_),
    do: {:error, ElixirDB.Error.invalid_request("index definition must be an object")}

  defp known_index_fields(definition) do
    allowed = [:name, :type, :fields, :tokenization, "name", "type", "fields", "tokenization"]

    if Enum.all?(Map.keys(definition), &(&1 in allowed)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("index definition contains an unknown field")}
  end

  defp required_string(map, key, label) do
    value = get(map, key)

    if is_binary(value) and value != "" and String.valid?(value) and byte_size(value) <= 128 and
         not Enum.any?(String.to_charlist(value), &(&1 < 0x20)),
       do: {:ok, value},
       else: {:error, ElixirDB.Error.invalid_request("#{label} must be a non-empty string")}
  end

  defp normalize_index_type("structured"), do: {:ok, "structured"}
  defp normalize_index_type("full_text"), do: {:ok, "full_text"}
  defp normalize_index_type(:structured), do: {:ok, "structured"}
  defp normalize_index_type(:full_text), do: {:ok, "full_text"}

  defp normalize_index_type(_),
    do: {:error, ElixirDB.Error.invalid_request("index type must be structured or full_text")}

  defp normalize_index_fields(fields, "structured") when is_list(fields) and fields != [] do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      with true <- is_map(field),
           true <- Map.keys(field) -- [:path, :type, :direction, "path", "type", "direction"] == [],
           {:ok, path} <- required_string(field, :path, "structured index path"),
           {:ok, [_ | _]} <- Pointer.parse(path),
           {:ok, type} <- normalize_field_type(get(field, :type)),
           {:ok, direction} <- normalize_direction(get(field, :direction) || "asc") do
        {:cont, {:ok, [%{"path" => path, "type" => type, "direction" => direction} | acc]}}
      else
        false ->
          {:halt,
           {:error, ElixirDB.Error.invalid_request("structured index fields must be objects")}}

        _ ->
          {:halt, {:error, ElixirDB.Error.invalid_request("structured index field is invalid")}}
      end
    end)
    |> reverse_result()
  end

  defp normalize_index_fields(fields, "full_text") when is_list(fields) and fields != [] do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      path = if is_binary(field), do: field, else: get(field, :path)

      with true <- is_binary(field) or (is_map(field) and Map.keys(field) -- [:path, "path"] == []),
           true <- is_binary(path),
           {:ok, [_ | _]} <- Pointer.parse(path) do
        {:cont, {:ok, [path | acc]}}
      else
        _ ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("full-text index fields must be non-empty JSON Pointers")}}
      end
    end)
    |> reverse_result()
  end

  defp normalize_index_fields(_, _),
    do: {:error, ElixirDB.Error.invalid_request("index fields must be a non-empty array")}

  defp normalize_field_type(value) when value in ["string", "number", "boolean", "null"],
    do: {:ok, value}

  defp normalize_field_type(value) when value in [:string, :number, :boolean, :null],
    do: {:ok, Atom.to_string(value)}

  defp normalize_field_type(_),
    do: {:error, ElixirDB.Error.invalid_request("structured index field type is invalid")}

  defp normalize_direction(value) when value in ["asc", "desc"], do: {:ok, value}
  defp normalize_direction(value) when value in [:asc, :desc], do: {:ok, Atom.to_string(value)}

  defp normalize_direction(_),
    do: {:error, ElixirDB.Error.invalid_request("index direction must be asc or desc")}

  defp normalize_tokenization(nil, "structured"), do: {:ok, nil}

  defp normalize_tokenization(_, "structured"),
    do: {:error, ElixirDB.Error.invalid_request("structured indexes do not accept tokenization")}

  defp normalize_tokenization(tokenization, "full_text")
       when is_nil(tokenization) or is_map(tokenization) do
    tokenization = tokenization || %{}
    strategy = get(tokenization, :strategy) || "unicode_words_v1"
    diacritics = get(tokenization, :diacritics) || "preserve"

    if Map.keys(tokenization) -- [:strategy, :diacritics, "strategy", "diacritics"] == [] and
         strategy == "unicode_words_v1" and diacritics in ["preserve", "remove"],
       do: {:ok, %{"strategy" => strategy, "diacritics" => diacritics}},
       else: {:error, ElixirDB.Error.invalid_request("unsupported full-text tokenization")}
  end

  defp normalize_tokenization(_, "full_text"),
    do: {:error, ElixirDB.Error.invalid_request("full-text tokenization must be an object")}

  defp maybe_put_tokenization(definition, nil), do: definition

  defp maybe_put_tokenization(definition, tokenization),
    do: Map.put(definition, "tokenization", tokenization)

  defp validate_query(request) do
    limit = value_or_default(get(request, :limit), 50)
    max = ElixirDB.Config.host_limits()[:max_query_results] || 500

    validators = [
      fn -> validate_limit(limit, max) end,
      fn -> validate_projection(request, max) end,
      fn -> validate_bookmark_type(request) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> {:error, error}
    end
  end

  defp validate_limit(limit, max) when is_integer(limit) and limit > 0 do
    if limit <= max,
      do: nil,
      else: ElixirDB.Error.resource_limit("query limit is outside the configured range")
  end

  defp validate_limit(_limit, _max),
    do: ElixirDB.Error.invalid_request("query limit must be a positive integer")

  defp validate_projection(%{fields: nil}, _max), do: nil

  defp validate_projection(request, max) do
    if length(get(request, :fields)) <= max,
      do: nil,
      else: ElixirDB.Error.resource_limit("query projection exceeds the configured limit")
  end

  defp validate_bookmark_type(%{bookmark: nil}), do: nil
  defp validate_bookmark_type(%{bookmark: bookmark}) when is_binary(bookmark), do: nil

  defp validate_bookmark_type(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark must be a string")

  defp validate_database_query(request, identity) do
    limit = value_or_default(get(request, :limit), 50)
    max = get_in(identity, [:config, "queries", "max_limit"]) || 500

    if limit <= max,
      do: :ok,
      else:
        {:error, ElixirDB.Error.resource_limit("query limit exceeds the database configuration")}
  end

  defp prepare_bookmark(request, identity, indexes) do
    case get(request, :bookmark) do
      nil ->
        {:ok, request}

      bookmark ->
        with {:ok, decoded} <-
               BookmarkCodec.decode(bookmark, %{
                 "query_fingerprint" => request.fingerprint
               }),
             sequence <- decoded.sequence,
             current <- get(identity, :current_sequence),
             :ok <- validate_bookmark_sequence(sequence, current),
             :ok <- validate_bookmark_index(decoded, request, indexes) do
          {:ok,
           request
           |> Map.put(:after_id, decoded.last_id)
           |> Map.put(:after_ordering, decoded.ordering_key)
           |> Map.put(:bookmark_payload, decoded)}
        end
    end
  end

  defp validate_bookmark_index(decoded, request, indexes) do
    with :ok <- validate_bookmark_bindings(decoded.index_bindings, indexes),
         {:ok, selected} <- Planner.select_index(indexes, request),
         {:ok, expected_bindings, expected_digest} <- transitional_metadata(request, selected),
         :ok <- validate_empty_binding_request(decoded.index_bindings, request, expected_bindings),
         true <- decoded.index_bindings == expected_bindings,
         true <- decoded.plan_digest == expected_digest do
      :ok
    else
      false ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark is bound to another plan")}

      {:error, %ElixirDB.Error{code: :invalid_index_hint}} ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark plan cannot be reproduced")}

      {:error, _} = error ->
        error
    end
  end

  defp validate_bookmark_bindings(bindings, indexes) do
    Enum.reduce_while(bindings, :ok, fn %{"index_id" => index_id, "definition_digest" => digest},
                                        :ok ->
      case Enum.find(indexes, &(&1["index_id"] == index_id)) do
        %{"definition_digest" => ^digest} ->
          {:cont, :ok}

        %{"definition_digest" => _} ->
          {:halt, {:error, ElixirDB.Error.invalid_bookmark("bookmark index definition changed")}}

        nil ->
          {:halt, {:error, ElixirDB.Error.invalid_bookmark("bookmark index no longer exists")}}
      end
    end)
  end

  defp validate_bookmark_sequence(sequence, sequence), do: :ok

  defp validate_bookmark_sequence(_sequence, _current),
    do: {:error, ElixirDB.Error.bookmark_stale("bookmark sequence is no longer current")}

  defp add_bookmark(result, request, identity) do
    if get(result, :has_more) || false,
      do: add_next_bookmark(result, request, identity),
      else: {:ok, without_bookmark(result)}
  end

  defp add_next_bookmark(result, request, identity) do
    values = get(result, :results) || get(result, :documents) || []
    last = List.last(values)
    last_id = get(last, :id)
    sequence = get(result, :sequence) || get(identity, :current_sequence) || 0

    with {:ok, index_bindings, plan_digest} <- result_plan_metadata(result, request),
         true <- is_binary(last_id) do
      sort_direction = sort_direction(request)
      ordering_key = value_or_default(get(result, :last_ordering_key), last_id)
      response = Map.delete(result, :last_ordering_key) |> Map.delete("last_ordering_key")

      {:ok,
       encode_bookmark(response, %{
         "query_fingerprint" => request.fingerprint,
         "sequence" => sequence,
         "last_id" => last_id,
         "plan_digest" => plan_digest,
         "index_bindings" => index_bindings,
         "sort_direction" => sort_direction,
         "ordering_key" => ordering_key
       })}
    else
      false -> {:error, ElixirDB.Error.invalid_request("query continuation requires a document id")}
      {:error, _} = error -> error
    end
  end

  defp sort_direction(request) do
    directions =
      (get(request, :sort) || [])
      |> Enum.map(&(get(&1, :direction) || "asc"))
      |> Enum.uniq()

    if directions == ["desc"], do: "desc", else: "asc"
  end

  defp encode_bookmark(response, payload) do
    case BookmarkCodec.encode(payload) do
      {:ok, bookmark} -> Map.put(response, :bookmark, bookmark)
      _ -> response
    end
  end

  defp without_bookmark(result) do
    result
    |> Map.delete(:last_ordering_key)
    |> Map.delete("last_ordering_key")
    |> Map.put(:bookmark, nil)
  end

  defp result_plan_metadata(result, _request) do
    raw_bindings = get(result, :index_bindings)

    with {:ok, bindings} <- normalize_result_bindings(raw_bindings, result),
         {:ok, selected} <- selected_from_bindings(bindings, result),
         {:ok, transitional} <- transitional_result_digest(selected),
         digest <- choose_result_digest(get(result, :plan_digest), transitional) do
      {:ok, bindings, digest}
    end
  end

  defp normalize_result_bindings(nil, result) do
    selected_index = get(result, :selected_index)
    definition_digest = get(result, :index_digest)

    cond do
      is_nil(selected_index) and is_nil(definition_digest) ->
        {:ok, []}

      is_binary(selected_index) and is_binary(definition_digest) ->
        {:ok, [%{"index_id" => selected_index, "definition_digest" => definition_digest}]}

      true ->
        {:error, ElixirDB.Error.invalid_request("selected index metadata is incomplete")}
    end
  end

  defp normalize_result_bindings(bindings, _result) when is_list(bindings) do
    Enum.reduce_while(bindings, {:ok, []}, fn binding, {:ok, acc} ->
      index_id = get(binding, :index_id)
      definition_digest = get(binding, :definition_digest)

      if is_binary(index_id) and is_binary(definition_digest) do
        {:cont, {:ok, [%{"index_id" => index_id, "definition_digest" => definition_digest} | acc]}}
      else
        {:halt, {:error, ElixirDB.Error.invalid_request("index binding metadata is invalid")}}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_result_bindings(_bindings, _result),
    do: {:error, ElixirDB.Error.invalid_request("index binding metadata is invalid")}

  defp selected_from_bindings([], _result), do: {:ok, nil}

  defp selected_from_bindings([binding], result) do
    kind =
      case get(result, :plan_kind) do
        "full_text" -> :full_text
        :full_text -> :full_text
        _ -> :single
      end

    {:ok,
     %{
       index_id: binding["index_id"],
       definition_digest: binding["definition_digest"],
       type: kind
     }}
  end

  defp selected_from_bindings(_bindings, _result),
    do: {:error, ElixirDB.Error.invalid_request("transitional bookmarks support one index binding")}

  defp transitional_result_digest(nil), do: Plan.transitional_digest(:bounded_scan, [])

  defp transitional_result_digest(selected) do
    kind = selected.type
    Plan.transitional_digest(kind, selected)
  end

  defp choose_result_digest(digest, transitional)
       when is_binary(digest) and digest != "" do
    if valid_digest?(digest) and digest != String.duplicate("0", 64), do: digest, else: transitional
  end

  defp choose_result_digest(_digest, transitional), do: transitional

  defp transitional_metadata(_request, nil) do
    with {:ok, digest} <- Plan.transitional_digest(:bounded_scan, []) do
      {:ok, [], digest}
    end
  end

  defp transitional_metadata(_request, selected) do
    kind = if selected["type"] == "full_text", do: :full_text, else: :single
    binding = %{index_id: selected["index_id"], definition_digest: selected["definition_digest"]}

    with {:ok, digest} <- Plan.transitional_digest(kind, binding) do
      {:ok, [%{"index_id" => binding.index_id, "definition_digest" => binding.definition_digest}],
       digest}
    end
  end

  defp validate_empty_binding_request([], request, []) do
    if is_nil(get(request, :index)) and is_nil(get(request, :search)),
      do: :ok,
      else:
        {:error, ElixirDB.Error.invalid_bookmark("empty bindings are valid only for bounded scans")}
  end

  defp validate_empty_binding_request([], _request, _expected),
    do: {:error, ElixirDB.Error.invalid_bookmark("bookmark plan requires an index binding")}

  defp validate_empty_binding_request(_bindings, _request, _expected), do: :ok

  defp valid_digest?(value), do: is_binary(value) and Regex.match?(~r/^[0-9a-f]{64}$/, value)
  defp value_or_default(nil, default), do: default
  defp value_or_default(value, _default), do: value

  defp reverse_result({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_result(error), do: error

  defp get(map, key) when is_map(map), do: MapAccess.get(map, key)
  defp get(_, _key), do: nil
end
