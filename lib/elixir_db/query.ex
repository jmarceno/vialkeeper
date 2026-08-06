defmodule ElixirDB.Query do
  @moduledoc "Query service and logical index lifecycle."

  alias ElixirDB.JSON.{Canonical, Pointer}
  alias ElixirDB.Query.Normalizer
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
         {:ok, result} <- DatabaseCatalog.command(uuid, {:command, :query, prepared}) do
      {:ok, add_bookmark(result, prepared, identity)}
    end
  end

  def explain(uuid, request) do
    with {:ok, normalized} <- Normalizer.normalize(request),
         :ok <- validate_query(normalized),
         {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}),
         :ok <- validate_database_query(normalized, identity) do
      DatabaseCatalog.command(uuid, {:command, :explain_query, normalized})
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
    limit = request[:limit] || request["limit"] || 50
    max = ElixirDB.Config.host_limits()[:max_query_results] || 500

    cond do
      not is_integer(limit) or limit <= 0 ->
        {:error, ElixirDB.Error.invalid_request("query limit must be a positive integer")}

      limit > max ->
        {:error, ElixirDB.Error.resource_limit("query limit is outside the configured range")}

      not is_nil(request[:fields]) and length(request[:fields]) > max ->
        {:error, ElixirDB.Error.resource_limit("query projection exceeds the configured limit")}

      not is_nil(request[:bookmark]) and not is_binary(request[:bookmark]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark must be a string")}

      true ->
        :ok
    end
  end

  defp validate_database_query(request, identity) do
    limit = request[:limit] || request["limit"] || 50
    max = get_in(identity, [:config, "queries", "max_limit"]) || 500

    if limit <= max,
      do: :ok,
      else:
        {:error, ElixirDB.Error.resource_limit("query limit exceeds the database configuration")}
  end

  defp prepare_bookmark(request, identity, indexes) do
    case request[:bookmark] || request["bookmark"] do
      nil ->
        {:ok, request}

      bookmark ->
        with {:ok, decoded} <-
               ElixirDB.Query.BookmarkCodec.decode(bookmark, %{
                 "query_fingerprint" => request.fingerprint
               }),
             sequence <- decoded.sequence,
             current <- identity[:current_sequence] || identity["current_sequence"],
             :ok <-
               if(sequence == current,
                 do: :ok,
                 else:
                   {:error, ElixirDB.Error.bookmark_stale("bookmark sequence is no longer current")}
               ),
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
    bookmark_index = decoded.index_id
    requested_index = request[:index] || request["index"]

    requested_matches_bookmark? =
      case requested_index do
        nil ->
          true

        value ->
          case Enum.find(indexes, &(&1["name"] == value or &1["index_id"] == value)) do
            %{"index_id" => ^bookmark_index} -> true
            _ -> false
          end
      end

    cond do
      not requested_matches_bookmark? ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark is bound to another index")}

      is_nil(bookmark_index) ->
        :ok

      true ->
        case Enum.find(indexes, &(&1["index_id"] == bookmark_index)) do
          %{"definition_digest" => digest} ->
            if digest == decoded.index_digest,
              do: :ok,
              else: {:error, ElixirDB.Error.invalid_bookmark("bookmark index definition changed")}

          nil ->
            {:error, ElixirDB.Error.invalid_bookmark("bookmark index no longer exists")}
        end
    end
  end

  defp add_bookmark(result, request, identity) do
    has_more = result[:has_more] || result["has_more"] || false

    if has_more do
      values =
        result[:results] || result["results"] || result[:documents] || result["documents"] || []

      last = List.last(values)
      last_id = last[:id] || last["id"]
      sequence = result[:sequence] || result["sequence"] || identity[:current_sequence] || 0
      selected_index = result[:selected_index] || result["selected_index"] || request[:index]
      index_digest = result[:index_digest] || result["index_digest"]
      sort = request[:sort] || request["sort"] || []
      directions = Enum.map(sort, &(&1[:direction] || &1["direction"] || "asc")) |> Enum.uniq()
      sort_direction = if directions == ["desc"], do: "desc", else: "asc"
      ordering_key = result[:last_ordering_key] || result["last_ordering_key"] || last_id

      response = Map.delete(result, :last_ordering_key) |> Map.delete("last_ordering_key")

      case ElixirDB.Query.BookmarkCodec.encode(%{
             "query_fingerprint" => request.fingerprint,
             "sequence" => sequence,
             "last_id" => last_id,
             "index_id" => selected_index,
             "index_digest" => index_digest,
             "sort_direction" => sort_direction,
             "ordering_key" => ordering_key
           }) do
        {:ok, bookmark} -> Map.put(response, :bookmark, bookmark)
        _ -> response
      end
    else
      result
      |> Map.delete(:last_ordering_key)
      |> Map.delete("last_ordering_key")
      |> Map.put(:bookmark, nil)
    end
  end

  defp reverse_result({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_result(error), do: error

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp get(_, _key), do: nil
end
