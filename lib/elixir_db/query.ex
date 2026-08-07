defmodule ElixirDB.Query do
  @moduledoc "Query service and logical index lifecycle."

  alias ElixirDB.JSON.{Canonical, Pointer}
  alias ElixirDB.MapAccess
  alias ElixirDB.Query.BookmarkCodec
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
    limit = get(request, :limit) || 50
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
    limit = get(request, :limit) || 50
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
    bookmark_index = decoded.index_id
    requested_index = get(request, :index)

    if requested_index_matches_bookmark?(requested_index, bookmark_index, indexes) do
      validate_bookmark_definition(bookmark_index, decoded.index_digest, indexes)
    else
      {:error, ElixirDB.Error.invalid_bookmark("bookmark is bound to another index")}
    end
  end

  defp validate_bookmark_sequence(sequence, sequence), do: :ok

  defp validate_bookmark_sequence(_sequence, _current),
    do: {:error, ElixirDB.Error.bookmark_stale("bookmark sequence is no longer current")}

  defp requested_index_matches_bookmark?(nil, _bookmark_index, _indexes), do: true

  defp requested_index_matches_bookmark?(requested_index, bookmark_index, indexes) do
    Enum.any?(indexes, fn index ->
      (index["name"] == requested_index or index["index_id"] == requested_index) and
        index["index_id"] == bookmark_index
    end)
  end

  defp validate_bookmark_definition(nil, _digest, _indexes), do: :ok

  defp validate_bookmark_definition(bookmark_index, digest, indexes) do
    case Enum.find(indexes, &(&1["index_id"] == bookmark_index)) do
      %{"definition_digest" => ^digest} ->
        :ok

      %{"definition_digest" => _} ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark index definition changed")}

      nil ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark index no longer exists")}
    end
  end

  defp add_bookmark(result, request, identity) do
    if get(result, :has_more) || false,
      do: add_next_bookmark(result, request, identity),
      else: without_bookmark(result)
  end

  defp add_next_bookmark(result, request, identity) do
    values = get(result, :results) || get(result, :documents) || []
    last = List.last(values)
    last_id = get(last, :id)
    sequence = get(result, :sequence) || get(identity, :current_sequence) || 0
    selected_index = get(result, :selected_index) || get(request, :index)
    index_digest = get(result, :index_digest)
    sort_direction = sort_direction(request)
    ordering_key = get(result, :last_ordering_key) || last_id
    response = Map.delete(result, :last_ordering_key) |> Map.delete("last_ordering_key")

    encode_bookmark(response, %{
      "query_fingerprint" => request.fingerprint,
      "sequence" => sequence,
      "last_id" => last_id,
      "index_id" => selected_index,
      "index_digest" => index_digest,
      "sort_direction" => sort_direction,
      "ordering_key" => ordering_key
    })
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

  defp reverse_result({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_result(error), do: error

  defp get(map, key) when is_map(map), do: MapAccess.get(map, key)
  defp get(_, _key), do: nil
end
