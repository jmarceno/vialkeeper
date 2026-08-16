defmodule VialKeeper.Query do
  @moduledoc "Query service and logical index lifecycle."

  alias VialKeeper.Config
  alias VialKeeper.JSON.{Canonical, Pointer}
  alias VialKeeper.MapAccess
  alias VialKeeper.Query.{BookmarkCodec, Normalizer, Prepared}
  alias VialKeeper.Runtime.DatabaseCatalog

  @type uuid :: binary()
  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @spec create_index(uuid(), map()) :: result(map())
  def create_index(uuid, definition) do
    with {:ok, normalized} <- normalize_index(definition) do
      DatabaseCatalog.command(
        uuid,
        {:command, :create_index, normalized},
        index_lifecycle_timeout(normalized)
      )
    end
  end

  @spec list_indexes(uuid()) :: result(map())
  def list_indexes(uuid), do: DatabaseCatalog.command(uuid, {:command, :list_indexes, %{}})

  @spec delete_index(uuid(), binary()) :: result(map())
  def delete_index(uuid, index_id),
    do: DatabaseCatalog.command(uuid, {:command, :delete_index, index_id})

  @spec rebuild_index(uuid(), binary()) :: result(map())
  def rebuild_index(uuid, index_id),
    do:
      DatabaseCatalog.command(
        uuid,
        {:command, :rebuild_index, index_id},
        Config.search_rebuild_timeout_ms()
      )

  @spec execute(uuid(), map()) :: result(map())
  def execute(uuid, request) do
    execute_internal(uuid, request, :ordinary)
  end

  @doc "Executes a query using a caller-provided shared deadline."
  @spec execute_with_deadline(uuid(), map(), pos_integer()) :: result(map())
  def execute_with_deadline(uuid, request, deadline_ms)
      when is_integer(deadline_ms) do
    execute_internal(uuid, request, {:deadline, deadline_ms})
  end

  defp execute_internal(uuid, request, timeout_mode) do
    with {:ok, normalized} <- Normalizer.normalize(request),
         :ok <- validate_query(normalized),
         {:ok, identity} <-
           command(uuid, {:command, :identity, %{}}, timeout_mode),
         :ok <- validate_database_query(normalized, identity),
         {:ok, prepared} <- prepare_bookmark(normalized, identity),
         {:ok, result} <-
           command(uuid, {:command, :query, Prepared.wrap(prepared)}, timeout_mode) do
      add_bookmark(result, prepared, identity)
    end
  end

  defp command(uuid, request, :ordinary), do: DatabaseCatalog.command(uuid, request)

  defp command(uuid, request, {:deadline, deadline}),
    do: DatabaseCatalog.command_with_deadline(uuid, request, deadline)

  @spec explain(uuid(), map()) :: result(map())
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
         logical <-
           %{
             "name" => name,
             "type" => type,
             "fields" => fields
           },
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
    do: {:error, VialKeeper.Error.invalid_request("index definition must be an object")}

  defp index_lifecycle_timeout(%{"type" => "full_text"}),
    do: Config.search_rebuild_timeout_ms()

  defp index_lifecycle_timeout(_definition), do: Config.request_timeout_ms()

  defp known_index_fields(definition) do
    allowed = [:name, :type, :fields, "name", "type", "fields"]

    case Enum.all?(Map.keys(definition), &(&1 in allowed)) do
      true -> :ok
      _ -> {:error, VialKeeper.Error.invalid_request("index definition contains an unknown field")}
    end
  end

  defp required_string(map, key, label) do
    value = get(map, key)

    case value do
      value when is_binary(value) ->
        valid_required_string(value, label)

      _ ->
        invalid_required_string(label)
    end
  end

  defp valid_required_string(value, label) do
    case {value, String.valid?(value)} do
      {"", _} -> invalid_required_string(label)
      {_value, false} -> invalid_required_string(label)
      {value, true} -> valid_required_string_size(value, label)
    end
  end

  defp valid_required_string_size(value, label) do
    valid_required_string_size(value, label, byte_size(value) <= 128)
  end

  defp valid_required_string_size(value, label, true),
    do: valid_required_string_characters(value, label)

  defp valid_required_string_size(_value, label, _within_limit),
    do: invalid_required_string(label)

  defp valid_required_string_characters(value, label) do
    case Enum.any?(String.to_charlist(value), &(&1 < 0x20)) do
      true -> invalid_required_string(label)
      _ -> {:ok, value}
    end
  end

  defp invalid_required_string(label),
    do: {:error, VialKeeper.Error.invalid_request("#{label} must be a non-empty string")}

  defp normalize_index_type("structured"), do: {:ok, "structured"}
  defp normalize_index_type("full_text"), do: {:ok, "full_text"}
  defp normalize_index_type(:structured), do: {:ok, "structured"}
  defp normalize_index_type(:full_text), do: {:ok, "full_text"}

  defp normalize_index_type(_),
    do: {:error, VialKeeper.Error.invalid_request("index type must be structured or full_text")}

  defp normalize_index_fields(fields, "structured") when is_list(fields) and fields != [] do
    Enum.reduce_while(fields, {:ok, []}, &normalize_structured_field/2)
    |> reverse_result()
  end

  defp normalize_index_fields(fields, "full_text") when is_list(fields) and fields != [] do
    Enum.reduce_while(fields, {:ok, []}, fn field, {:ok, acc} ->
      path = full_text_path(field)

      with :ok <- validate_full_text_field(field, path),
           {:ok, [_ | _]} <- Pointer.parse(path) do
        {:cont, {:ok, [path | acc]}}
      else
        _ ->
          {:halt,
           {:error,
            VialKeeper.Error.invalid_request(
              "full-text index fields must be non-empty JSON Pointers"
            )}}
      end
    end)
    |> reverse_result()
  end

  defp normalize_index_fields(_, _),
    do: {:error, VialKeeper.Error.invalid_request("index fields must be a non-empty array")}

  defp normalize_structured_field(field, {:ok, acc}) when is_map(field) do
    with :ok <- validate_structured_field_keys(field),
         {:ok, path} <- required_string(field, :path, "structured index path"),
         {:ok, [_ | _]} <- Pointer.parse(path),
         {:ok, type} <- normalize_field_type(get(field, :type)),
         {:ok, direction} <-
           normalize_direction(value_or_default(get(field, :direction), "asc")) do
      {:cont, {:ok, [%{"path" => path, "type" => type, "direction" => direction} | acc]}}
    else
      _ -> {:halt, {:error, VialKeeper.Error.invalid_request("structured index field is invalid")}}
    end
  end

  defp normalize_structured_field(_field, _state),
    do:
      {:halt, {:error, VialKeeper.Error.invalid_request("structured index fields must be objects")}}

  defp validate_full_text_field(_field, path) when not is_binary(path),
    do: invalid_full_text_field()

  defp validate_full_text_field(field, _path) when is_binary(field), do: :ok

  defp validate_full_text_field(field, _path) when is_map(field) do
    case Enum.find(Map.keys(field), &unknown_full_text_key/1) do
      nil -> :ok
      _unknown -> {:error, VialKeeper.Error.invalid_request("full-text index field is invalid")}
    end
  end

  defp validate_full_text_field(_field, _path), do: invalid_full_text_field()

  defp invalid_full_text_field,
    do:
      {:error,
       VialKeeper.Error.invalid_request("full-text index fields must be non-empty JSON Pointers")}

  defp full_text_path(field) when is_binary(field), do: field
  defp full_text_path(field) when is_map(field), do: get(field, :path)
  defp full_text_path(_field), do: nil

  defp validate_structured_field_keys(field) do
    case Enum.find(Map.keys(field), &unknown_structured_key/1) do
      nil -> :ok
      _unknown -> {:error, VialKeeper.Error.invalid_request("structured index field is invalid")}
    end
  end

  defp unknown_full_text_key(key) when key in [:path, "path"], do: nil
  defp unknown_full_text_key(_key), do: :unknown

  defp unknown_structured_key(key)
       when key in [:path, :type, :direction, "path", "type", "direction"],
       do: nil

  defp unknown_structured_key(_key), do: :unknown

  defp normalize_field_type(value) when value in ["string", "number", "boolean", "null"],
    do: {:ok, value}

  defp normalize_field_type(value) when value in [:string, :number, :boolean, :null],
    do: {:ok, Atom.to_string(value)}

  defp normalize_field_type(_),
    do: {:error, VialKeeper.Error.invalid_request("structured index field type is invalid")}

  defp normalize_direction(value) when value in ["asc", "desc"], do: {:ok, value}
  defp normalize_direction(value) when value in [:asc, :desc], do: {:ok, Atom.to_string(value)}

  defp normalize_direction(_),
    do: {:error, VialKeeper.Error.invalid_request("index direction must be asc or desc")}

  defp validate_query(request) do
    limit = value_or_default(get(request, :limit), 50)
    max = value_or_default(VialKeeper.Config.host_limits()[:max_query_results], 500)

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
    case limit_status(limit, max) do
      :within -> nil
      :exceeds -> VialKeeper.Error.resource_limit("query limit is outside the configured range")
    end
  end

  defp validate_limit(_limit, _max),
    do: VialKeeper.Error.invalid_request("query limit must be a positive integer")

  defp validate_projection(%{fields: nil}, _max), do: nil

  defp validate_projection(request, max) do
    case projection_limit_status(get(request, :fields), max) do
      :within -> nil
      :exceeds -> VialKeeper.Error.resource_limit("query projection exceeds the configured limit")
    end
  end

  defp limit_status(limit, max) when limit <= max, do: :within
  defp limit_status(_limit, _max), do: :exceeds

  defp projection_limit_status(fields, max) do
    case Enum.count_until(fields, max + 1) do
      count when count <= max -> :within
      _ -> :exceeds
    end
  end

  defp validate_bookmark_type(%{bookmark: nil}), do: nil
  defp validate_bookmark_type(%{bookmark: bookmark}) when is_binary(bookmark), do: nil

  defp validate_bookmark_type(_),
    do: VialKeeper.Error.invalid_bookmark("bookmark must be a string")

  defp validate_database_query(request, identity) do
    limit = value_or_default(get(request, :limit), 50)
    max = value_or_default(get_in(identity, [:config, "queries", "max_limit"]), 500)

    case limit_status(limit, max) do
      :within ->
        :ok

      :exceeds ->
        {:error, VialKeeper.Error.resource_limit("query limit exceeds the database configuration")}
    end
  end

  defp prepare_bookmark(request, identity) do
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
             :ok <- validate_bookmark_sequence(sequence, current) do
          {:ok,
           request
           |> Map.put(:after_id, decoded.last_id)
           |> Map.put(:after_ordering, decoded.ordering_key)
           |> Map.put(:bookmark_payload, decoded)}
        end
    end
  end

  defp validate_bookmark_sequence(sequence, sequence), do: :ok

  defp validate_bookmark_sequence(_sequence, _current),
    do: {:error, VialKeeper.Error.bookmark_stale("bookmark sequence is no longer current")}

  defp add_bookmark(result, request, identity) do
    case get(result, :has_more) do
      true -> add_next_bookmark(result, request, identity)
      _ -> {:ok, without_bookmark(result)}
    end
  end

  defp add_next_bookmark(result, request, identity) do
    values =
      case get(result, :results) do
        nil -> value_or_default(get(result, :documents), [])
        result_values -> result_values
      end

    last =
      case values do
        [] -> nil
        _ -> hd(Enum.reverse(values))
      end

    last_id = get(last, :id)

    sequence =
      get(result, :sequence)
      |> value_or_default(get(identity, :current_sequence))
      |> value_or_default(0)

    case result_plan_metadata(result, request) do
      {:ok, index_bindings, plan_digest} when is_binary(last_id) ->
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

      {:ok, _index_bindings, _plan_digest} ->
        {:error, VialKeeper.Error.invalid_request("query continuation requires a document id")}

      {:error, _} = error ->
        error
    end
  end

  defp sort_direction(request) do
    directions =
      value_or_default(get(request, :sort), [])
      |> Enum.map(&value_or_default(get(&1, :direction), "asc"))
      |> Enum.uniq()

    case directions do
      ["desc"] -> "desc"
      _ -> "asc"
    end
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
         digest when is_binary(digest) <- get(result, :plan_digest) do
      {:ok, bindings, digest}
    else
      _ -> {:error, VialKeeper.Error.invalid_request("query plan metadata is incomplete")}
    end
  end

  defp normalize_result_bindings(nil, result) do
    selected_index = get(result, :selected_index)
    definition_digest = get(result, :index_digest)

    case {selected_index, definition_digest} do
      {nil, nil} ->
        {:ok, []}

      {selected_index, definition_digest}
      when is_binary(selected_index) and is_binary(definition_digest) ->
        {:ok, [%{"index_id" => selected_index, "definition_digest" => definition_digest}]}

      _ ->
        {:error, VialKeeper.Error.invalid_request("selected index metadata is incomplete")}
    end
  end

  defp normalize_result_bindings(bindings, _result) when is_list(bindings) do
    Enum.reduce_while(bindings, {:ok, []}, fn binding, {:ok, acc} ->
      index_id = get(binding, :index_id)
      definition_digest = get(binding, :definition_digest)

      case {index_id, definition_digest} do
        {index_id, definition_digest}
        when is_binary(index_id) and is_binary(definition_digest) ->
          {:cont,
           {:ok, [%{"index_id" => index_id, "definition_digest" => definition_digest} | acc]}}

        _ ->
          {:halt, {:error, VialKeeper.Error.invalid_request("index binding metadata is invalid")}}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_result_bindings(_bindings, _result),
    do: {:error, VialKeeper.Error.invalid_request("index binding metadata is invalid")}

  defp value_or_default(nil, default), do: default
  defp value_or_default(value, _default), do: value

  defp reverse_result({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_result(error), do: error

  defp get(map, key) when is_map(map), do: MapAccess.get(map, key)
  defp get(_, _key), do: nil
end
