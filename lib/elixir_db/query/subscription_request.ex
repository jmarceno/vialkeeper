defmodule ElixirDB.Query.SubscriptionRequest do
  @moduledoc "Validates and normalizes live query subscription requests."

  alias ElixirDB.Query.Normalizer

  @outer_keys ~w(query heartbeat_ms)
  @query_keys ~w(selector fields)
  @snapshot_keys ~w(selector predicate fields max_members)
  @forbidden_query_keys ~w(sort limit bookmark index search)

  @spec normalize(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def normalize(request, config) when is_map(request) and is_map(config) do
    with :ok <- validate_outer_keys(request),
         {:ok, query} <- require_query_object(request),
         :ok <- validate_query_keys(query),
         {:ok, normalized_query} <- normalize_query(query),
         {:ok, heartbeat_ms} <- normalize_heartbeat(request, config) do
      {:ok,
       %{
         selector: normalized_query.selector,
         predicate: normalized_query.predicate,
         fields: normalized_query.fields,
         heartbeat_ms: heartbeat_ms
       }}
    end
  end

  def normalize(_, _),
    do: {:error, ElixirDB.Error.invalid_request("subscription request must be an object")}

  @doc """
  Validates and normalizes a subscription snapshot command request.
  """
  @spec prepare_snapshot(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prepare_snapshot(request, config) when is_map(request) and is_map(config) do
    if query_object?(request) do
      max_members_input = get(request, :max_members)

      with {:ok, normalized} <- normalize(snapshot_outer_request(request), config),
           {:ok, max_members} <- validate_max_members(max_members_input, config) do
        {:ok, Map.put(normalized, :max_members, max_members)}
      end
    else
      with :ok <- reject_forbidden_query_fields(request),
           :ok <- validate_snapshot_keys(request),
           {:ok, normalized} <- normalize_snapshot_query(request),
           {:ok, max_members} <- validate_max_members(get(request, :max_members), config) do
        {:ok, Map.put(normalized, :max_members, max_members)}
      end
    end
  end

  def prepare_snapshot(_, _),
    do: {:error, ElixirDB.Error.invalid_request("subscription snapshot request must be an object")}

  @doc """
  Validates a bounded revision batch request list.
  """
  @spec validate_revisions_batch([map()], map()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_revisions_batch(requests, config) when is_list(requests) and is_map(config) do
    maximum =
      get_in(config, ["changes", "max_batch"]) ||
        ElixirDB.Config.host_limits()[:max_changes_batch] ||
        500

    with :ok <- validate_revision_batch_size(requests, maximum) do
      validate_revision_batch_items(requests)
    end
  end

  def validate_revisions_batch(_, _),
    do: {:error, ElixirDB.Error.invalid_request("revision batch requests must be a list")}

  defp query_object?(request), do: not is_nil(get(request, :query))

  defp validate_outer_keys(request) do
    validate_allowed_keys(request, @outer_keys, "subscription request contains an unknown field")
  end

  defp validate_snapshot_keys(request) do
    validate_allowed_keys(
      request,
      @snapshot_keys,
      "subscription snapshot request contains an unknown field"
    )
  end

  defp validate_query_keys(query) do
    validate_allowed_keys(query, @query_keys, "subscription query contains an unknown field")
  end

  defp validate_allowed_keys(request, allowed_keys, unknown_message) do
    allowed = MapSet.new(allowed_keys)

    Enum.reduce_while(Map.keys(request), :ok, fn key, :ok ->
      validate_allowed_key(key, allowed, unknown_message)
    end)
  end

  defp validate_allowed_key(key, allowed, unknown_message) do
    case stringify_key(key) do
      {:ok, string} -> validate_allowed_string(string, allowed, unknown_message)
      {:error, _} = error -> {:halt, error}
    end
  end

  defp validate_allowed_string(string, allowed, unknown_message) do
    if MapSet.member?(allowed, string),
      do: {:cont, :ok},
      else: {:halt, {:error, ElixirDB.Error.invalid_request(unknown_message)}}
  end

  defp require_query_object(request) do
    case get(request, :query) do
      query when is_map(query) -> {:ok, query}
      nil -> {:error, ElixirDB.Error.invalid_request("subscription request requires query")}
      _ -> {:error, ElixirDB.Error.invalid_request("query must be an object")}
    end
  end

  defp reject_forbidden_query_fields(request) do
    with {:ok, keys} <- request_string_keys(request) do
      case Enum.find(@forbidden_query_keys, &MapSet.member?(keys, &1)) do
        nil -> :ok
        field -> {:error, ElixirDB.Error.invalid_request("subscription query rejects #{field}")}
      end
    end
  end

  defp request_string_keys(request) do
    Enum.reduce_while(Map.keys(request), {:ok, MapSet.new()}, fn key, {:ok, acc} ->
      case stringify_key(key) do
        {:ok, string} -> {:cont, {:ok, MapSet.put(acc, string)}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp stringify_key(key) when is_binary(key), do: {:ok, key}
  defp stringify_key(key) when is_atom(key), do: {:ok, Atom.to_string(key)}

  defp stringify_key(_),
    do: {:error, ElixirDB.Error.invalid_request("request contains an invalid field name")}

  defp normalize_query(query) do
    Normalizer.normalize(%{
      selector: get(query, :selector) || %{},
      fields: get(query, :fields)
    })
  end

  defp normalize_snapshot_query(request) do
    normalize_query(request)
  end

  defp snapshot_outer_request(request) do
    request
    |> Map.drop([:max_members, "max_members"])
  end

  defp validate_max_members(nil, config) do
    {:ok, configured_max_members(config)}
  end

  defp validate_max_members(value, config) when is_integer(value) and value > 0 do
    maximum = configured_max_members(config)

    if value <= maximum,
      do: {:ok, value},
      else:
        {:error,
         ElixirDB.Error.resource_limit("max_members exceeds the configured maximum", %{
           maximum: maximum,
           value: value
         })}
  end

  defp validate_max_members(_value, _config),
    do: {:error, ElixirDB.Error.invalid_request("max_members must be a positive integer")}

  defp configured_max_members(config),
    do: get_in(config, ["subscriptions", "max_members"]) || 500

  defp validate_revision_batch_size([], _maximum), do: :ok

  defp validate_revision_batch_size(requests, maximum) do
    if Enum.count(requests) > maximum do
      {:error,
       ElixirDB.Error.resource_limit("revision batch exceeds the configured maximum", %{
         maximum: maximum,
         count: Enum.count(requests)
       })}
    else
      :ok
    end
  end

  defp validate_revision_batch_items(requests) do
    Enum.reduce_while(requests, :ok, fn request, :ok ->
      case validate_revision_batch_item(request) do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp validate_revision_batch_item(request) when is_map(request) do
    allowed = MapSet.new(~w(document_id revision_id))

    with :ok <- validate_item_keys(request, allowed),
         :ok <- validate_required_binary(request, :document_id),
         :ok <- validate_required_binary(request, :revision_id) do
      :ok
    else
      {:error, _} = error -> error
    end
  end

  defp validate_revision_batch_item(_),
    do: {:error, ElixirDB.Error.invalid_request("revision batch item must be an object")}

  defp validate_item_keys(request, allowed) do
    Enum.reduce_while(Map.keys(request), :ok, fn key, :ok ->
      validate_allowed_key(key, allowed, "revision batch item has unknown fields")
    end)
  end

  defp validate_required_binary(request, key) do
    case get(request, key) do
      value when is_binary(value) and value != "" -> :ok
      nil -> {:error, ElixirDB.Error.invalid_request("#{key} is required")}
      _ -> {:error, ElixirDB.Error.invalid_request("#{key} must be a non-empty string")}
    end
  end

  defp normalize_heartbeat(request, config) do
    subscriptions = Map.get(config, "subscriptions", %{})
    default = Map.get(subscriptions, "default_heartbeat_ms", 15_000)
    maximum = Map.get(subscriptions, "max_heartbeat_ms", 60_000)

    case get(request, :heartbeat_ms) do
      nil ->
        {:ok, default}

      value when is_integer(value) and value > 0 and value <= maximum ->
        {:ok, value}

      value when is_integer(value) and value > maximum ->
        {:error,
         ElixirDB.Error.resource_limit("heartbeat_ms exceeds the configured maximum", %{
           maximum: maximum,
           value: value
         })}

      _ ->
        {:error, ElixirDB.Error.invalid_request("heartbeat_ms must be a positive integer")}
    end
  end

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
