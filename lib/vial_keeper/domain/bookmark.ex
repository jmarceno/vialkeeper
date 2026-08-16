defmodule VialKeeper.Domain.Bookmark do
  @moduledoc "Validated bookmark state for paginated queries."
  alias VialKeeper.Error

  @digest_pattern ~r/^[0-9a-f]{64}$/
  @enforce_keys [
    :version,
    :protocol_major,
    :query_fingerprint,
    :plan_digest,
    :index_bindings,
    :sequence,
    :last_id,
    :checksum
  ]
  defstruct [
    :version,
    :protocol_major,
    :query_fingerprint,
    :plan_digest,
    :index_bindings,
    :sequence,
    :sort_direction,
    :ordering_key,
    :last_id,
    :checksum
  ]

  @type index_binding :: %{
          required(binary()) => binary()
        }

  @type t :: %__MODULE__{
          version: pos_integer(),
          protocol_major: pos_integer(),
          query_fingerprint: binary(),
          plan_digest: binary(),
          index_bindings: [index_binding()],
          sequence: non_neg_integer(),
          sort_direction: binary() | nil,
          ordering_key: term(),
          last_id: binary(),
          checksum: binary()
        }

  @known [
    :version,
    :protocol_major,
    :query_fingerprint,
    :plan_digest,
    :index_bindings,
    :sequence,
    :sort_direction,
    :ordering_key,
    :last_id,
    :checksum
  ]

  @wire_keys [
    "version",
    "protocol_major",
    "query_fingerprint",
    "plan_digest",
    "index_bindings",
    "sequence",
    "sort_direction",
    "ordering_key",
    "last_id",
    "checksum"
  ]

  @spec new(map()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, Error.invalid_request("unknown bookmark field")},
      else: build(attrs)
  end

  def new(_), do: {:error, Error.invalid_request("bookmark must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @wire_keys)) do
      {:error, Error.invalid_request("unknown bookmark field")}
    else
      new(%{
        version: attrs["version"],
        protocol_major: attrs["protocol_major"],
        query_fingerprint: attrs["query_fingerprint"],
        plan_digest: attrs["plan_digest"],
        index_bindings: attrs["index_bindings"],
        sequence: attrs["sequence"],
        sort_direction: attrs["sort_direction"],
        ordering_key: attrs["ordering_key"],
        last_id: attrs["last_id"],
        checksum: attrs["checksum"]
      })
    end
  end

  def from_wire(_), do: {:error, Error.invalid_request("bookmark must be an object")}

  defp build(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         nil <- bookmark_validation_error(attrs) do
      {:ok, struct(__MODULE__, attrs)}
    else
      error when is_struct(error, Error) -> {:error, error}
      {:error, _} = error -> error
    end
  end

  defp normalize_attrs(attrs) do
    with {:ok, bindings} <- normalize_bindings(Map.get(attrs, :index_bindings)) do
      {:ok, Map.put(attrs, :index_bindings, bindings)}
    end
  end

  defp bookmark_validation_error(attrs) do
    with nil <- validate_version(attrs),
         nil <- validate_protocol_major(attrs),
         nil <- validate_query_fingerprint(attrs),
         nil <- validate_plan_digest(attrs),
         nil <- validate_index_bindings(attrs),
         nil <- validate_sequence(attrs),
         nil <- validate_ordering_key(attrs),
         nil <- validate_last_id(attrs),
         nil <- validate_checksum(attrs) do
      validate_sort_direction(attrs)
    end
  end

  defp validate_version(%{version: 1}), do: nil
  defp validate_version(_), do: Error.invalid_bookmark("unsupported bookmark version")

  defp validate_protocol_major(%{protocol_major: value}) when is_integer(value) and value >= 1,
    do: nil

  defp validate_protocol_major(_),
    do: Error.invalid_bookmark("bookmark protocol_major is invalid")

  defp validate_query_fingerprint(%{query_fingerprint: value}) when is_binary(value), do: nil

  defp validate_query_fingerprint(_),
    do: Error.invalid_bookmark("bookmark query_fingerprint is required")

  defp validate_plan_digest(%{plan_digest: value}) when is_binary(value) do
    if valid_digest?(value),
      do: nil,
      else: Error.invalid_bookmark("bookmark plan_digest is invalid")
  end

  defp validate_plan_digest(_),
    do: Error.invalid_bookmark("bookmark plan_digest is required")

  defp validate_index_bindings(%{index_bindings: value}) when is_list(value), do: nil

  defp validate_index_bindings(_),
    do: Error.invalid_bookmark("bookmark index_bindings must be an array")

  defp validate_sequence(%{sequence: value}) when is_integer(value) and value >= 0, do: nil

  defp validate_sequence(_),
    do: Error.invalid_bookmark("bookmark sequence must be non-negative")

  defp validate_last_id(%{last_id: value}) when is_binary(value), do: nil
  defp validate_last_id(_), do: Error.invalid_bookmark("bookmark last_id is required")

  defp validate_ordering_key(%{ordering_key: value}) when is_list(value) do
    if valid_cursor_value?(value),
      do: nil,
      else: Error.invalid_bookmark("bookmark ordering_key is invalid")
  end

  defp validate_ordering_key(%{ordering_key: value})
       when is_binary(value) or is_number(value) or is_boolean(value),
       do: nil

  defp validate_ordering_key(%{ordering_key: value}) when is_map(value) do
    case normalize_ordering_cursor(value) do
      {:ok, %{"sort" => sort, "id" => id} = cursor}
      when is_list(sort) and is_binary(id) and map_size(cursor) in [2, 3] ->
        if valid_cursor_value?(sort) and valid_rank?(cursor),
          do: nil,
          else: Error.invalid_bookmark("bookmark ordering_key is invalid")

      _ ->
        Error.invalid_bookmark("bookmark ordering_key is invalid")
    end
  end

  defp validate_ordering_key(_),
    do: Error.invalid_bookmark("bookmark ordering_key is required")

  defp validate_checksum(%{checksum: value}) when is_binary(value), do: nil
  defp validate_checksum(_), do: Error.invalid_bookmark("bookmark checksum is required")

  defp validate_sort_direction(%{sort_direction: value})
       when is_nil(value) or value in ["asc", "desc"], do: nil

  defp validate_sort_direction(_),
    do: Error.invalid_bookmark("bookmark sort_direction is invalid")

  defp normalize_bindings(bindings) when is_list(bindings) do
    Enum.reduce_while(bindings, {:ok, [], MapSet.new()}, fn binding, {:ok, acc, ids} ->
      with {:ok, normalized} <- normalize_binding(binding),
           false <- MapSet.member?(ids, normalized["index_id"]) do
        {:cont, {:ok, [normalized | acc], MapSet.put(ids, normalized["index_id"])}}
      else
        true ->
          {:halt,
           {:error, Error.invalid_bookmark("bookmark index_bindings must be ordered and unique")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, values, _ids} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_bindings(_),
    do: {:error, Error.invalid_bookmark("bookmark index_bindings must be an array")}

  defp normalize_binding(binding) when is_map(binding) do
    keys = Map.keys(binding)
    index_id = Map.get(binding, :index_id, Map.get(binding, "index_id"))
    definition_digest = Map.get(binding, :definition_digest, Map.get(binding, "definition_digest"))

    cond do
      key_collision?(keys) ->
        {:error, Error.invalid_bookmark("bookmark index binding keys must be unique")}

      Enum.any?(keys, &(&1 not in [:index_id, :definition_digest, "index_id", "definition_digest"])) ->
        {:error, Error.invalid_bookmark("bookmark index binding contains an unknown field")}

      not is_binary(index_id) or index_id == "" ->
        {:error, Error.invalid_bookmark("bookmark index_id is invalid")}

      not valid_digest?(definition_digest) ->
        {:error, Error.invalid_bookmark("bookmark definition_digest is invalid")}

      true ->
        {:ok, %{"index_id" => index_id, "definition_digest" => definition_digest}}
    end
  end

  defp normalize_binding(_),
    do: {:error, Error.invalid_bookmark("bookmark index binding must be an object")}

  defp key_collision?(keys) do
    keys
    |> Enum.map(fn
      key when is_atom(key) -> Atom.to_string(key)
      key when is_binary(key) -> key
      _key -> :invalid
    end)
    |> case do
      values ->
        Enum.member?(values, :invalid) or length(values) != MapSet.size(MapSet.new(values))
    end
  end

  defp valid_digest?(value) do
    is_binary(value) and Regex.match?(@digest_pattern, value) and
      value != String.duplicate("0", 64)
  end

  defp normalize_ordering_cursor(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}}, fn {key, child}, {:ok, acc} ->
      put_cursor_entry(acc, key, child)
    end)
  end

  defp put_cursor_entry(acc, key, child) do
    with {:ok, string_key} <- cursor_key(key),
         false <- Map.has_key?(acc, string_key) do
      {:cont, {:ok, Map.put(acc, string_key, child)}}
    else
      _ -> {:halt, :error}
    end
  end

  defp cursor_key(key) when key in ["sort", "id", "rank"], do: {:ok, key}
  defp cursor_key(:sort), do: {:ok, "sort"}
  defp cursor_key(:id), do: {:ok, "id"}
  defp cursor_key(:rank), do: {:ok, "rank"}
  defp cursor_key(_), do: :error

  defp valid_rank?(%{"rank" => rank}), do: is_number(rank)
  defp valid_rank?(%{}), do: true

  defp valid_cursor_value?(value) when is_list(value), do: Enum.all?(value, &valid_cursor_value?/1)

  defp valid_cursor_value?(value) when is_map(value) do
    Enum.all?(value, fn {key, child} ->
      is_binary(key) and valid_cursor_value?(child)
    end)
  end

  defp valid_cursor_value?(value) when is_binary(value) or is_number(value) or is_boolean(value),
    do: true

  defp valid_cursor_value?(nil), do: true
  defp valid_cursor_value?(_value), do: false
end
