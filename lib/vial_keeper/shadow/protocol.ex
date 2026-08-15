defmodule VialKeeper.Shadow.Protocol do
  @moduledoc "Versioned capability and request validation for the shadow control plane."

  alias VialKeeper.Error

  @protocol_major 1
  @max_generation 9_223_372_036_854_775_807
  @capabilities [
    "generation_fencing_v1",
    "shadow_provision_v1",
    "shadow_pull_v1",
    "shadow_point_reads_v1",
    "external_attachment_store_v1",
    "physical_attachment_read_v1",
    "zstd_json_v1"
  ]

  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @spec major() :: pos_integer()
  def major, do: @protocol_major

  @spec capabilities() :: [binary()]
  def capabilities, do: @capabilities

  @spec required_capabilities() :: [binary()]
  def required_capabilities, do: @capabilities

  @spec max_generation() :: pos_integer()
  def max_generation, do: @max_generation

  @spec response(binary()) :: map()
  def response(node_id) when is_binary(node_id) do
    %{
      "node_id" => node_id,
      "protocol_major" => @protocol_major,
      "capabilities" => @capabilities
    }
  end

  @spec compatible?(map()) :: boolean()
  def compatible?(value) when is_map(value) do
    case normalize_string_keys(value) do
      {:ok, value} ->
        value["protocol_major"] == @protocol_major and
          is_list(value["capabilities"]) and
          Enum.all?(@capabilities, &(&1 in value["capabilities"]))

      {:error, :invalid_keys} ->
        false
    end
  end

  def compatible?(_), do: false

  @spec ensure_compatible(map()) :: :ok | {:error, Error.t()}
  def ensure_compatible(value) do
    if compatible?(value),
      do: :ok,
      else: {:error, Error.shadow_incompatible("shadow control capabilities are incompatible")}
  end

  @doc "Validates a control request's immutable source/generation identity."
  @spec generation_request(map(), [binary()]) :: {:ok, map()} | {:error, Error.t()}
  def generation_request(value, allowed_fields) when is_map(value) and is_list(allowed_fields) do
    with {:ok, value} <- normalize_request_keys(value),
         :ok <- reject_unknown(value, allowed_fields),
         :ok <- uuid(value["source_uuid"], "source_uuid"),
         {:ok, generation} <- generation(value["generation"]),
         :ok <- uuid(value["shadow_uuid"], "shadow_uuid"),
         :ok <- uuid(value["operation_id"], "operation_id") do
      {:ok,
       value
       |> Map.put("source_uuid", String.downcase(value["source_uuid"]))
       |> Map.put("shadow_uuid", String.downcase(value["shadow_uuid"]))
       |> Map.put("operation_id", String.downcase(value["operation_id"]))
       |> Map.put("generation", generation)}
    end
  end

  def generation_request(_, _),
    do: {:error, Error.invalid_request("shadow control request must be an object")}

  @spec string_keys(map()) :: map()
  def string_keys(value) when is_map(value) do
    case normalize_string_keys(value) do
      {:ok, normalized} -> normalized
      {:error, :invalid_keys} -> value
    end
  end

  defp reject_unknown(value, allowed) do
    if Enum.any?(Map.keys(value), &(&1 not in allowed)),
      do: {:error, Error.invalid_request("shadow control request contains an unknown field")},
      else: :ok
  end

  defp uuid(value, field) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, Error.invalid_request("shadow #{field} must be a UUID")}
  end

  defp uuid(_value, field),
    do: {:error, Error.invalid_request("shadow #{field} must be a UUID")}

  defp generation(value)
       when is_integer(value) and value >= 0 and value <= @max_generation,
       do: {:ok, value}

  defp generation(_),
    do:
      {:error,
       Error.invalid_request("shadow generation must be an integer within the supported range")}

  defp string_key(key) when is_binary(key), do: key
  defp string_key(key) when is_atom(key), do: Atom.to_string(key)

  defp normalize_request_keys(value) do
    case normalize_string_keys(value) do
      {:ok, normalized} ->
        {:ok, normalized}

      {:error, :invalid_keys} ->
        {:error, Error.invalid_request("shadow control request contains invalid fields")}
    end
  end

  defp normalize_string_keys(value) when is_struct(value), do: {:error, :invalid_keys}

  defp normalize_string_keys(value) do
    Enum.reduce_while(value, {:ok, %{}}, &put_normalized_member/2)
  end

  defp put_normalized_member({key, member}, {:ok, acc}) do
    case normalized_key(key) do
      {:ok, key} -> put_unique_member(acc, key, member)
      {:error, :invalid_keys} = error -> {:halt, error}
    end
  end

  defp put_unique_member(acc, key, member) do
    if Map.has_key?(acc, key),
      do: {:halt, {:error, :invalid_keys}},
      else: {:cont, {:ok, Map.put(acc, key, member)}}
  end

  defp normalized_key(key) when is_binary(key) or is_atom(key), do: {:ok, string_key(key)}
  defp normalized_key(_key), do: {:error, :invalid_keys}
end
