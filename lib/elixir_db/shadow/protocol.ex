defmodule ElixirDB.Shadow.Protocol do
  @moduledoc "Versioned capability and request validation for the shadow control plane."

  alias ElixirDB.Error

  @protocol_major 1
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
    value = string_keys(value)

    value["protocol_major"] == @protocol_major and
      is_list(value["capabilities"]) and
      Enum.all?(@capabilities, &(&1 in value["capabilities"]))
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
    value = string_keys(value)

    with :ok <- reject_unknown(value, allowed_fields),
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
  def string_keys(value) when is_map(value),
    do: Map.new(value, fn {key, member} -> {to_string(key), member} end)

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

  defp generation(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp generation(_), do: {:error, Error.invalid_request("shadow generation must be non-negative")}
end
