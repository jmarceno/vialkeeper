defmodule VialKeeper.Shadow.Definition do
  @moduledoc "Validated desired state for one source-bound managed shadow."

  alias VialKeeper.MapAccess

  @enforce_keys [:source_uuid, :enabled, :generation, :shadow_uuid, :operation_id]
  defstruct [
    :source_uuid,
    :enabled,
    :location,
    :attachment_location,
    :generation,
    :shadow_uuid,
    :operation_id,
    :specification_digest
  ]

  @type t :: %__MODULE__{
          source_uuid: String.t(),
          enabled: boolean(),
          location: String.t() | nil,
          attachment_location: String.t() | nil,
          generation: non_neg_integer(),
          shadow_uuid: String.t() | nil,
          operation_id: String.t() | nil,
          specification_digest: String.t() | nil
        }

  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @spec new(String.t(), map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def new(source_uuid, attrs) when is_binary(source_uuid) and is_map(attrs) do
    with {:ok, attrs} <- normalize_keys(attrs),
         :ok <- valid_uuid(source_uuid, "source_uuid"),
         {:ok, location} <- required_text(attrs, "location"),
         {:ok, attachment_location} <- required_text(attrs, "attachment_location"),
         :ok <- valid_attachment_location(attachment_location),
         {:ok, generation} <- non_negative(Map.get(attrs, "generation", 0), :generation),
         {:ok, shadow_uuid} <- generated_uuid(attrs, "shadow_uuid"),
         {:ok, operation_id} <- generated_uuid(attrs, "operation_id"),
         {:ok, digest} <- optional_digest(attrs["specification_digest"]) do
      {:ok,
       %__MODULE__{
         source_uuid: source_uuid,
         enabled: true,
         location: location,
         attachment_location: attachment_location,
         generation: generation,
         shadow_uuid: shadow_uuid,
         operation_id: operation_id,
         specification_digest: digest
       }}
    end
  end

  def new(_source_uuid, _attrs),
    do: {:error, VialKeeper.Error.invalid_request("shadow definition must be an object")}

  @spec disabled(t()) :: t()
  def disabled(%__MODULE__{} = definition), do: %{definition | enabled: false}

  @spec replace(t(), map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def replace(%__MODULE__{} = previous, attrs) when is_map(attrs) do
    with {:ok, attrs} <- normalize_keys(attrs) do
      attrs =
        previous
        |> Map.from_struct()
        |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
        |> Map.merge(attrs)
        |> Map.put("generation", previous.generation + 1)

      new(previous.source_uuid, attrs)
    end
  end

  @spec from_map(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def from_map(map) when is_map(map) do
    with {:ok, map} <- normalize_keys(map) do
      source_uuid = map["source_uuid"]
      enabled = map["enabled"]
      attrs = Map.put(map, "generation", Map.get(map, "generation", 0))

      case enabled do
        true ->
          new(source_uuid, attrs)

        false ->
          disabled_from_map(source_uuid, attrs)

        _ ->
          {:error,
           VialKeeper.Error.integrity_violation("shadow desired state enabled flag is invalid")}
      end
    end
  end

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = definition) do
    %{
      "enabled" => definition.enabled,
      "location" => definition.location,
      "attachment_location" => definition.attachment_location,
      "generation" => definition.generation,
      "shadow_uuid" => definition.shadow_uuid,
      "operation_id" => definition.operation_id,
      "specification_digest" => definition.specification_digest
    }
  end

  @spec token(t()) :: map()
  def token(%__MODULE__{} = definition),
    do: %{
      source_uuid: definition.source_uuid,
      generation: definition.generation,
      shadow_uuid: definition.shadow_uuid,
      operation_id: definition.operation_id
    }

  defp disabled_from_map(source_uuid, attrs) do
    with {:ok, attrs} <- normalize_keys(attrs),
         :ok <- valid_uuid(source_uuid, "source_uuid"),
         {:ok, generation} <- non_negative(Map.get(attrs, "generation", 0), :generation),
         {:ok, shadow_uuid} <- optional_uuid(attrs["shadow_uuid"], "shadow_uuid"),
         {:ok, operation_id} <- optional_uuid(attrs["operation_id"], "operation_id"),
         {:ok, digest} <- optional_digest(attrs["specification_digest"]) do
      {:ok,
       %__MODULE__{
         source_uuid: source_uuid,
         enabled: false,
         location: attrs["location"],
         attachment_location: attrs["attachment_location"],
         generation: generation,
         shadow_uuid: shadow_uuid,
         operation_id: operation_id,
         specification_digest: digest
       }}
    end
  end

  defp required_text(attrs, key) do
    value = attrs[key]

    if is_binary(value) and String.trim(value) != "",
      do: {:ok, String.trim(value)},
      else: {:error, VialKeeper.Error.invalid_request("shadow #{key} must be a non-empty string")}
  end

  defp valid_attachment_location(value) do
    if Path.type(value) == :absolute and not String.contains?(value, <<0>>),
      do: :ok,
      else:
        {:error,
         VialKeeper.Error.invalid_request("shadow attachment_location must be an absolute path")}
  end

  defp generated_uuid(attrs, key) do
    value = Map.get(attrs, key, VialKeeper.UUID.v4())
    optional_uuid(value, key)
  end

  defp optional_uuid(nil, _key), do: {:ok, nil}

  defp optional_uuid(value, key) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: {:ok, String.downcase(value)},
      else: {:error, VialKeeper.Error.invalid_request("shadow #{key} must be a UUID")}
  end

  defp optional_uuid(_, key),
    do: {:error, VialKeeper.Error.invalid_request("shadow #{key} must be a UUID")}

  defp optional_digest(nil), do: {:ok, nil}

  defp optional_digest(value) when is_binary(value) do
    if byte_size(value) == 64 and Regex.match?(~r/^[0-9a-f]+$/i, value),
      do: {:ok, String.downcase(value)},
      else:
        {:error,
         VialKeeper.Error.invalid_request("shadow specification_digest must be SHA-256 hex")}
  end

  defp optional_digest(_),
    do:
      {:error, VialKeeper.Error.invalid_request("shadow specification_digest must be SHA-256 hex")}

  defp non_negative(value, _key) when is_integer(value) and value >= 0, do: {:ok, value}

  defp non_negative(_, key),
    do: {:error, VialKeeper.Error.invalid_request("shadow #{key} must be a non-negative integer")}

  defp valid_uuid(value, _field) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, VialKeeper.Error.invalid_request("shadow source_uuid must be a UUID")}
  end

  defp valid_uuid(_, field),
    do: {:error, VialKeeper.Error.invalid_request("shadow #{field} must be a UUID")}

  defp normalize_keys(map) do
    case MapAccess.string_keys(map) do
      {:ok, normalized} ->
        {:ok, normalized}

      :key_collision ->
        {:error, VialKeeper.Error.invalid_request("shadow fields contain duplicate keys")}
    end
  end
end
