defmodule ElixirDB.Shadow.Definition do
  @moduledoc "Validated desired state for one source-bound managed shadow."

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

  @spec new(String.t(), map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(source_uuid, attrs) when is_binary(source_uuid) and is_map(attrs) do
    attrs = string_keys(attrs)

    with :ok <- valid_uuid(source_uuid, "source_uuid"),
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
    do: {:error, ElixirDB.Error.invalid_request("shadow definition must be an object")}

  @spec disabled(t()) :: t()
  def disabled(%__MODULE__{} = definition), do: %{definition | enabled: false}

  @spec replace(t(), map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def replace(%__MODULE__{} = previous, attrs) when is_map(attrs) do
    attrs =
      Map.merge(Map.from_struct(previous), attrs) |> Map.put(:generation, previous.generation + 1)

    new(previous.source_uuid, attrs)
  end

  @spec from_map(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_map(map) when is_map(map) do
    map = string_keys(map)
    source_uuid = map["source_uuid"]
    enabled = map["enabled"]
    attrs = Map.put(map, "generation", Map.get(map, "generation", 0))

    case enabled do
      true ->
        new(source_uuid, attrs)

      false ->
        disabled_from_map(source_uuid, attrs)

      _ ->
        {:error, ElixirDB.Error.integrity_violation("shadow desired state enabled flag is invalid")}
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
    attrs = string_keys(attrs)

    with :ok <- valid_uuid(source_uuid, "source_uuid"),
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
      else: {:error, ElixirDB.Error.invalid_request("shadow #{key} must be a non-empty string")}
  end

  defp valid_attachment_location(value) do
    if Path.type(value) == :absolute and not String.contains?(value, <<0>>),
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.invalid_request("shadow attachment_location must be an absolute path")}
  end

  defp generated_uuid(attrs, key) do
    value = Map.get(attrs, key, ElixirDB.UUID.v4())
    optional_uuid(value, key)
  end

  defp optional_uuid(nil, _key), do: {:ok, nil}

  defp optional_uuid(value, key) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: {:ok, String.downcase(value)},
      else: {:error, ElixirDB.Error.invalid_request("shadow #{key} must be a UUID")}
  end

  defp optional_uuid(_, key),
    do: {:error, ElixirDB.Error.invalid_request("shadow #{key} must be a UUID")}

  defp optional_digest(nil), do: {:ok, nil}

  defp optional_digest(value) when is_binary(value) do
    if byte_size(value) == 64 and Regex.match?(~r/^[0-9a-f]+$/i, value),
      do: {:ok, String.downcase(value)},
      else:
        {:error, ElixirDB.Error.invalid_request("shadow specification_digest must be SHA-256 hex")}
  end

  defp optional_digest(_),
    do: {:error, ElixirDB.Error.invalid_request("shadow specification_digest must be SHA-256 hex")}

  defp non_negative(value, _key) when is_integer(value) and value >= 0, do: {:ok, value}

  defp non_negative(_, key),
    do: {:error, ElixirDB.Error.invalid_request("shadow #{key} must be a non-negative integer")}

  defp valid_uuid(value, _field) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("shadow source_uuid must be a UUID")}
  end

  defp valid_uuid(_, field),
    do: {:error, ElixirDB.Error.invalid_request("shadow #{field} must be a UUID")}

  defp string_keys(map),
    do:
      Map.new(map, fn {key, value} ->
        {if(is_atom(key), do: Atom.to_string(key), else: key), value}
      end)
end
