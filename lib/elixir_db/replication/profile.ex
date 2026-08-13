defmodule ElixirDB.Replication.Profile do
  @moduledoc "Explicit orchestration profile for peer and shadow replication."

  @enforce_keys [:kind]
  defstruct [
    :kind,
    :source_database_uuid,
    :target_database_uuid,
    :generation,
    :operation_id
  ]

  @type kind :: :peer | :shadow
  @type t :: %__MODULE__{
          kind: kind(),
          source_database_uuid: binary() | nil,
          target_database_uuid: binary() | nil,
          generation: pos_integer() | nil,
          operation_id: binary() | nil
        }

  @spec peer() :: t()
  def peer, do: %__MODULE__{kind: :peer}

  @spec shadow(map() | keyword()) :: t()
  def shadow(attrs) when is_list(attrs), do: shadow(Map.new(attrs))

  def shadow(attrs) when is_map(attrs) do
    %__MODULE__{
      kind: :shadow,
      source_database_uuid: fetch(attrs, :source_database_uuid),
      target_database_uuid: fetch(attrs, :target_database_uuid),
      generation: fetch(attrs, :generation),
      operation_id: fetch(attrs, :operation_id)
    }
  end

  @spec shadow?(t()) :: boolean()
  def shadow?(%__MODULE__{kind: :shadow}), do: true
  def shadow?(_), do: false

  @spec validate(t()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate(%__MODULE__{kind: :peer}), do: :ok

  def validate(%__MODULE__{kind: :shadow} = profile) do
    with :ok <- uuid(profile.source_database_uuid, "source_database_uuid"),
         :ok <- uuid(profile.target_database_uuid, "target_database_uuid"),
         :ok <- positive_generation(profile.generation),
         :ok <- uuid(profile.operation_id, "operation_id") do
      different_uuids(profile.source_database_uuid, profile.target_database_uuid)
    end
  end

  def validate(_),
    do: {:error, ElixirDB.Error.invalid_request("replication profile is invalid")}

  defp fetch(attrs, key), do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))

  defp uuid(value, field) when is_binary(value) do
    if Regex.match?(
         ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
         value
       ),
       do: :ok,
       else: {:error, ElixirDB.Error.invalid_request("shadow #{field} must be a UUID")}
  end

  defp uuid(_value, field),
    do: {:error, ElixirDB.Error.invalid_request("shadow #{field} must be a UUID")}

  defp positive_generation(value) when is_integer(value) and value > 0, do: :ok

  defp positive_generation(_value),
    do: {:error, ElixirDB.Error.invalid_request("shadow generation must be positive")}

  defp different_uuids(source, target) when source != target, do: :ok

  defp different_uuids(_source, _target),
    do:
      {:error,
       ElixirDB.Error.shadow_identity_conflict("shadow source and target UUIDs must differ")}
end
