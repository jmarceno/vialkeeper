defmodule ElixirDB.Domain.DatabaseInfo do
  @enforce_keys [:database_uuid, :current_sequence, :file_format_version, :logical_schema_version]
  defstruct [
    :database_uuid,
    :current_sequence,
    :file_format_version,
    :logical_schema_version,
    :config,
    :runtime_state
  ]

  @type t :: %__MODULE__{
          database_uuid: binary(),
          current_sequence: non_neg_integer(),
          file_format_version: pos_integer(),
          logical_schema_version: pos_integer(),
          config: map() | nil,
          runtime_state: atom() | nil
        }

  def new(attrs) when is_map(attrs) do
    with {:ok, uuid} <- required_string(attrs, :database_uuid),
         {:ok, sequence} <- required_nonnegative(attrs, :current_sequence),
         {:ok, format} <- required_positive(attrs, :file_format_version),
         {:ok, schema} <- required_positive(attrs, :logical_schema_version) do
      {:ok,
       struct(__MODULE__, %{
         database_uuid: uuid,
         current_sequence: sequence,
         file_format_version: format,
         logical_schema_version: schema,
         config: Map.get(attrs, :config),
         runtime_state: Map.get(attrs, :runtime_state)
       })}
    end
  end

  defp required_string(map, key),
    do:
      if(is_binary(map[key]) and map[key] != "",
        do: {:ok, map[key]},
        else: {:error, ElixirDB.Error.invalid_request("#{key} must be a non-empty string")}
      )

  defp required_nonnegative(map, key),
    do:
      if(is_integer(map[key]) and map[key] >= 0,
        do: {:ok, map[key]},
        else: {:error, ElixirDB.Error.invalid_request("#{key} must be a non-negative integer")}
      )

  defp required_positive(map, key),
    do:
      if(is_integer(map[key]) and map[key] > 0,
        do: {:ok, map[key]},
        else: {:error, ElixirDB.Error.invalid_request("#{key} must be a positive integer")}
      )
end
