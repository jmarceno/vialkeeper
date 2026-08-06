defmodule ElixirDB.Domain.IndexDefinition do
  @enforce_keys [:name, :type, :fields]
  defstruct [:index_id, :name, :type, :fields, :tokenization, :definition_digest, :lifecycle_state]

  @type t :: %__MODULE__{
          index_id: binary() | nil,
          name: binary(),
          type: :structured | :full_text,
          fields: list(),
          tokenization: map() | nil,
          definition_digest: binary() | nil,
          lifecycle_state: atom() | nil
        }

  def new(attrs) when is_map(attrs) do
    with :ok <- known(attrs),
         {:ok, name} <- string(attrs, :name),
         {:ok, type} <- type(attrs),
         {:ok, fields} <- fields(attrs, type) do
      {:ok,
       struct(__MODULE__, %{
         name: name,
         type: type,
         fields: fields,
         tokenization: Map.get(attrs, :tokenization)
       })}
    end
  end

  defp known(attrs),
    do:
      if(
        Enum.all?(
          Map.keys(attrs),
          &(&1 in [
              :index_id,
              :name,
              :type,
              :fields,
              :tokenization,
              :definition_digest,
              :lifecycle_state
            ])
        ),
        do: :ok,
        else: {:error, ElixirDB.Error.invalid_request("unknown index field")}
      )

  defp string(attrs, key),
    do:
      if(is_binary(attrs[key]) and attrs[key] != "",
        do: {:ok, attrs[key]},
        else: {:error, ElixirDB.Error.invalid_request("index #{key} is required")}
      )

  defp type(%{type: "structured"}), do: {:ok, :structured}
  defp type(%{type: "full_text"}), do: {:ok, :full_text}
  defp type(%{type: type}) when type in [:structured, :full_text], do: {:ok, type}

  defp type(_),
    do: {:error, ElixirDB.Error.invalid_request("index type must be structured or full_text")}

  defp fields(%{fields: fields}, _type) when is_list(fields) and fields != [], do: {:ok, fields}

  defp fields(_, _),
    do: {:error, ElixirDB.Error.invalid_request("index fields must be a non-empty array")}
end
