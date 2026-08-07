defmodule ElixirDB.Domain.Revision do
  @moduledoc "Validated immutable document revision state."

  @enforce_keys [:document_id, :revision_id, :generation, :parent_revision, :deleted, :body]
  defstruct [
    :document_id,
    :revision_id,
    :generation,
    :parent_revision,
    :digest,
    :deleted,
    :body,
    :insertion_sequence
  ]

  @type t :: %__MODULE__{
          document_id: binary() | nil,
          revision_id: binary(),
          generation: pos_integer(),
          parent_revision: binary() | nil,
          digest: binary() | nil,
          deleted: boolean(),
          body: map() | nil,
          insertion_sequence: non_neg_integer() | nil
        }

  def new(attrs) when is_map(attrs) do
    with :ok <- validate_attrs(attrs),
         {:ok, generation} <- generation(attrs),
         :ok <- validate_body(attrs) do
      {:ok, struct(__MODULE__, Map.put(attrs, :generation, generation))}
    end
  end

  defp validate_attrs(attrs) do
    required = [:document_id, :revision_id, :deleted]

    if Enum.all?(required, &Map.has_key?(attrs, &1)) and is_binary(attrs.document_id) and
         is_binary(attrs.revision_id) and is_boolean(attrs.deleted),
       do: :ok,
       else: {:error, ElixirDB.Error.invalid_request("invalid revision fields")}
  end

  defp generation(%{generation: generation}) when is_integer(generation) and generation > 0,
    do: {:ok, generation}

  defp generation(_),
    do: {:error, ElixirDB.Error.invalid_request("revision generation must be positive")}

  defp validate_body(%{deleted: true, body: nil}), do: :ok
  defp validate_body(%{deleted: false, body: body}) when is_map(body), do: :ok

  defp validate_body(_),
    do: {:error, ElixirDB.Error.invalid_request("revision body does not match deletion state")}
end
