defmodule VialKeeper.Federation.SourceCursor do
  @moduledoc "Immutable cursor state for one source during a federation request."

  @enforce_keys [:source_uuid]
  defstruct source_uuid: nil,
            sequence: nil,
            buffered_documents: [],
            source_bookmark: nil,
            exhausted?: false

  @type t :: %__MODULE__{
          source_uuid: binary(),
          sequence: non_neg_integer() | nil,
          buffered_documents: [map()],
          source_bookmark: binary() | nil,
          exhausted?: boolean()
        }

  @doc "Creates an empty cursor for a registered source."
  @spec new(binary()) :: t()
  def new(source_uuid) when is_binary(source_uuid), do: %__MODULE__{source_uuid: source_uuid}

  @doc "Replaces the cursor buffer with one validated source page."
  @spec put_page(t(), non_neg_integer(), [map()], binary() | nil, boolean()) :: t()
  def put_page(%__MODULE__{} = cursor, sequence, documents, source_bookmark, has_more?)
      when is_integer(sequence) and sequence >= 0 and is_list(documents) and
             (is_binary(source_bookmark) or is_nil(source_bookmark)) and is_boolean(has_more?) do
    %{
      cursor
      | sequence: sequence,
        buffered_documents: documents,
        source_bookmark: source_bookmark,
        exhausted?: not has_more?
    }
  end

  @doc "Returns whether the cursor has no buffered source document."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{buffered_documents: documents}), do: documents == []

  @doc "Returns the current source head, or `:empty`."
  @spec head(t()) :: {:ok, map()} | :empty
  def head(%__MODULE__{buffered_documents: [document | _]}), do: {:ok, document}
  def head(%__MODULE__{buffered_documents: []}), do: :empty

  @doc "Removes and returns the current source head, or `:empty`."
  @spec pop(t()) :: {{:ok, map()}, t()} | :empty
  def pop(%__MODULE__{buffered_documents: [document | rest]} = cursor) do
    {{:ok, document}, %{cursor | buffered_documents: rest}}
  end

  def pop(%__MODULE__{buffered_documents: []}), do: :empty
end
