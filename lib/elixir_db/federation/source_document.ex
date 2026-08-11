defmodule ElixirDB.Federation.SourceDocument do
  @moduledoc "Validated full document data returned by one federation source."

  @enforce_keys [:id, :revision, :body]
  defstruct [:id, :revision, :body]

  @type t :: %__MODULE__{
          id: binary(),
          revision: binary(),
          body: map() | nil
        }

  @doc "Builds a validated source document value."
  @spec new(binary(), binary(), map() | nil) :: t()
  def new(id, revision, body)
      when is_binary(id) and id != "" and is_binary(revision) and
             (is_map(body) or is_nil(body)) do
    %__MODULE__{id: id, revision: revision, body: body}
  end
end
