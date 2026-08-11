defmodule ElixirDB.View.Document do
  @moduledoc "Evaluates declarative view definitions against document bodies."

  alias ElixirDB.View.Program

  @type row :: %{
          required(:document_id) => binary(),
          required(:revision_id) => binary(),
          required(:key) => [term()],
          optional(:value) => term()
        }

  @spec map(map(), binary(), binary(), map()) ::
          {:ok, row() | :remove} | {:error, ElixirDB.Error.t()}
  def map(definition, document_id, revision_id, body),
    do: Program.map(definition, document_id, revision_id, body)
end
