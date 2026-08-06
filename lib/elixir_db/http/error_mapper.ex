defmodule ElixirDB.HTTP.ErrorMapper do
  @moduledoc false

  @spec normalize(term()) :: ElixirDB.Error.t()
  def normalize(%ElixirDB.Error{} = error), do: error

  def normalize({:error, %ElixirDB.Error{} = error}), do: error

  def normalize(other),
    do: ElixirDB.Error.new(:internal_error, "internal server error", %{}, cause: other)
end
