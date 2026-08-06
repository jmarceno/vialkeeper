defmodule ElixirDB.HTTP.ErrorMapper do
  @moduledoc false

  def normalize(%ElixirDB.Error{} = error), do: error
  def normalize(_), do: ElixirDB.Error.internal_error("internal server error")
end
