defmodule ElixirDB.Storage.Results do
  @moduledoc "Storage-neutral result aliases and constructors."

  def ok(value), do: {:ok, value}
  def error(%ElixirDB.Error{} = error), do: {:error, error}
end
