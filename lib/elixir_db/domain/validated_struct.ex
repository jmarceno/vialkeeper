defmodule ElixirDB.Domain.ValidatedStruct do
  @moduledoc false

  @spec build(module(), map(), (map() -> nil | ElixirDB.Error.t())) ::
          {:ok, struct()} | {:error, ElixirDB.Error.t()}
  def build(module, attrs, validate) when is_function(validate, 1) do
    case validate.(attrs) do
      nil -> {:ok, struct(module, attrs)}
      error -> {:error, error}
    end
  end
end
