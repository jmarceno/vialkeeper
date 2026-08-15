defmodule VialKeeper.Domain.ValidatedStruct do
  @moduledoc "Small validation-to-struct construction helper for domain types."

  @spec build(module(), map(), (map() -> nil | VialKeeper.Error.t())) ::
          {:ok, struct()} | {:error, VialKeeper.Error.t()}
  def build(module, attrs, validate) when is_function(validate, 1) do
    case validate.(attrs) do
      nil -> {:ok, struct(module, attrs)}
      error -> {:error, error}
    end
  end
end
