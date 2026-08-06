defmodule ElixirDB.Domain.Leaf do
  @enforce_keys [:revision, :deleted]
  defstruct [:revision, :deleted]
  @type t :: %__MODULE__{revision: binary(), deleted: boolean()}
end
