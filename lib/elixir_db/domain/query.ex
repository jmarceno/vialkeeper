defmodule ElixirDB.Domain.Query do
  @enforce_keys [:selector]
  defstruct [:selector, :sort, :fields, :limit, :bookmark, :index, :search]

  @type t :: %__MODULE__{
          selector: map(),
          sort: list(),
          fields: list() | nil,
          limit: pos_integer(),
          bookmark: binary() | nil,
          index: binary() | nil,
          search: map() | nil
        }
end
