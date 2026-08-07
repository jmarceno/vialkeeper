defmodule ElixirDB.Changes.Request do
  @moduledoc "Normalized bounded request for reading the changes feed."

  @enforce_keys [:since, :limit, :wait_ms]
  defstruct [:since, :limit, :wait_ms]

  @type t :: %__MODULE__{
          since: non_neg_integer(),
          limit: pos_integer(),
          wait_ms: non_neg_integer()
        }
end
