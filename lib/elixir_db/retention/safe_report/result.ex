defmodule ElixirDB.Retention.SafeReport.Result do
  @moduledoc false

  @enforce_keys [:safe_source_sequence, :advanced, :reason]
  defstruct [:safe_source_sequence, :advanced, :reason]

  @type t :: %__MODULE__{
          safe_source_sequence: non_neg_integer(),
          advanced: boolean(),
          reason: atom()
        }

  @spec unchanged(non_neg_integer()) :: t()
  def unchanged(safe_source_sequence) do
    %__MODULE__{
      safe_source_sequence: safe_source_sequence,
      advanced: false,
      reason: :unchanged
    }
  end

  @spec advanced(non_neg_integer()) :: t()
  def advanced(safe_source_sequence) do
    %__MODULE__{
      safe_source_sequence: safe_source_sequence,
      advanced: true,
      reason: :advanced
    }
  end

  @spec blocked(non_neg_integer(), atom()) :: t()
  def blocked(safe_source_sequence, reason) do
    %__MODULE__{
      safe_source_sequence: safe_source_sequence,
      advanced: false,
      reason: reason
    }
  end
end
