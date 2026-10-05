defmodule VialKeeper.Storage.SequenceView do
  @moduledoc """
  The visible watermark, the data version, and the data version the serving
  ledger started its run at (`data_version_base`). Every data version of an
  earlier run is below the base.
  """

  @enforce_keys [:visible, :data_version, :data_version_base]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          visible: non_neg_integer(),
          data_version: pos_integer(),
          data_version_base: pos_integer()
        }
end
