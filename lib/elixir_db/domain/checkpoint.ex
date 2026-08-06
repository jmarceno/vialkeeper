defmodule ElixirDB.Domain.Checkpoint do
  @enforce_keys [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]
  defstruct [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]

  @type t :: %__MODULE__{
          version: pos_integer(),
          replication_id: binary(),
          checkpoint_version: non_neg_integer(),
          session_id: binary(),
          source_sequence: non_neg_integer(),
          history: list()
        }
end
