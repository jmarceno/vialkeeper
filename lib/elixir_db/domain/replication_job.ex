defmodule ElixirDB.Domain.ReplicationJob do
  @enforce_keys [:job_id, :direction, :mode, :endpoint, :enabled]
  defstruct [
    :job_id,
    :direction,
    :mode,
    :endpoint,
    :enabled,
    :persist,
    :batch,
    :retry,
    :state,
    :diagnostic
  ]
end
