defmodule VialKeeper.Storage.Ports.ChangeLog do
  @moduledoc """
  Change-log fact port: sequence reservations, append, and causal reads.

  Sequences are handed out by the runtime sequence ledger, never by a write
  transaction. The backend only stores the highest sequence that may have
  been handed out (`sequence_high_water/1`), raised by
  `persist_sequence_reservation/2` in a transaction of its own.

  Retention policy and product change-feed shaping remain outside this port.
  """

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}
  @type change_entry :: map()

  @callback sequence_high_water(BackendContext.t()) :: result(non_neg_integer())
  @callback persist_sequence_reservation(BackendContext.t(), non_neg_integer()) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback append_change(BackendContext.t(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback append_changes(BackendContext.t(), [map()]) :: :ok | {:error, VialKeeper.Error.t()}
  @callback read_page(BackendContext.t(), non_neg_integer(), non_neg_integer(), pos_integer()) ::
              result(%{
                results: [change_entry()],
                last_sequence: non_neg_integer(),
                has_more: boolean()
              })
  @callback has_local_origin_changes?(BackendContext.t()) :: result(boolean())
  @callback has_local_origin_changes?(BackendContext.t(), binary() | nil) :: result(boolean())
  @callback clear_pending_local_causal(BackendContext.t()) :: result(:cleared)
  @callback clear_pending_local_causal(BackendContext.t(), binary() | nil) :: result(:cleared)
  @callback delete_through_boundary(BackendContext.t(), non_neg_integer()) ::
              :ok | {:error, VialKeeper.Error.t()}
end
