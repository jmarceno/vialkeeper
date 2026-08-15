defmodule VialKeeper.Storage.Ports.DerivedState do
  @moduledoc """
  Derived-materialization fact and effect port.

  Backends expose metadata, source cursors, contribution maps, group aggregates,
  and atomic persistence effects. Shared `VialKeeper.Storage.Services.DerivedViews`
  owns apply/rebuild product workflows, batch effects, reducers, and generated
  outputs.
  """

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @callback get_derived_metadata(BackendContext.t()) :: result(map())
  @callback list_derived_sources(BackendContext.t()) :: result([map()])
  @callback get_derived_source(BackendContext.t(), binary()) :: result(map())
  @callback put_derived_enabled(BackendContext.t(), map()) :: result(map())
  @callback put_derived_source_error(BackendContext.t(), map()) :: result(map())
  @callback put_derived_rebuild_begin(BackendContext.t(), map()) :: result(map())
  @callback put_derived_rebuild_cursor(BackendContext.t(), map()) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback put_derived_rebuild_finish(BackendContext.t(), map()) :: result(map())
  @callback put_derived_source_checkpoint(BackendContext.t(), map()) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback clear_derived_source_error(BackendContext.t(), binary()) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback refresh_derived_status(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback fetch_contributions(BackendContext.t(), binary(), [binary()]) :: result(map())
  @callback delete_contributions(BackendContext.t(), binary(), [binary()]) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback upsert_contributions(BackendContext.t(), binary(), [map()], pos_integer()) ::
              :ok | {:error, VialKeeper.Error.t()}
  @callback list_stale_contribution_ids(BackendContext.t(), map()) :: result([binary()])
  @callback fetch_group(BackendContext.t(), binary(), binary()) :: result(map())
  @callback upsert_group(BackendContext.t(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback delete_group(BackendContext.t(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback list_group_numeric_values(BackendContext.t(), binary()) ::
              result([{term(), binary() | nil, binary(), binary()}])
end
