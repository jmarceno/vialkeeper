defmodule ElixirDB.Storage.Ports.DerivedState do
  @moduledoc """
  Derived-materialization state port.

  Metadata, source cursors, contributions, and atomic batch primitives. Shared
  materialization code decides the effect of a batch.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback get_derived_view(BackendContext.t()) :: result(map())
  @callback set_derived_enabled(BackendContext.t(), map()) :: result(map())
  @callback list_derived_sources(BackendContext.t()) :: result([map()])
  @callback set_derived_source_error(BackendContext.t(), map()) :: result(map())
  @callback apply_derived_source_batch(BackendContext.t(), map()) :: result(map())
  @callback begin_derived_source_rebuild(BackendContext.t(), map()) :: result(map())
  @callback apply_derived_rebuild_page(BackendContext.t(), map()) :: result(map())
  @callback prune_derived_rebuild_stale_page(BackendContext.t(), map()) :: result(map())
  @callback finish_derived_source_rebuild(BackendContext.t(), map()) :: result(map())
end
