defmodule ElixirDB.Storage.Ports.ViewState do
  @moduledoc """
  View definition, state, and row-persistence port.

  Shared view code decides map/reduce evaluation and public query behavior.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback list_views(BackendContext.t()) :: result([map()])
  @callback create_view(BackendContext.t(), map()) :: result(map())
  @callback delete_view(BackendContext.t(), binary()) :: result(map())
  @callback view_state(BackendContext.t(), binary()) :: result(map())
  @callback apply_view_batch(BackendContext.t(), map()) :: result(map())
  @callback begin_view_rebuild(BackendContext.t(), map()) :: result(map())
  @callback append_view_rebuild_page(BackendContext.t(), map()) :: result(map())
  @callback finish_view_rebuild(BackendContext.t(), map()) :: result(map())
  @callback read_view_rows(BackendContext.t(), map()) :: result(map())
  @callback read_winning_documents_page(BackendContext.t(), map()) :: result(map())
end
