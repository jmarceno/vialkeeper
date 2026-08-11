defmodule ElixirDB.Storage.Ports.ViewState do
  @moduledoc """
  View definition, state, and row-persistence port.

  Backends expose catalog rows, generation state, and physical row/range scans.
  Shared `ElixirDB.Storage.Services.Views` owns definition CAS, rebuild
  transitions, query planning, bookmarks, and result shaping.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback list_views(BackendContext.t()) :: result([map()])
  @callback find_view_by_name(BackendContext.t(), binary()) :: result(map() | nil)
  @callback count_views(BackendContext.t()) :: result(non_neg_integer())
  @callback get_view_definition(BackendContext.t(), binary()) :: result(map())
  @callback get_view_state(BackendContext.t(), binary()) :: result(map())
  @callback insert_view(BackendContext.t(), map()) :: result(map())
  @callback delete_view(BackendContext.t(), binary()) :: result(map())
  @callback upsert_view_rows(BackendContext.t(), binary(), integer(), [map()]) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback delete_view_rows(BackendContext.t(), binary(), integer(), [binary()]) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback put_view_indexed_through(BackendContext.t(), binary(), integer()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback clear_view_generation_rows(BackendContext.t(), binary(), integer()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback begin_view_rebuild_effect(BackendContext.t(), binary(), integer()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback finish_view_rebuild_effect(BackendContext.t(), binary(), integer(), integer()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback scan_view_rows(BackendContext.t(), map()) :: result([map()])
  @callback read_winning_documents_page(BackendContext.t(), map()) :: result(map())
  @callback adapter_query_config(BackendContext.t()) :: map()
end
