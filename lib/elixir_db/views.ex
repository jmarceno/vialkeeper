defmodule ElixirDB.Views do
  @moduledoc "Public facade for declarative map/reduce views."

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.View.{Builder, Manager}

  @spec create(binary(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(uuid, definition) when is_binary(uuid) and is_map(definition) do
    create(uuid, definition, [])
  end

  @spec create(binary(), map(), keyword()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(uuid, definition, opts)
      when is_binary(uuid) and is_map(definition) and is_list(opts) do
    with {:ok, created} <- DatabaseCatalog.command(uuid, {:command, :create_view, definition}),
         view_id <- created["view_id"],
         :ok <- Manager.start_builder(uuid, view_id, opts) do
      {:ok, created}
    end
  end

  @spec delete(binary(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def delete(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    with :ok <- Manager.stop_builder(uuid, view_id) do
      DatabaseCatalog.command(uuid, {:command, :delete_view, view_id})
    end
  end

  @spec list(binary()) :: {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list(uuid) when is_binary(uuid),
    do: DatabaseCatalog.command(uuid, {:command, :list_views, %{}})

  @spec state(binary(), binary()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def state(uuid, view_id) when is_binary(uuid) and is_binary(view_id),
    do: DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

  @spec rebuild(binary(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def rebuild(uuid, view_id) when is_binary(uuid) and is_binary(view_id),
    do: Builder.request_rebuild(uuid, view_id)

  @spec await_consistent(binary(), binary(), non_neg_integer(), timeout()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def await_consistent(uuid, view_id, target_sequence, timeout \\ 5_000)
      when is_binary(uuid) and is_binary(view_id) and is_integer(target_sequence) do
    Builder.await_sequence(uuid, view_id, target_sequence, timeout)
  end
end
