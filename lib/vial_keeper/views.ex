defmodule VialKeeper.Views do
  @moduledoc "Public facade for declarative map/reduce views."

  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.{Builder, Manager}

  @spec create(binary(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def create(uuid, definition) when is_binary(uuid) and is_map(definition) do
    create(uuid, definition, [])
  end

  @spec create(binary(), map(), keyword()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def create(uuid, definition, opts)
      when is_binary(uuid) and is_map(definition) and is_list(opts) do
    with {:ok, created} <- DatabaseCatalog.command(uuid, {:command, :create_view, definition}),
         view_id <- created["view_id"],
         :ok <- Manager.start_builder(uuid, view_id, opts) do
      {:ok, created}
    end
  end

  @spec delete(binary(), binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def delete(uuid, view_id) when is_binary(uuid) and is_binary(view_id) do
    with :ok <- Manager.stop_builder(uuid, view_id) do
      DatabaseCatalog.command(uuid, {:command, :delete_view, view_id})
    end
  end

  @spec list(binary()) :: {:ok, [map()]} | {:error, VialKeeper.Error.t()}
  def list(uuid) when is_binary(uuid),
    do: DatabaseCatalog.command(uuid, {:command, :list_views, %{}})

  @spec state(binary(), binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def state(uuid, view_id) when is_binary(uuid) and is_binary(view_id),
    do: DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

  @spec rebuild(binary(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def rebuild(uuid, view_id) when is_binary(uuid) and is_binary(view_id),
    do: Builder.request_rebuild(uuid, view_id)

  @spec query(binary(), binary(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def query(uuid, view_id, request)
      when is_binary(uuid) and is_binary(view_id) and is_map(request) do
    consistency = Map.get(request, "consistency", "stale_ok")

    with :ok <- validate_consistency(consistency),
         {:ok, _state} <- state(uuid, view_id),
         {:ok, target, timeout} <- consistency_target(uuid, consistency),
         :ok <- await_if_consistent(uuid, view_id, consistency, target, timeout),
         result <-
           DatabaseCatalog.command(
             uuid,
             {:command, :query_view, Map.put(request, "view_id", view_id)}
           ),
         :ok <- request_catch_up(uuid, view_id, consistency) do
      result
    end
  end

  @spec await_consistent(binary(), binary(), non_neg_integer(), timeout()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def await_consistent(uuid, view_id, target_sequence, timeout \\ 5_000)
      when is_binary(uuid) and is_binary(view_id) and is_integer(target_sequence) do
    Builder.await_sequence(uuid, view_id, target_sequence, timeout)
  end

  defp validate_consistency(mode) when mode in ["stale_ok", "update_after", "consistent"], do: :ok

  defp validate_consistency(_mode),
    do:
      {:error,
       VialKeeper.Error.invalid_request(
         "view consistency must be stale_ok, update_after, or consistent"
       )}

  defp consistency_target(_uuid, "stale_ok"), do: {:ok, nil, 0}
  defp consistency_target(_uuid, "update_after"), do: {:ok, nil, 0}

  defp consistency_target(uuid, "consistent") do
    with {:ok, identity} <- DatabaseCatalog.command(uuid, {:command, :identity, %{}}) do
      config_wait = get_in(identity, [:config, "views", "consistent_wait_ms"]) || 5_000
      {:ok, identity.current_sequence, config_wait}
    end
  end

  defp await_if_consistent(_uuid, _view_id, mode, _target, _timeout) when mode != "consistent",
    do: :ok

  defp await_if_consistent(uuid, view_id, "consistent", target, timeout),
    do: await_consistent(uuid, view_id, target, timeout)

  defp request_catch_up(_uuid, _view_id, "stale_ok"), do: :ok

  defp request_catch_up(uuid, view_id, _mode) do
    case Builder.request_catch_up(uuid, view_id) do
      :ok -> :ok
      _ -> :ok
    end
  end
end
