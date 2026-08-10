defmodule ElixirDB.TestSupport.ViewBuilderProbe do
  @moduledoc "Test helper for observing view builder lifecycle probe messages."
  import ExUnit.Assertions

  @doc "Installs a probe that receives `{:view_builder_probe, ref, uuid, view_id, event, metadata}`."
  @spec install(pid()) :: reference()
  def install(pid \\ self()) do
    ref = make_ref()
    Application.put_env(:elixir_db, :view_builder_probe, {pid, ref})
    ref
  end

  @spec uninstall() :: :ok
  def uninstall do
    Application.delete_env(:elixir_db, :view_builder_probe)
    uninstall_apply_view_batch_barrier()
    :ok
  end

  @doc """
  Arms an owner-body sync gate so the next `:apply_view_batch` maintenance command
  blocks after `before_incremental_apply` fires.
  """
  @spec install_apply_view_batch_barrier(binary(), pid()) :: reference()
  def install_apply_view_batch_barrier(uuid, pid \\ self()) when is_binary(uuid) do
    gate = make_ref()

    :ok =
      Application.put_env(
        :elixir_db,
        :admitted_command_owner_body_sync,
        {pid, gate, uuid, :apply_view_batch}
      )

    gate
  end

  @spec uninstall_apply_view_batch_barrier() :: :ok
  def uninstall_apply_view_batch_barrier do
    Application.delete_env(:elixir_db, :admitted_command_owner_body_sync)
    :ok
  end

  @doc """
  Waits for `before_incremental_apply` and the matching owner-body gate, returning
  `{metadata, executor_pid}` while the builder is blocked before apply commits.
  """
  @spec await_incremental_apply_blocked(reference(), reference(), timeout()) ::
          {map(), pid()}
  def await_incremental_apply_blocked(ref, gate, timeout \\ 5_000) do
    metadata = await(ref, :before_incremental_apply, timeout)

    receive do
      {^gate, :owner_body, executor} when is_pid(executor) ->
        {metadata, executor}
    after
      timeout ->
        flunk("expected apply_view_batch owner-body gate #{inspect(gate)} within #{timeout}ms")
    end
  end

  @spec release_apply_view_batch_barrier(reference(), pid()) :: :ok
  def release_apply_view_batch_barrier(gate, executor) when is_pid(executor) do
    send(executor, {:go, gate})
    uninstall_apply_view_batch_barrier()
  end

  @spec await(reference(), atom(), timeout()) :: map()
  def await(ref, event, timeout \\ 5_000) do
    receive do
      {:view_builder_probe, ^ref, _uuid, _view_id, ^event, metadata} ->
        metadata
    after
      timeout -> flunk("expected view builder probe #{inspect(event)} within #{timeout}ms")
    end
  end

  @spec drain(reference(), timeout()) :: [{atom(), map()}]
  def drain(ref, timeout \\ 0) do
    drain(ref, [], timeout)
  end

  defp drain(_ref, acc, 0), do: Enum.reverse(acc)

  defp drain(ref, acc, timeout) do
    receive do
      {:view_builder_probe, ^ref, _uuid, _view_id, event, metadata} ->
        drain(ref, [{event, metadata} | acc], timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end
