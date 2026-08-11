defmodule ElixirDB.Storage.PortFault do
  @moduledoc """
  Injects scheduled failures at shared service transaction/port checkpoints.

  Faults ride on backend identity hooks (`retention_fault`, `view_fault`,
  `derived_fault`) rather than wrapping every high-level adapter callback.
  """

  alias ElixirDB.FaultAdapter, as: Fault

  @retention_points MapSet.new([
                      :before_compact_retention,
                      :compact_retention_mid_tx,
                      :after_compact_retention
                    ])
  @view_points MapSet.new([:view_upsert_row])
  @derived_points MapSet.new([:derived_upsert_row, :derived_generated_mutation])

  @type adapter :: %{
          :__struct__ => module(),
          optional(:retention_fault) => term(),
          optional(:view_fault) => term(),
          optional(:derived_fault) => term(),
          optional(atom()) => term()
        }

  @doc "Attaches a one-shot or repeating fault schedule for `point` on `adapter`."
  @spec inject(adapter(), atom(), Fault.fault()) :: adapter()
  def inject(%{__struct__: _} = adapter, point, fault_spec) when is_atom(point) do
    schedule = Fault.wrap(:port) |> Fault.inject(point, fault_spec)
    fun = fault_fun(schedule)
    Map.put(adapter, field_for!(point), fun)
  end

  @doc "Runs compact retention through the real adapter, honoring before/after points."
  @spec compact_retention(adapter(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def compact_retention(adapter, request \\ %{})

  def compact_retention(%{__struct__: mod} = adapter, request) when is_map(request) do
    with :ok <- invoke_fault(adapter, :retention_fault, :before_compact_retention),
         result <- mod.compact_retention(adapter, request) do
      fail_after(adapter, :retention_fault, :after_compact_retention, result)
    end
  end

  @doc "Runs apply_view_batch through the real adapter."
  @spec apply_view_batch(adapter(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_view_batch(%{__struct__: mod} = adapter, request) when is_map(request),
    do: mod.apply_view_batch(adapter, request)

  @doc "Runs apply_derived_source_batch through the real adapter."
  @spec apply_derived_source_batch(adapter(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_source_batch(%{__struct__: mod} = adapter, request) when is_map(request),
    do: mod.apply_derived_source_batch(adapter, request)

  @doc "Runs apply_derived_rebuild_page through the real adapter."
  @spec apply_derived_rebuild_page(adapter(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def apply_derived_rebuild_page(%{__struct__: mod} = adapter, request) when is_map(request),
    do: mod.apply_derived_rebuild_page(adapter, request)

  @doc "Runs prune_derived_rebuild_stale_page through the real adapter."
  @spec prune_derived_rebuild_stale_page(adapter(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def prune_derived_rebuild_stale_page(%{__struct__: mod} = adapter, request)
      when is_map(request),
      do: mod.prune_derived_rebuild_stale_page(adapter, request)

  defp field_for!(point) do
    cond do
      MapSet.member?(@retention_points, point) -> :retention_fault
      MapSet.member?(@view_points, point) -> :view_fault
      MapSet.member?(@derived_points, point) -> :derived_fault
      true -> raise ArgumentError, "unsupported port fault point: #{inspect(point)}"
    end
  end

  defp fault_fun(schedule) do
    {:ok, agent} = Agent.start_link(fn -> schedule end)

    fn point ->
      Agent.get_and_update(agent, &evaluate_fault(&1, point))
    end
  end

  defp evaluate_fault(current, point) do
    case Fault.maybe_fail(current, point) do
      {:ok, updated} -> {:ok, updated}
      {:error, error, updated} -> {{:error, error}, updated}
    end
  end

  defp invoke_fault(adapter, field, point) do
    case Map.get(adapter, field) do
      fun when is_function(fun, 1) ->
        case fun.(point) do
          :ok -> :ok
          {:error, %ElixirDB.Error{} = error} -> {:error, error}
        end

      _ ->
        :ok
    end
  end

  defp fail_after(adapter, field, point, result) do
    case invoke_fault(adapter, field, point) do
      :ok -> result
      {:error, _} = error -> error
    end
  end
end
