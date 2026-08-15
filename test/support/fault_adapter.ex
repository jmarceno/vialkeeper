defmodule VialKeeper.FaultAdapter do
  @moduledoc """
  Injects failures at named points for replication and adapter fault tests.

  Scaffolding for fault-injection tests: wrap a real endpoint or adapter call
  site and fail before or after named transitions without changing production
  code.
  """

  defstruct inner: nil, faults: %{}, hits: %{}

  @type fault ::
          VialKeeper.Error.t()
          | {:error, VialKeeper.Error.t()}
          | {:once, VialKeeper.Error.t()}
          | {:times, pos_integer(), VialKeeper.Error.t()}
          | (map() -> :ok | {:error, VialKeeper.Error.t()})

  @type t :: %__MODULE__{
          inner: term(),
          faults: %{optional(atom()) => fault()},
          hits: %{optional(atom()) => non_neg_integer()}
        }

  @doc """
  Wraps an inner adapter or endpoint handle with an empty fault schedule.
  """
  @spec wrap(term()) :: t()
  def wrap(inner), do: %__MODULE__{inner: inner, faults: %{}, hits: %{}}

  @doc """
  Schedules a fault at a named injection point.
  """
  @spec inject(t(), atom(), fault()) :: t()
  def inject(%__MODULE__{} = adapter, point, fault) when is_atom(point) do
    %{adapter | faults: Map.put(adapter.faults, point, fault)}
  end

  @doc """
  Clears the fault schedule for one named point.
  """
  @spec clear(t(), atom()) :: t()
  def clear(%__MODULE__{} = adapter, point) when is_atom(point) do
    %{adapter | faults: Map.delete(adapter.faults, point)}
  end

  @doc """
  Clears every scheduled fault.
  """
  @spec clear_all(t()) :: t()
  def clear_all(%__MODULE__{} = adapter), do: %{adapter | faults: %{}, hits: %{}}

  @doc """
  Returns `:ok` or an injected error for `point`, updating one-shot/times counters.
  """
  @spec maybe_fail(t(), atom()) :: {:ok, t()} | {:error, VialKeeper.Error.t(), t()}
  def maybe_fail(%__MODULE__{} = adapter, point) when is_atom(point) do
    hits = Map.update(adapter.hits, point, 1, &(&1 + 1))
    adapter = %{adapter | hits: hits}
    fault_result(adapter, point, Map.get(adapter.faults, point))
  end

  defp fault_result(adapter, _point, nil), do: {:ok, adapter}

  defp fault_result(adapter, _point, %VialKeeper.Error{} = error),
    do: {:error, error, adapter}

  defp fault_result(adapter, _point, {:error, %VialKeeper.Error{} = error}),
    do: {:error, error, adapter}

  defp fault_result(adapter, point, {:once, %VialKeeper.Error{} = error}),
    do: {:error, error, clear(adapter, point)}

  defp fault_result(adapter, point, {:times, 1, %VialKeeper.Error{} = error}),
    do: {:error, error, clear(adapter, point)}

  defp fault_result(adapter, point, {:times, n, %VialKeeper.Error{} = error})
       when is_integer(n) and n > 1,
       do: {:error, error, inject(adapter, point, {:times, n - 1, error})}

  defp fault_result(adapter, _point, fun) when is_function(fun, 1),
    do: run_fault_function(fun, adapter)

  defp run_fault_function(fun, adapter) do
    case fun.(adapter) do
      :ok -> {:ok, adapter}
      {:error, %VialKeeper.Error{} = error} -> {:error, error, adapter}
    end
  end

  @doc """
  Runs `fun` against the inner handle unless `point` injects a failure first.
  """
  @spec call(t(), atom(), (term() -> result)) :: {result | {:error, VialKeeper.Error.t()}, t()}
        when result: term()
  def call(%__MODULE__{} = adapter, point, fun) when is_atom(point) and is_function(fun, 1) do
    case maybe_fail(adapter, point) do
      {:ok, adapter} -> {fun.(adapter.inner), adapter}
      {:error, error, adapter} -> {{:error, error}, adapter}
    end
  end
end
