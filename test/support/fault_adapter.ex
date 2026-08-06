defmodule ElixirDB.FaultAdapter do
  @moduledoc """
  Injects failures at named points for replication and adapter fault tests.

  Scaffolding for Plan §12.4 / gap B2: wrap a real endpoint or adapter call site
  and fail before or after named transitions without changing production code.
  """

  defstruct inner: nil, faults: %{}, hits: %{}

  @type fault ::
          ElixirDB.Error.t()
          | {:error, ElixirDB.Error.t()}
          | {:once, ElixirDB.Error.t()}
          | {:times, pos_integer(), ElixirDB.Error.t()}
          | (map() -> :ok | {:error, ElixirDB.Error.t()})

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
  @spec maybe_fail(t(), atom()) :: {:ok, t()} | {:error, ElixirDB.Error.t(), t()}
  def maybe_fail(%__MODULE__{} = adapter, point) when is_atom(point) do
    hits = Map.update(adapter.hits, point, 1, &(&1 + 1))
    adapter = %{adapter | hits: hits}

    case Map.get(adapter.faults, point) do
      nil ->
        {:ok, adapter}

      %ElixirDB.Error{} = error ->
        {:error, error, adapter}

      {:error, %ElixirDB.Error{} = error} ->
        {:error, error, adapter}

      {:once, %ElixirDB.Error{} = error} ->
        {:error, error, clear(adapter, point)}

      {:times, 1, %ElixirDB.Error{} = error} ->
        {:error, error, clear(adapter, point)}

      {:times, n, %ElixirDB.Error{} = error} when is_integer(n) and n > 1 ->
        {:error, error, inject(adapter, point, {:times, n - 1, error})}

      fun when is_function(fun, 1) ->
        case fun.(adapter) do
          :ok -> {:ok, adapter}
          {:error, %ElixirDB.Error{} = error} -> {:error, error, adapter}
        end
    end
  end

  @doc """
  Runs `fun` against the inner handle unless `point` injects a failure first.
  """
  @spec call(t(), atom(), (term() -> result)) :: {result | {:error, ElixirDB.Error.t()}, t()}
        when result: term()
  def call(%__MODULE__{} = adapter, point, fun) when is_atom(point) and is_function(fun, 1) do
    case maybe_fail(adapter, point) do
      {:ok, adapter} -> {fun.(adapter.inner), adapter}
      {:error, error, adapter} -> {{:error, error}, adapter}
    end
  end
end
