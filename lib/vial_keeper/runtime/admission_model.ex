defmodule VialKeeper.Runtime.AdmissionModel do
  @moduledoc "Pure reference model for class-aware bounded admission scheduling."

  alias VialKeeper.Runtime.{
    AdmissionCapacity,
    AdmissionPolicy,
    AdmissionSchedule,
    ServiceClass
  }

  @enforce_keys [:limit, :policy, :schedule, :cursor, :queues, :active]
  defstruct @enforce_keys

  @type request_id :: term()
  @type queues :: %{ServiceClass.t() => [request_id()]}
  @type active :: %{class: ServiceClass.t(), request_id: request_id()} | nil

  @type t :: %__MODULE__{
          limit: pos_integer(),
          policy: AdmissionPolicy.t(),
          schedule: AdmissionSchedule.t(),
          cursor: non_neg_integer(),
          queues: queues(),
          active: active()
        }

  @spec new(pos_integer(), AdmissionPolicy.t()) :: t()
  def new(limit, %AdmissionPolicy{} = policy) when is_integer(limit) and limit > 0 do
    %__MODULE__{
      limit: limit,
      policy: policy,
      schedule: AdmissionSchedule.build(policy),
      cursor: 0,
      queues: empty_queues(),
      active: nil
    }
  end

  @spec enqueue(t(), ServiceClass.t(), request_id()) ::
          {:ok, t()} | {:error, :database_overloaded}
  def enqueue(%__MODULE__{} = state, class, request_id) do
    unless class in ServiceClass.classes(),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    if accepts?(state, class) do
      {:ok, %{state | queues: enqueue_queue(state.queues, class, request_id)}}
    else
      {:error, :database_overloaded}
    end
  end

  @spec release(t()) :: t()
  def release(%__MODULE__{active: nil} = state), do: state

  def release(%__MODULE__{} = state) do
    state
    |> Map.put(:active, nil)
    |> grant_next()
  end

  @spec grant_next(t()) :: t()
  def grant_next(%__MODULE__{active: active} = state) when active != nil, do: state

  def grant_next(%__MODULE__{} = state) do
    schedule_len = length(state.schedule)

    if schedule_len == 0 or all_queues_empty?(state.queues) do
      state
    else
      scan_schedule_for_grant(state, schedule_len)
    end
  end

  defp scan_schedule_for_grant(state, schedule_len) do
    0..(schedule_len - 1)
    |> Enum.reduce_while(state, fn offset, current ->
      try_grant_at_offset(offset, current, schedule_len)
    end)
  end

  defp try_grant_at_offset(offset, current, schedule_len) do
    index = rem(current.cursor + offset, schedule_len)
    class = Enum.at(current.schedule, index)

    case dequeue(current.queues, class) do
      {nil, _} ->
        {:cont, current}

      {request_id, queues} ->
        {:halt, grant_at(current, class, request_id, queues, index, schedule_len)}
    end
  end

  defp grant_at(current, class, request_id, queues, index, schedule_len) do
    current
    |> Map.put(:queues, queues)
    |> Map.put(:active, %{class: class, request_id: request_id})
    |> Map.put(:cursor, rem(index + 1, schedule_len))
  end

  @spec queued_counts(t()) :: %{ServiceClass.t() => non_neg_integer()}
  def queued_counts(%__MODULE__{queues: queues}) do
    Map.new(queues, fn {class, queue} -> {class, length(queue)} end)
  end

  @spec total_occupancy(t()) :: non_neg_integer()
  def total_occupancy(%__MODULE__{} = state) do
    state
    |> occupancy()
    |> Enum.reduce(0, fn {_class, count}, acc -> acc + count end)
  end

  @spec occupancy(t()) :: AdmissionCapacity.occupancy()
  def occupancy(%__MODULE__{} = state) do
    active_class =
      case state.active do
        %{class: class} -> class
        nil -> nil
      end

    AdmissionCapacity.occupancy_from_counts(queued_counts(state), active_class)
  end

  @spec accepts?(t(), ServiceClass.t()) :: boolean()
  def accepts?(%__MODULE__{} = state, class) do
    AdmissionCapacity.accepts?(
      state.limit,
      AdmissionPolicy.reserved_slots(state.policy),
      occupancy(state),
      class
    )
  end

  defp empty_queues do
    Map.new(ServiceClass.classes(), fn class -> {class, []} end)
  end

  defp enqueue_queue(queues, class, request_id) do
    Map.update!(queues, class, fn queue -> queue ++ [request_id] end)
  end

  defp dequeue(queues, class) do
    case Map.fetch!(queues, class) do
      [] -> {nil, queues}
      [head | tail] -> {head, Map.put(queues, class, tail)}
    end
  end

  defp all_queues_empty?(queues) do
    Enum.all?(queues, fn {_class, queue} -> queue == [] end)
  end
end
