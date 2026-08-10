defmodule ElixirDB.Runtime.AdmissionCapacity do
  @moduledoc false

  alias ElixirDB.Runtime.ServiceClass

  @type occupancy :: %{ServiceClass.t() => non_neg_integer()}

  @spec accepts?(
          pos_integer(),
          %{ServiceClass.t() => non_neg_integer()},
          occupancy(),
          ServiceClass.t()
        ) :: boolean()
  def accepts?(admission_limit, reserved_slots, occupancy, class)
      when is_integer(admission_limit) and admission_limit > 0 do
    unless class in ServiceClass.classes(),
      do: raise(ArgumentError, "invalid service class #{inspect(class)}")

    unused_reservations_for_others =
      ServiceClass.classes()
      |> Enum.reject(&(&1 == class))
      |> Enum.reduce(0, fn other, acc ->
        acc + max(Map.fetch!(reserved_slots, other) - Map.fetch!(occupancy, other), 0)
      end)

    total_occupancy =
      Enum.reduce(occupancy, 0, fn {_class, count}, acc -> acc + count end)

    total_occupancy + 1 <= admission_limit - unused_reservations_for_others
  end

  @spec empty_occupancy() :: occupancy()
  def empty_occupancy do
    Map.new(ServiceClass.classes(), fn class -> {class, 0} end)
  end

  @spec occupancy_from_counts(
          %{ServiceClass.t() => non_neg_integer()},
          ServiceClass.t() | nil
        ) :: occupancy()
  def occupancy_from_counts(queued_counts, active_class) do
    ServiceClass.classes()
    |> Map.new(fn class ->
      active? = active_class == class
      {class, Map.fetch!(queued_counts, class) + if(active?, do: 1, else: 0)}
    end)
  end
end
