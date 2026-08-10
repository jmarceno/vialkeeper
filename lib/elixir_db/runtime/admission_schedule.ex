defmodule ElixirDB.Runtime.AdmissionSchedule do
  @moduledoc false

  alias ElixirDB.Runtime.{AdmissionPolicy, ServiceClass}

  @type t :: [ServiceClass.t()]

  @spec build(AdmissionPolicy.t()) :: t()
  def build(%AdmissionPolicy{} = policy) do
    weights = AdmissionPolicy.weights(policy)

    Enum.flat_map(ServiceClass.classes(), fn class ->
      List.duplicate(class, Map.fetch!(weights, class))
    end)
  end
end
