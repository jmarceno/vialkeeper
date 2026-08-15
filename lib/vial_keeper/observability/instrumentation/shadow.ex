defmodule VialKeeper.Observability.Instrumentation.Shadow do
  @moduledoc "Observability emitters for shadow reads and exact-route fallback."

  alias VialKeeper.Observability.{Meters, Tracer}

  @read_span "vial_keeper.shadow.read"
  @read_count :"vial_keeper.shadow.read.count"
  @fallback_span "vial_keeper.shadow.route.fallback"
  @fallback_count :"vial_keeper.shadow.route.fallback.count"

  @doc "Records a bounded shadow read span and the actual served outcome."
  @spec read(binary(), (-> term())) :: term()
  def read(source_uuid, fun) when is_binary(source_uuid) and is_function(fun, 0) do
    Tracer.with_span(@read_span, [db_uuid: source_uuid], fn ->
      result = fun.()
      Meters.add(@read_count, db_uuid: source_uuid, outcome: read_outcome(result))
      result
    end)
  end

  @doc "Records route retirement without exposing the failed request or endpoint."
  @spec fallback(binary()) :: :ok
  def fallback(source_uuid) when is_binary(source_uuid) do
    Tracer.with_span(@fallback_span, [db_uuid: source_uuid], fn ->
      Meters.add(@fallback_count, db_uuid: source_uuid, outcome: :fallback)
      :ok
    end)
  end

  defp read_outcome({:ok, _value, %{served_by: "shadow"}}), do: :shadow

  defp read_outcome({:ok, _value, %{served_by: served_by}}) when served_by in ["source", "primary"],
    do: :source

  defp read_outcome({:error, _error}), do: :error
  defp read_outcome(_), do: :invalid
end
