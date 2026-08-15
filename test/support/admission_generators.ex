defmodule VialKeeper.TestSupport.AdmissionGenerators do
  @moduledoc "StreamData generators for admission policy and scheduler properties."

  alias VialKeeper.Runtime.{AdmissionPolicy, ServiceClass}

  @reserved_keys [
    :foreground_reserved_slots,
    :subscription_reserved_slots,
    :replication_reserved_slots,
    :maintenance_reserved_slots
  ]

  @spec admission_limit() :: StreamData.t(pos_integer())
  def admission_limit do
    StreamData.integer(1..64)
  end

  @spec weights() :: StreamData.t(keyword())
  def weights do
    StreamData.fixed_map(%{
      foreground_weight: StreamData.integer(1..64),
      subscription_weight: StreamData.integer(1..64),
      replication_weight: StreamData.integer(1..64),
      maintenance_weight: StreamData.integer(1..64)
    })
    |> StreamData.map(&Map.to_list/1)
  end

  @spec reserved_slots(pos_integer()) :: StreamData.t(keyword())
  def reserved_slots(admission_limit) when is_integer(admission_limit) and admission_limit > 0 do
    StreamData.bind(StreamData.integer(0..admission_limit), fn total ->
      partition_sum(total)
      |> StreamData.map(fn slots -> Enum.zip(@reserved_keys, slots) end)
    end)
  end

  @spec valid_policy_keyword() :: StreamData.t({keyword(), pos_integer()})
  def valid_policy_keyword do
    StreamData.bind(admission_limit(), &valid_policy_keyword_for_limit/1)
  end

  defp valid_policy_keyword_for_limit(limit) do
    StreamData.map(
      StreamData.tuple({weights(), reserved_slots(limit)}),
      fn {weight_keyword, reserved_keyword} ->
        {Keyword.merge(weight_keyword, reserved_keyword), limit}
      end
    )
  end

  @spec valid_policy() :: StreamData.t({AdmissionPolicy.t(), pos_integer()})
  def valid_policy do
    StreamData.map(valid_policy_keyword(), fn {keyword, limit} ->
      {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
      {policy, limit}
    end)
  end

  @spec service_class() :: StreamData.t(ServiceClass.t())
  def service_class do
    StreamData.member_of(ServiceClass.classes())
  end

  @spec request_id() :: StreamData.t(term())
  def request_id do
    alphanumeric_atom = StreamData.atom(:alphanumeric)

    StreamData.one_of([
      StreamData.integer(),
      alphanumeric_atom,
      StreamData.tuple({alphanumeric_atom, StreamData.integer()})
    ])
  end

  @spec enqueue_sequence() :: StreamData.t([{ServiceClass.t(), term()}])
  def enqueue_sequence do
    StreamData.list_of(
      StreamData.tuple({service_class(), request_id()}),
      min_length: 0,
      max_length: 24
    )
  end

  @spec occupancy(pos_integer()) :: StreamData.t(map())
  def occupancy(limit) when is_integer(limit) and limit > 0 do
    StreamData.bind(StreamData.integer(0..limit), fn total ->
      partition_sum(total)
      |> StreamData.map(fn slots -> Map.new(Enum.zip(ServiceClass.classes(), slots)) end)
    end)
  end

  defp partition_sum(0), do: StreamData.constant([0, 0, 0, 0])

  defp partition_sum(total) do
    StreamData.bind(StreamData.integer(0..total), fn first ->
      StreamData.bind(StreamData.integer(0..(total - first)), fn second ->
        StreamData.bind(StreamData.integer(0..(total - first - second)), fn third ->
          StreamData.constant([first, second, third, total - first - second - third])
        end)
      end)
    end)
  end
end
