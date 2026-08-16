defmodule VialKeeper.Runtime.AdmissionModelTest do
  use ExUnitProperties
  use ExUnit.Case, async: true

  alias VialKeeper.Runtime.{
    AdmissionCapacity,
    AdmissionModel,
    AdmissionPolicy,
    AdmissionSchedule,
    ServiceClass
  }

  alias VialKeeper.TestSupport.AdmissionGenerators

  @default_keyword AdmissionPolicy.default_keyword()
  @default_limit 128
  @default_policy (fn ->
                     {:ok, policy} = AdmissionPolicy.from_keyword(@default_keyword, @default_limit)
                     policy
                   end).()
  @default_schedule AdmissionSchedule.build(@default_policy)

  defp model(limit \\ @default_limit, policy \\ @default_policy) do
    AdmissionModel.new(limit, policy)
  end

  defp policy_from_keyword(keyword, limit) do
    {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
    policy
  end

  defp enqueue_ok(model, class, id) do
    {:ok, model} = AdmissionModel.enqueue(model, class, id)
    model
  end

  defp release_only(%AdmissionModel{} = model) do
    %{model | active: nil}
  end

  defp grant(model) do
    model
    |> AdmissionModel.grant_next()
    |> then(fn granted ->
      assert granted.active != nil
      granted
    end)
  end

  defp release(model) do
    AdmissionModel.release(model)
  end

  defp schedule_counts(schedule) do
    Map.new(ServiceClass.classes(), fn class ->
      {class, Enum.count(schedule, &(&1 == class))}
    end)
  end

  describe "schedule generation" do
    test "default weights produce the 8:4:2:1 schedule vector" do
      assert Enum.count(@default_schedule) == 15

      assert Enum.count(@default_schedule, &(&1 == :foreground)) == 8
      assert Enum.count(@default_schedule, &(&1 == :subscription)) == 4
      assert Enum.count(@default_schedule, &(&1 == :replication)) == 2
      assert Enum.count(@default_schedule, &(&1 == :maintenance)) == 1

      assert @default_schedule ==
               Enum.concat([
                 List.duplicate(:foreground, 8),
                 List.duplicate(:subscription, 4),
                 List.duplicate(:replication, 2),
                 [:maintenance]
               ])
    end

    test "custom weights preserve ServiceClass ordering in the schedule" do
      keyword =
        Keyword.merge(@default_keyword,
          foreground_weight: 1,
          subscription_weight: 3,
          replication_weight: 2,
          maintenance_weight: 1
        )

      policy = policy_from_keyword(keyword, 16)
      schedule = AdmissionSchedule.build(policy)

      assert schedule ==
               Enum.concat([
                 [:foreground],
                 List.duplicate(:subscription, 3),
                 List.duplicate(:replication, 2),
                 [:maintenance]
               ])

      assert schedule_counts(schedule) == AdmissionPolicy.weights(policy)
    end

    property "schedule length and per-class counts match configured weights" do
      check all({keyword, limit} <- AdmissionGenerators.valid_policy_keyword()) do
        {:ok, policy} = AdmissionPolicy.from_keyword(keyword, limit)
        schedule = AdmissionSchedule.build(policy)
        weights = AdmissionPolicy.weights(policy)

        assert length(schedule) == Map.values(weights) |> Enum.sum()
        assert schedule_counts(schedule) == weights
      end
    end

    property "schedule preserves ServiceClass declaration order" do
      check all({policy, _limit} <- AdmissionGenerators.valid_policy()) do
        schedule = AdmissionSchedule.build(policy)
        weights = AdmissionPolicy.weights(policy)

        expected =
          Enum.flat_map(ServiceClass.classes(), fn class ->
            List.duplicate(class, Map.fetch!(weights, class))
          end)

        assert schedule == expected
      end
    end
  end

  describe "weighted grant order" do
    test "all classes continuously queued follow schedule weights over full cycles" do
      model =
        model()
        |> enqueue_ok(:foreground, :f)
        |> enqueue_ok(:subscription, :s)
        |> enqueue_ok(:replication, :r)
        |> enqueue_ok(:maintenance, :m)

      {grants, _model} =
        Enum.reduce(1..15, {[], model}, fn _, {grants, current} ->
          granted = grant(current)
          class = granted.active.class

          current =
            granted
            |> release_only()
            |> enqueue_ok(class, make_ref())

          {Enum.concat(grants, [class]), current}
        end)

      assert grants == @default_schedule
    end

    test "grant counts over multiple cycles match configured weights" do
      model = model()

      {counts, _} =
        Enum.reduce(1..30, {%{}, model}, fn _, {counts, current} ->
          current =
            Enum.reduce(ServiceClass.classes(), current, fn class, acc ->
              enqueue_ok(acc, class, make_ref())
            end)

          granted = grant(current)
          class = granted.active.class
          counts = Map.update(counts, class, 1, &(&1 + 1))

          current =
            granted
            |> release_only()
            |> enqueue_ok(class, make_ref())

          {counts, current}
        end)

      assert counts[:foreground] == 16
      assert counts[:subscription] == 8
      assert counts[:replication] == 4
      assert counts[:maintenance] == 2
    end
  end

  describe "FIFO within class" do
    test "requests grant in enqueue order per class" do
      ids = [:a, :b, :c]

      model =
        Enum.reduce(ids, model(), fn id, acc ->
          enqueue_ok(acc, :foreground, id)
        end)

      {granted_ids, _} =
        Enum.reduce(ids, {[], model}, fn _expected, {seen, current} ->
          granted = grant(current)
          seen = Enum.concat(seen, [granted.active.request_id])
          {seen, release(granted)}
        end)

      assert granted_ids == ids
    end

    for class <- ServiceClass.classes() do
      test "FIFO for #{class}" do
        class = unquote(class)
        ids = Enum.map(1..3, &{class, &1})

        model =
          Enum.reduce(ids, model(), fn id, acc ->
            enqueue_ok(acc, class, id)
          end)

        {granted_ids, _} =
          Enum.reduce(ids, {[], model}, fn _expected, {seen, current} ->
            granted = grant(current)
            seen = Enum.concat(seen, [granted.active.request_id])
            {seen, release(granted)}
          end)

        assert granted_ids == ids
      end
    end

    property "grants dequeue requests in enqueue order per class" do
      check all(
              {policy, limit} <- AdmissionGenerators.valid_policy(),
              class <- AdmissionGenerators.service_class(),
              ids <-
                StreamData.list_of(AdmissionGenerators.request_id(), min_length: 1, max_length: 6)
            ) do
        model =
          Enum.reduce(ids, model(limit, policy), fn id, acc ->
            case AdmissionModel.enqueue(acc, class, id) do
              {:ok, next} -> next
              {:error, :database_overloaded} -> acc
            end
          end)

        enqueued = Map.fetch!(model.queues, class)

        {granted_ids, _} =
          Enum.reduce(enqueued, {[], model}, fn _expected, {seen, current} ->
            granted = grant(current)
            assert granted.active.class == class
            {Enum.concat(seen, [granted.active.request_id]), release_only(granted)}
          end)

        assert granted_ids == enqueued
      end
    end
  end

  describe "empty classes" do
    test "foreground-only queue drains without stalling on empty schedule slots" do
      model =
        model()
        |> enqueue_ok(:foreground, 1)
        |> enqueue_ok(:foreground, 2)

      granted1 = grant(model)
      assert granted1.active.request_id == 1
      granted2 = grant(release(granted1))
      assert granted2.active.request_id == 2
      assert release(granted2).active == nil
    end

    test "maintenance-only queue drains normally" do
      model = enqueue_ok(model(), :maintenance, :only)

      granted = grant(model)
      assert granted.active.class == :maintenance
      assert granted.active.request_id == :only
      assert release(granted).active == nil
    end
  end

  describe "no starvation" do
    test "lower classes receive grants within one schedule cycle while foreground stays backlogged" do
      model =
        model(32)
        |> then(fn acc ->
          Enum.reduce(1..20, acc, fn n, inner ->
            enqueue_ok(inner, :foreground, {:f, n})
          end)
        end)
        |> enqueue_ok(:subscription, :s)
        |> enqueue_ok(:replication, :r)
        |> enqueue_ok(:maintenance, :m)

      schedule_len = length(@default_schedule)

      {lower_grants, final_model} =
        Enum.reduce(1..schedule_len, {MapSet.new(), model}, fn _, {seen, current} ->
          granted = grant(current)
          class = granted.active.class

          seen =
            if class in [:subscription, :replication, :maintenance],
              do: MapSet.put(seen, class),
              else: seen

          {seen, release(granted)}
        end)

      assert MapSet.equal?(lower_grants, MapSet.new([:subscription, :replication, :maintenance]))
      assert AdmissionModel.queued_counts(final_model)[:foreground] > 0
    end
  end

  describe "reserved capacity" do
    test "foreground flood cannot occupy every slot when each class reserves one of eight" do
      limit = 8
      model = AdmissionModel.new(limit, @default_policy)

      model =
        Enum.reduce(1..limit, model, fn n, acc ->
          case AdmissionModel.enqueue(acc, :foreground, n) do
            {:ok, next} -> next
            {:error, :database_overloaded} -> acc
          end
        end)

      assert {:ok, _} = AdmissionModel.enqueue(model, :subscription, :sub)
      assert {:ok, _} = AdmissionModel.enqueue(model, :replication, :rep)
      assert {:ok, _} = AdmissionModel.enqueue(model, :maintenance, :mnt)
    end

    test "additional capacity follows the ordinary total bound once reservations are occupied" do
      limit = 8
      model = AdmissionModel.new(limit, @default_policy)

      model =
        model
        |> enqueue_ok(:foreground, :f)
        |> enqueue_ok(:subscription, :s)
        |> enqueue_ok(:replication, :r)
        |> enqueue_ok(:maintenance, :m)

      assert AdmissionModel.total_occupancy(model) == 4

      model =
        Enum.reduce(1..4, model, fn n, acc ->
          enqueue_ok(acc, :foreground, {:extra, n})
        end)

      assert AdmissionModel.total_occupancy(model) == limit
      assert {:error, :database_overloaded} = AdmissionModel.enqueue(model, :foreground, :overflow)
    end

    test "accept? matches the reserved-capacity algorithm" do
      reserved = AdmissionPolicy.reserved_slots(@default_policy)
      occupancy = AdmissionCapacity.empty_occupancy()

      assert AdmissionCapacity.accepts?(8, reserved, occupancy, :foreground)
      refute AdmissionCapacity.accepts?(4, reserved, %{occupancy | foreground: 3}, :foreground)
    end

    test "active permit counts toward admission_limit occupancy" do
      limit = 4

      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 0,
          subscription_reserved_slots: 0,
          replication_reserved_slots: 0,
          maintenance_reserved_slots: 0
        )

      policy = policy_from_keyword(keyword, limit)

      model =
        model(limit, policy)
        |> enqueue_ok(:foreground, 1)
        |> enqueue_ok(:foreground, 2)
        |> enqueue_ok(:foreground, 3)
        |> enqueue_ok(:foreground, 4)
        |> grant()

      assert AdmissionModel.total_occupancy(model) == limit
      refute AdmissionModel.accepts?(model, :foreground)
      assert {:error, :database_overloaded} = AdmissionModel.enqueue(model, :foreground, 5)
    end

    test "acceptance and rejection at exact reservation boundaries" do
      limit = 4

      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 2,
          subscription_reserved_slots: 1,
          replication_reserved_slots: 1,
          maintenance_reserved_slots: 0
        )

      policy = policy_from_keyword(keyword, limit)
      model = model(limit, policy)

      model = enqueue_ok(model, :foreground, 1)
      model = enqueue_ok(model, :foreground, 2)
      assert {:error, :database_overloaded} = AdmissionModel.enqueue(model, :foreground, :overflow)

      model = enqueue_ok(model, :subscription, :s)
      model = enqueue_ok(model, :replication, :r)

      refute AdmissionModel.accepts?(model, :foreground)
      refute AdmissionModel.accepts?(model, :maintenance)
      assert AdmissionModel.total_occupancy(model) == limit
    end

    test "zero reservations allow occupancy up to the admission limit" do
      limit = 3

      keyword =
        Keyword.merge(@default_keyword,
          foreground_reserved_slots: 0,
          subscription_reserved_slots: 0,
          replication_reserved_slots: 0,
          maintenance_reserved_slots: 0
        )

      policy = policy_from_keyword(keyword, limit)
      model = model(limit, policy)

      model =
        Enum.reduce(1..limit, model, fn n, acc ->
          enqueue_ok(acc, :foreground, n)
        end)

      assert AdmissionModel.total_occupancy(model) == limit
      assert {:error, :database_overloaded} = AdmissionModel.enqueue(model, :foreground, :overflow)
    end

    property "accepts? matches independent reserved-capacity reference for generated occupancy" do
      check all(
              {policy, limit} <- AdmissionGenerators.valid_policy(),
              class <- AdmissionGenerators.service_class(),
              occupancy <- AdmissionGenerators.occupancy(limit)
            ) do
        reserved = AdmissionPolicy.reserved_slots(policy)

        model = %AdmissionModel{
          limit: limit,
          policy: policy,
          schedule: AdmissionSchedule.build(policy),
          cursor: 0,
          queues: empty_queues_from_occupancy(occupancy),
          active: nil
        }

        assert AdmissionModel.accepts?(model, class) ==
                 reference_accepts?(limit, reserved, occupancy, class)
      end
    end

    property "successful enqueues never exceed admission_limit occupancy" do
      check all(
              {policy, limit} <- AdmissionGenerators.valid_policy(),
              sequence <- AdmissionGenerators.enqueue_sequence()
            ) do
        model =
          Enum.reduce(sequence, model(limit, policy), fn {class, id}, acc ->
            case AdmissionModel.enqueue(acc, class, id) do
              {:ok, next} -> next
              {:error, :database_overloaded} -> acc
            end
          end)

        assert AdmissionModel.total_occupancy(model) <= limit
      end
    end
  end

  describe "cursor advancement" do
    property "cursor advances to the slot after each grant" do
      check all({policy, limit} <- AdmissionGenerators.valid_policy()) do
        schedule = AdmissionSchedule.build(policy)
        schedule_len = length(schedule)

        if schedule_len > 0 do
          class = hd(schedule)

          case AdmissionModel.enqueue(model(limit, policy), class, make_ref()) do
            {:ok, model} ->
              cursor_before = model.cursor
              granted = grant(model)
              index = rem(cursor_before, schedule_len)
              assert granted.cursor == rem(index + 1, schedule_len)

            {:error, :database_overloaded} ->
              :ok
          end
        end
      end
    end
  end

  describe "determinism" do
    test "identical enqueue/release sequences yield identical grant order" do
      sequence = [
        {:foreground, 1},
        {:subscription, 1},
        {:foreground, 2},
        {:replication, 1},
        {:maintenance, 1},
        {:subscription, 2}
      ]

      run = fn ->
        model =
          Enum.reduce(sequence, model(16), fn {class, id}, acc ->
            enqueue_ok(acc, class, id)
          end)

        {grants, _} =
          Enum.reduce(1..length(sequence), {[], model}, fn _, {grants, current} ->
            granted = grant(current)
            class = granted.active.class
            id = granted.active.request_id
            {Enum.concat(grants, [{class, id}]), release(granted)}
          end)

        grants
      end

      assert run.() == run.()
    end

    property "identical generated sequences yield identical grant order" do
      check all(
              {policy, limit} <- AdmissionGenerators.valid_policy(),
              sequence <- AdmissionGenerators.enqueue_sequence()
            ) do
        run = fn ->
          model =
            Enum.reduce(sequence, model(limit, policy), fn {class, id}, acc ->
              case AdmissionModel.enqueue(acc, class, id) do
                {:ok, next} -> next
                {:error, :database_overloaded} -> acc
              end
            end)

          grants_for(model, length(sequence))
        end

        assert run.() == run.()
      end
    end
  end

  defp grants_for(model, max_grants) when max_grants > 0 do
    Enum.reduce(1..max_grants, {[], model}, fn _, {grants, current} ->
      {more, next} = next_grant(current)
      {grants ++ more, next}
    end)
    |> elem(0)
  end

  defp grants_for(_model, 0), do: []

  defp next_grant(%{active: nil} = current) do
    if all_queues_empty?(current.queues) do
      {[], current}
    else
      granted = AdmissionModel.grant_next(current)

      if granted.active do
        entry = {granted.active.class, granted.active.request_id}
        {[entry], %{granted | active: nil}}
      else
        {[], granted}
      end
    end
  end

  defp next_grant(%{active: active} = current) do
    released = %{current | active: nil}
    entry = {active.class, active.request_id}
    {[entry], released}
  end

  defp all_queues_empty?(queues) do
    Enum.all?(queues, fn {_class, queue} -> queue == [] end)
  end

  defp empty_queues_from_occupancy(occupancy) do
    Map.new(ServiceClass.classes(), fn class ->
      {class, List.duplicate(:queued, Map.fetch!(occupancy, class))}
    end)
  end

  # Independent reference for reserved-capacity acceptance (not delegated to AdmissionCapacity).
  defp reference_accepts?(limit, reserved_slots, occupancy, class) do
    unused_reservations_for_others =
      ServiceClass.classes()
      |> Enum.reject(&(&1 == class))
      |> Enum.map(fn other ->
        reserved = Map.fetch!(reserved_slots, other)
        occupied = Map.fetch!(occupancy, other)
        max(reserved - occupied, 0)
      end)
      |> Enum.sum()

    total_occupancy = occupancy |> Map.values() |> Enum.sum()

    total_occupancy + 1 <= limit - unused_reservations_for_others
  end
end
