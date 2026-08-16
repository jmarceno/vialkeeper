defmodule VialKeeper.TestSupport.AdmissionScenario do
  @moduledoc "Shared end-to-end scenario helpers for admission behavior tests."
  import ExUnit.Assertions

  alias VialKeeper.Eventual
  alias VialKeeper.Query.Subscriptions

  alias VialKeeper.Runtime.{
    AdmissionModel,
    AdmissionPolicy,
    AdmissionSchedule,
    DatabaseAdmission,
    DatabaseCatalog,
    ServiceClass
  }

  alias VialKeeper.TestSupport.AdmissionClassProbe

  @default_keyword AdmissionPolicy.default_keyword()

  @spec install_probe() :: reference()
  def install_probe do
    AdmissionClassProbe.uninstall()
    AdmissionClassProbe.install()
  end

  @spec install_test_hook() :: reference()
  def install_test_hook do
    ref = make_ref()
    Application.put_env(:vial_keeper, :admission_test_hook, {self(), ref})
    ref
  end

  @spec uninstall_test_hook() :: :ok
  def uninstall_test_hook do
    Application.delete_env(:vial_keeper, :admission_test_hook)
    :ok
  end

  @spec uninstall_probe() :: :ok
  def uninstall_probe, do: AdmissionClassProbe.uninstall()

  @spec drain_grants(reference(), timeout()) :: [{atom(), term() | nil}]
  def drain_grants(ref, timeout \\ 100), do: AdmissionClassProbe.drain(ref, timeout)

  @spec begin_peak_occupancy_tracking(binary()) :: :ok
  def begin_peak_occupancy_tracking(uuid) when is_binary(uuid) do
    :ok = DatabaseAdmission.reset_peak_occupancy(uuid)
    :ok
  end

  @spec peak_occupancy(binary()) :: non_neg_integer()
  def peak_occupancy(uuid) when is_binary(uuid) do
    {:ok, %{peak_occupancy: peak}} = DatabaseAdmission.stats(uuid)
    peak
  end

  @spec assert_max_occupancy!(non_neg_integer(), pos_integer()) :: :ok
  def assert_max_occupancy!(peak_seen, limit) do
    assert peak_seen <= limit,
           "admission occupancy peaked at #{peak_seen}, above limit #{limit}"

    :ok
  end

  @spec await_stats(binary(), (map() -> boolean()), keyword()) :: map()
  def await_stats(uuid, predicate, opts \\ []) do
    Eventual.eventually(
      fn -> poll_stats(uuid, predicate) end,
      Keyword.merge([timeout: 5_000, message: "admission stats predicate not satisfied"], opts)
    )
  end

  defp poll_stats(uuid, predicate) do
    case DatabaseAdmission.stats(uuid) do
      {:ok, stats} -> if predicate.(stats), do: stats, else: false
      _ -> false
    end
  end

  defp fetch_stats!(uuid) do
    {:ok, stats} = DatabaseAdmission.stats(uuid)
    stats
  end

  @spec policy_for_limit(pos_integer()) :: AdmissionPolicy.t()
  def policy_for_limit(limit) when is_integer(limit) and limit > 0 do
    {:ok, policy} = AdmissionPolicy.from_keyword(@default_keyword, limit)
    policy
  end

  @spec schedule_for_limit(pos_integer()) :: [atom()]
  def schedule_for_limit(limit) when is_integer(limit) and limit > 0 do
    limit |> policy_for_limit() |> AdmissionSchedule.build()
  end

  @spec expected_grant_classes(pos_integer(), pos_integer()) :: [atom()]
  def expected_grant_classes(limit, count) when count > 0 do
    policy = policy_for_limit(limit)

    model =
      Enum.reduce(ServiceClass.classes(), AdmissionModel.new(limit, policy), fn class, acc ->
        {:ok, acc} = AdmissionModel.enqueue(acc, class, class)
        acc
      end)

    simulate_class_grants(model, count, fn class, _id, model ->
      AdmissionModel.enqueue(model, class, class)
    end)
  end

  @spec expected_fairness_grant_classes(pos_integer(), pos_integer(), pos_integer()) :: [atom()]
  def expected_fairness_grant_classes(limit, count, backlog_per_class)
      when count > 0 and backlog_per_class > 0 do
    policy = policy_for_limit(limit)

    {:ok, model} = AdmissionModel.enqueue(AdmissionModel.new(limit, policy), :foreground, :blocker)
    model = %{AdmissionModel.grant_next(model) | active: nil}

    model =
      Enum.reduce(ServiceClass.classes(), model, fn class, acc ->
        Enum.reduce(1..backlog_per_class, acc, fn index, inner ->
          {:ok, inner} = AdmissionModel.enqueue(inner, class, {class, index})
          inner
        end)
      end)

    simulate_class_grants(model, count, fn class, request_id, next_model ->
      case request_id do
        {^class, index} ->
          next_index = rem(index, backlog_per_class) + 1
          AdmissionModel.enqueue(next_model, class, {class, next_index})

        other ->
          raise ArgumentError, "unexpected fairness request id #{inspect(other)}"
      end
    end)
  end

  @spec assert_sustained_grant_sequence!(
          [{atom(), term() | nil}],
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: :ok
  def assert_sustained_grant_sequence!(grants, limit, sample_count, backlog_per_class) do
    actual = Enum.take(Enum.map(grants, fn {class, _op} -> class end), sample_count)
    expected = expected_fairness_grant_classes(limit, sample_count, backlog_per_class)

    assert actual == expected,
           "grant sequence diverged from weighted schedule after foreground block; expected #{inspect(expected)}, got #{inspect(actual)}"

    :ok
  end

  @spec assert_fairness_grant_sequence!([{atom(), term() | nil}], pos_integer(), pos_integer()) ::
          :ok
  def assert_fairness_grant_sequence!(grants, limit, sample_count) do
    backlog_per_class = min(2, div(limit - 1, length(ServiceClass.classes())))
    assert_sustained_grant_sequence!(grants, limit, sample_count, backlog_per_class)
  end

  @spec expected_sustained_grant_classes(pos_integer(), pos_integer(), pos_integer()) :: [atom()]
  def expected_sustained_grant_classes(limit, count, backlog_per_class)
      when count > 0 and backlog_per_class > 0 do
    policy = policy_for_limit(limit)

    model =
      Enum.reduce(ServiceClass.classes(), AdmissionModel.new(limit, policy), fn class, acc ->
        Enum.reduce(1..backlog_per_class, acc, fn index, inner ->
          {:ok, inner} = AdmissionModel.enqueue(inner, class, {class, index})
          inner
        end)
      end)

    simulate_class_grants(model, count, fn class, request_id, next_model ->
      case request_id do
        {^class, index} ->
          next_index = rem(index, backlog_per_class) + 1
          AdmissionModel.enqueue(next_model, class, {class, next_index})

        other ->
          raise ArgumentError, "unexpected sustained request id #{inspect(other)}"
      end
    end)
  end

  defp simulate_class_grants(model, count, reenqueue_fun) do
    {classes, _} =
      Enum.reduce(1..count, {[], model}, fn _, {acc, current} ->
        case pick_grant(current) do
          {:grant, class, request_id, next} ->
            {:ok, requeued} = reenqueue_fun.(class, request_id, next)
            {[class | acc], requeued}

          :done ->
            {acc, current}
        end
      end)

    Enum.reverse(classes)
  end

  defp pick_grant(%{active: nil} = model) do
    queued_total =
      Enum.reduce(AdmissionModel.queued_counts(model), 0, fn {_class, count}, acc ->
        acc + count
      end)

    if queued_total == 0 do
      :done
    else
      granted = AdmissionModel.grant_next(model)

      case granted.active do
        %{class: class, request_id: request_id} ->
          {:grant, class, request_id, %{granted | active: nil}}

        nil ->
          :done
      end
    end
  end

  defp pick_grant(%{active: %{class: class, request_id: request_id}} = model) do
    {:grant, class, request_id, %{model | active: nil}}
  end

  @spec assert_two_cycle_frequencies!([{atom(), term() | nil}], pos_integer()) :: :ok
  def assert_two_cycle_frequencies!(grants, limit) do
    schedule = schedule_for_limit(limit)
    cycles = 2
    schedule_len = length(schedule)
    sample_size = schedule_len * cycles
    sample = Enum.take(grants, sample_size)
    sample_len = length(sample)

    assert sample_len == sample_size,
           "expected #{sample_size} grants for two schedule cycles, got #{sample_len}"

    counts = Enum.frequencies(Enum.map(sample, fn {class, _} -> class end))
    expected = schedule_counts(schedule)

    expected_two_cycles =
      Map.new(expected, fn {class, weight} -> {class, weight * cycles} end)

    assert counts == expected_two_cycles,
           "grant frequencies #{inspect(counts)} do not match two weighted schedule cycles #{inspect(expected_two_cycles)}"

    :ok
  end

  @spec assert_grant_sequence_matches_schedule!(
          [{atom(), term() | nil}],
          pos_integer(),
          pos_integer()
        ) :: :ok
  def assert_grant_sequence_matches_schedule!(grants, limit, sample_count) do
    actual = Enum.take(Enum.map(grants, fn {class, _op} -> class end), sample_count)
    expected = expected_grant_classes(limit, sample_count)

    assert actual == expected,
           "grant sequence diverged from weighted schedule; expected #{inspect(expected)}, got #{inspect(actual)}"

    :ok
  end

  @spec assert_foreground_dominant_share!([{atom(), term() | nil}], pos_integer(), keyword()) ::
          :ok
  def assert_foreground_dominant_share!(grants, limit, opts \\ []) do
    exact_counts? = Keyword.get(opts, :exact_counts, true)

    window =
      Keyword.get(opts, :window, min(length(grants), length(schedule_for_limit(limit))))

    sample = Enum.take(grants, window)
    refute sample == []

    counts =
      Enum.frequencies(Enum.map(sample, fn {class, _} -> class end))

    fg = Map.get(counts, :foreground, 0)
    sub = Map.get(counts, :subscription, 0)
    rep = Map.get(counts, :replication, 0)
    mnt = Map.get(counts, :maintenance, 0)

    assert fg >= sub and fg >= rep and fg >= mnt,
           "foreground did not dominate grant share in #{inspect(counts)}"

    if exact_counts? and length(sample) >= length(schedule_for_limit(limit)) do
      assert counts == schedule_counts(schedule_for_limit(limit)),
             "per-class grant counts #{inspect(counts)} do not match schedule weights #{inspect(schedule_counts(schedule_for_limit(limit)))}"
    end

    :ok
  end

  @spec assert_no_starvation!([{atom(), term() | nil}], pos_integer()) :: :ok
  def assert_no_starvation!(grants, limit) do
    schedule_len = length(schedule_for_limit(limit))
    window = Enum.take(grants, schedule_len)
    classes = MapSet.new(Enum.map(window, fn {class, _} -> class end))

    for required <- [:subscription, :replication, :maintenance] do
      assert MapSet.member?(classes, required),
             "class #{inspect(required)} did not receive a grant within one schedule cycle; window #{inspect(window)}"
    end

    :ok
  end

  @spec assert_class_fifo!([{atom(), term() | nil}], atom(), [term()]) :: :ok
  def assert_class_fifo!(grants, class, expected_ops) do
    actual_ops =
      grants
      |> Enum.filter(fn {grant_class, _op} -> grant_class == class end)
      |> Enum.map(fn {_class, op} -> op end)
      |> Enum.take(length(expected_ops))

    assert actual_ops == expected_ops,
           "FIFO violated for #{inspect(class)}: expected ops #{inspect(expected_ops)}, got #{inspect(actual_ops)}"

    :ok
  end

  @spec assert_all_classes_fifo!([{atom(), term() | nil}], pos_integer()) :: :ok
  def assert_all_classes_fifo!(grants, backlog_per_class) do
    synthetic_grants = Enum.filter(grants, fn {_class, op} -> synthetic_probe_op?(op) end)

    for class <- ServiceClass.classes() do
      expected_ops = for index <- 1..backlog_per_class, do: {class, index}
      assert_class_fifo!(synthetic_grants, class, expected_ops)
    end

    :ok
  end

  @spec assert_sustained_fairness!(binary(), reference(), pos_integer(), keyword()) :: :ok
  def assert_sustained_fairness!(uuid, probe_ref, limit, opts \\ []) do
    backlog_per_class = fairness_backlog(limit, opts)
    grant_target = length(schedule_for_limit(limit)) * 2
    job_id = Keyword.get(opts, :job_id)
    hook_ref = Keyword.fetch!(opts, :hook_ref)

    close_subscription_pids(Keyword.get(opts, :subscription_pids, []))
    {:ok, counter} = start_fairness_counter()
    spawn_request = fairness_spawn_fun(uuid, counter, backlog_per_class)

    _ = drain_grants(probe_ref, 0)
    flush_hook_mailbox!(hook_ref)
    maybe_suspend_replication(uuid, job_id)

    try do
      run_fairness_measurement!(
        uuid,
        probe_ref,
        hook_ref,
        limit,
        backlog_per_class,
        grant_target,
        spawn_request
      )
    after
      maybe_resume_replication(uuid, job_id)
      Agent.stop(counter)
    end

    :ok
  end

  @doc """
  Continuously backlog all four classes through real admission entrypoints
  (catalog/LocalEndpoint/RetentionScheduler-style maintenance execute) and prove
  weighted share, no-starvation, and per-class FIFO by request_ref.
  """
  @spec assert_real_path_continuous_fairness!(binary(), reference(), pos_integer(), keyword()) ::
          :ok
  def assert_real_path_continuous_fairness!(uuid, probe_ref, limit, opts \\ []) do
    backlog_per_class = fairness_backlog(limit, opts)
    grant_target = length(schedule_for_limit(limit)) * 2
    hook_ref = Keyword.fetch!(opts, :hook_ref)

    _ = drain_grants(probe_ref, 0)
    flush_hook_mailbox!(hook_ref)

    with_suspended_replication_workers!(uuid, fn ->
      run_real_path_fairness_measurement!(
        uuid,
        probe_ref,
        hook_ref,
        limit,
        backlog_per_class,
        grant_target
      )
    end)

    :ok
  end

  defp run_real_path_fairness_measurement!(
         uuid,
         _probe_ref,
         hook_ref,
         limit,
         backlog_per_class,
         grant_target
       ) do
    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, gate_ref, uuid})

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :blocked end)
      end)

    assert_receive {^gate_ref, :before_begin, blocker_executor}, 2_000
    flush_hook_mailbox!(hook_ref)

    enqueue_order = %{foreground: [], subscription: [], replication: [], maintenance: []}

    enqueue_order =
      Enum.reduce(1..backlog_per_class, enqueue_order, fn _index, acc ->
        Enum.reduce(ServiceClass.classes(), acc, fn class, inner ->
          spawn_real_path_request!(uuid, class)
          assert_receive {^hook_ref, :enqueued, request_ref, ^class, _op, _caller}, 5_000
          Map.update!(inner, class, &Enum.concat(&1, [request_ref]))
        end)
      end)

    await_stats(uuid, &fairness_backlog_ready?(&1, backlog_per_class), timeout: 10_000)

    # Release only the blocker; keep the sync gate so later grants pause at before_begin
    # until we re-enqueue the granted class (continuous four-class backlog).
    send(blocker_executor, {:go, gate_ref})
    assert :blocked = Task.await(blocker, 10_000)

    {grants, final_order} =
      collect_real_path_grants!(uuid, hook_ref, gate_ref, grant_target, enqueue_order)

    Application.delete_env(:vial_keeper, :admitted_command_sync)

    class_grants = Enum.map(grants, fn {class, op, _ref} -> {class, op} end)

    refute class_grants == []
    assert_two_cycle_frequencies!(class_grants, limit)
    assert_no_starvation!(class_grants, limit)
    assert_real_path_fifo!(grants, final_order)

    assert_foreground_dominant_share!(class_grants, limit,
      exact_counts: false,
      window: grant_target
    )

    await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)
  end

  defp spawn_real_path_request!(uuid, :foreground) do
    {:ok, _} =
      Task.start(fn ->
        _ =
          DatabaseCatalog.command(
            uuid,
            {:command, :put,
             %{
               document_id: "fg-#{System.unique_integer([:positive])}",
               body: %{"n" => 1}
             }}
          )
      end)
  end

  defp spawn_real_path_request!(uuid, :subscription) do
    {:ok, _} =
      Task.start(fn ->
        _ =
          DatabaseCatalog.command_as(
            uuid,
            :subscription,
            {:command, :put,
             %{
               document_id: "sub-#{System.unique_integer([:positive])}",
               body: %{"n" => 1}
             }}
          )
      end)
  end

  defp spawn_real_path_request!(uuid, :replication) do
    {:ok, _} =
      Task.start(fn ->
        _ =
          DatabaseCatalog.command_as(
            uuid,
            :replication,
            {:command, :put,
             %{
               document_id: "repl-#{System.unique_integer([:positive])}",
               body: %{"n" => 1}
             }}
          )
      end)
  end

  defp spawn_real_path_request!(uuid, :maintenance) do
    # RetentionScheduler maintenance owner path (compact) and schedule-ms identity path.
    {:ok, _} =
      Task.start(fn ->
        _ =
          DatabaseCatalog.command_as(
            uuid,
            :maintenance,
            {:command, :compact_retention, %{trigger: :admission_e2e}}
          )
      end)
  end

  defp collect_real_path_grants!(uuid, hook_ref, gate_ref, grant_target, enqueue_order) do
    collect_real_path_grants_loop(uuid, hook_ref, gate_ref, grant_target, enqueue_order, [], 60_000)
  end

  defp collect_real_path_grants_loop(
         uuid,
         hook_ref,
         gate_ref,
         grant_target,
         enqueue_order,
         grants,
         remaining
       ) do
    cond do
      length(grants) >= grant_target ->
        {Enum.take(grants, grant_target), enqueue_order}

      remaining <= 0 ->
        flunk(
          "did not observe #{grant_target} real-path grants under four-class backlog; got #{length(grants)}"
        )

      true ->
        collect_real_path_grant(
          uuid,
          hook_ref,
          gate_ref,
          grant_target,
          enqueue_order,
          grants,
          remaining
        )
    end
  end

  defp collect_real_path_grant(
         uuid,
         hook_ref,
         gate_ref,
         grant_target,
         enqueue_order,
         grants,
         remaining
       ) do
    receive do
      {^hook_ref, :granted, request_ref, class, op} ->
        assert_receive {^gate_ref, :before_begin, executor_pid}, 5_000
        next_grants = Enum.concat(grants, [{class, op, request_ref}])

        enqueue_order =
          if length(next_grants) < grant_target do
            spawn_real_path_request!(uuid, class)
            assert_receive {^hook_ref, :enqueued, new_ref, ^class, _op, _caller}, 5_000
            Map.update!(enqueue_order, class, &Enum.concat(&1, [new_ref]))
          else
            enqueue_order
          end

        send(executor_pid, {:go, gate_ref})

        collect_real_path_grants_loop(
          uuid,
          hook_ref,
          gate_ref,
          grant_target,
          enqueue_order,
          next_grants,
          remaining
        )
    after
      50 ->
        collect_real_path_grants_loop(
          uuid,
          hook_ref,
          gate_ref,
          grant_target,
          enqueue_order,
          grants,
          remaining - 50
        )
    end
  end

  defp assert_real_path_fifo!(grants, enqueue_order) do
    for class <- ServiceClass.classes() do
      granted_refs =
        grants
        |> Enum.filter(fn {grant_class, _op, _ref} -> grant_class == class end)
        |> Enum.map(fn {_c, _op, ref} -> ref end)

      expected = Enum.take(Map.fetch!(enqueue_order, class), length(granted_refs))

      assert granted_refs == expected,
             "FIFO violated for real-path #{inspect(class)}: expected #{inspect(expected)}, got #{inspect(granted_refs)}"
    end

    :ok
  end

  defp fairness_backlog(limit, opts) do
    backlog =
      opts
      |> Keyword.get(:backlog_per_class, 2)
      |> min(div(limit - 1, length(ServiceClass.classes())))

    assert backlog >= 1,
           "admission limit #{limit} is too small for a four-class backlog"

    backlog
  end

  defp close_subscription_pids(pids) do
    Enum.each(pids, &Subscriptions.close/1)
  end

  defp start_fairness_counter do
    Agent.start_link(fn -> Map.new(ServiceClass.classes(), fn class -> {class, 0} end) end)
  end

  defp fairness_spawn_fun(uuid, counter, backlog_per_class) do
    fn class ->
      index = next_fairness_index(counter, class)
      probe_op = {class, rem(index - 1, backlog_per_class) + 1}
      {:ok, _pid} = Task.start(fn -> run_fairness_owner(uuid, class, probe_op) end)
    end
  end

  defp next_fairness_index(counter, class) do
    Agent.get_and_update(counter, fn counts ->
      next = Map.fetch!(counts, class) + 1
      {next, Map.put(counts, class, next)}
    end)
  end

  defp run_fairness_owner(uuid, class, probe_op) do
    _ = DatabaseAdmission.execute_owner(uuid, class, fn -> :ok end, :infinity, probe_op)
  end

  defp maybe_suspend_replication(_uuid, nil), do: :ok

  defp maybe_suspend_replication(uuid, job_id) do
    case replication_worker_pid(uuid, job_id) do
      pid when is_pid(pid) -> :sys.suspend(pid)
      _ -> :ok
    end
  end

  defp maybe_resume_replication(_uuid, nil), do: :ok

  defp maybe_resume_replication(uuid, job_id) do
    case replication_worker_pid(uuid, job_id) do
      pid when is_pid(pid) -> :sys.resume(pid)
      _ -> :ok
    end
  end

  defp run_fairness_measurement!(
         uuid,
         probe_ref,
         hook_ref,
         limit,
         backlog_per_class,
         grant_target,
         spawn_request
       ) do
    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, gate_ref, uuid})

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :blocked end)
      end)

    assert_receive {^gate_ref, :before_begin, blocker_executor}, 2_000
    flush_hook_mailbox!(hook_ref)
    _ = drain_grants(probe_ref, 0)

    # Enqueue deterministically per class so FIFO probe ops match grant order.
    for class <- ServiceClass.classes(), index <- 1..backlog_per_class do
      spawn_request.(class)
      assert_receive {^hook_ref, :enqueued, _ref, ^class, {^class, ^index}, _caller}, 5_000
    end

    await_stats(uuid, &fairness_backlog_ready?(&1, backlog_per_class), timeout: 10_000)

    send(blocker_executor, {:go, gate_ref})
    assert :blocked = Task.await(blocker, 10_000)

    grants =
      collect_immediate_grants!(probe_ref, hook_ref, gate_ref, spawn_request, grant_target)

    Application.delete_env(:vial_keeper, :admitted_command_sync)

    refute grants == []
    assert_two_cycle_frequencies!(grants, limit)
    assert_no_starvation!(grants, limit)
    assert_all_classes_fifo!(grants, backlog_per_class)
    assert_foreground_dominant_share!(grants, limit, exact_counts: false, window: grant_target)
    await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)
  end

  defp flush_hook_mailbox!(hook_ref) do
    receive do
      {^hook_ref, _, _, _, _} -> flush_hook_mailbox!(hook_ref)
      {^hook_ref, _, _, _, _, _} -> flush_hook_mailbox!(hook_ref)
    after
      0 -> :ok
    end
  end

  defp fairness_backlog_ready?(stats, backlog_per_class) do
    stats.queued_foreground >= backlog_per_class and
      stats.queued_subscription >= backlog_per_class and
      stats.queued_replication >= backlog_per_class and
      stats.queued_maintenance >= backlog_per_class
  end

  defp collect_immediate_grants!(probe_ref, hook_ref, gate_ref, spawn_request, grant_target) do
    collect_immediate_grants_loop(
      probe_ref,
      hook_ref,
      gate_ref,
      spawn_request,
      grant_target,
      [],
      60_000
    )
  end

  defp collect_immediate_grants_loop(
         probe_ref,
         hook_ref,
         gate_ref,
         spawn_request,
         grant_target,
         grants,
         remaining
       ) do
    cond do
      length(grants) >= grant_target ->
        Enum.take(grants, grant_target)

      remaining <= 0 ->
        flunk(
          "did not observe #{grant_target} synthetic grants under four-class backlog; got #{length(grants)}"
        )

      true ->
        collect_immediate_grant(
          probe_ref,
          hook_ref,
          gate_ref,
          spawn_request,
          grant_target,
          grants,
          remaining
        )
    end
  end

  defp collect_immediate_grant(
         probe_ref,
         hook_ref,
         gate_ref,
         spawn_request,
         grant_target,
         grants,
         remaining
       ) do
    receive do
      {^probe_ref, :admission_grant, class, op} ->
        grants =
          if synthetic_probe_op?(op) do
            assert_receive {^gate_ref, :before_begin, executor_pid}, 5_000
            next = Enum.concat(grants, [{class, op}])

            if length(next) < grant_target do
              spawn_request.(class)
              assert_receive {^hook_ref, :enqueued, _ref, ^class, _op, _caller}, 5_000
            end

            send(executor_pid, {:go, gate_ref})
            next
          else
            grants
          end

        collect_immediate_grants_loop(
          probe_ref,
          hook_ref,
          gate_ref,
          spawn_request,
          grant_target,
          grants,
          remaining
        )
    after
      50 ->
        collect_immediate_grants_loop(
          probe_ref,
          hook_ref,
          gate_ref,
          spawn_request,
          grant_target,
          grants,
          remaining - 50
        )
    end
  end

  defp replication_worker_pid(uuid, job_id) do
    case :ets.lookup(:vial_keeper_replication_jobs, job_id) do
      [{^job_id, _state, pid, ^uuid, _replication_id, _details}] when is_pid(pid) -> pid
      [{^job_id, _state, pid, ^uuid, _replication_id}] when is_pid(pid) -> pid
      _ -> nil
    end
  end

  defp synthetic_probe_op?({class, index}) when is_atom(class) and is_integer(index), do: true
  defp synthetic_probe_op?(_), do: false

  @spec assert_reservation_pressure!(binary(), pos_integer()) :: :ok
  def assert_reservation_pressure!(uuid, limit) do
    with_suspended_replication_workers!(uuid, fn ->
      do_assert_reservation_pressure!(uuid, limit)
    end)
  end

  defp do_assert_reservation_pressure!(uuid, limit) do
    await_stats(uuid, &(&1.total_occupancy == 0 and &1.active_class == nil), timeout: 15_000)

    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, gate_ref, uuid})

    # With one reserved slot per class, foreground may use all unreserved capacity.
    # Keep one free reserved slot each for subscription/replication/maintenance.
    reserved_for_others = 3
    flood_count = max(limit - 1 - reserved_for_others, 1)

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :blocked end)
      end)

    assert_receive {^gate_ref, :before_begin, blocker_executor}, 2_000

    flood_tasks = spawn_foreground_floods(uuid, flood_count)

    await_stats(
      uuid,
      fn stats ->
        stats.active_class == :foreground and stats.queued_foreground >= flood_count
      end,
      timeout: 10_000
    )

    assert {:error, %VialKeeper.Error{code: :database_overloaded}} =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :rejected end)

    sub_task =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :subscription, fn -> :reserved_sub end)
      end)

    rep_task =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :replication, fn -> :reserved_rep end)
      end)

    mnt_task =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :maintenance, fn -> :reserved_mnt end)
      end)

    await_stats(
      uuid,
      fn stats ->
        stats.queued_subscription >= 1 and stats.queued_replication >= 1 and
          stats.queued_maintenance >= 1
      end,
      timeout: 10_000
    )

    send(blocker_executor, {:go, gate_ref})
    Application.delete_env(:vial_keeper, :admitted_command_sync)

    for task <- [blocker | flood_tasks] ++ [sub_task, rep_task, mnt_task] do
      assert Task.await(task, 10_000)
    end

    await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)
    :ok
  end

  defp suspend_replication_workers!(uuid) do
    for pid <- replication_worker_pids(uuid), Process.alive?(pid) do
      :sys.suspend(pid)
    end
  end

  defp resume_replication_workers!(uuid) do
    for pid <- replication_worker_pids(uuid), Process.alive?(pid) do
      :sys.resume(pid)
    end
  end

  defp replication_worker_pids(uuid) do
    :vial_keeper_replication_jobs
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {_job_id, _state, pid, ^uuid, _replication_id, _details} when is_pid(pid) -> [pid]
      {_job_id, _state, pid, ^uuid, _replication_id} when is_pid(pid) -> [pid]
      _ -> []
    end)
  end

  @spec with_suspended_replication_workers!(binary(), (-> term())) :: term()
  def with_suspended_replication_workers!(uuid, fun) when is_function(fun, 0) do
    suspend_replication_workers!(uuid)

    try do
      fun.()
    after
      resume_replication_workers!(uuid)
    end
  end

  @spec assert_killed_queued_never_granted!(binary(), reference(), reference()) :: :ok
  def assert_killed_queued_never_granted!(uuid, probe_ref, hook_ref) do
    with_suspended_replication_workers!(uuid, fn ->
      do_assert_killed_queued_never_granted!(uuid, probe_ref, hook_ref)
    end)
  end

  defp do_assert_killed_queued_never_granted!(uuid, probe_ref, hook_ref) do
    await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)
    _ = drain_grants(probe_ref, 0)
    flush_hook_mailbox!(hook_ref)

    {blocker, gate_ref, executor_pid} = hold_admission_slot!(uuid)
    {fg_ref, repl_ref, victims} = enqueue_kill_victims!(uuid, hook_ref)

    await_stats(
      uuid,
      &(&1.queued_foreground >= 1 and &1.queued_replication >= 1),
      timeout: 5_000
    )

    refute_hook_grants!(hook_ref, [fg_ref, repl_ref], 0)
    kill_queued_victims!(uuid, victims)
    refute_hook_grants!(hook_ref, [fg_ref, repl_ref], 0)

    send(executor_pid, {:go, gate_ref})
    Application.delete_env(:vial_keeper, :admitted_command_sync)
    assert :hold = Task.await(blocker, 5_000)

    refute_hook_grants!(hook_ref, [fg_ref, repl_ref], 200)
    refute_victim_probe_grants!(probe_ref)
    assert_after_kill_successor!(uuid)
    :ok
  end

  defp hold_admission_slot!(uuid) do
    parent = self()
    gate_ref = make_ref()
    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, gate_ref, uuid})

    blocker =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :hold end)
      end)

    assert_receive {^gate_ref, :before_begin, executor_pid}, 2_000
    {blocker, gate_ref, executor_pid}
  end

  defp enqueue_kill_victims!(uuid, hook_ref) do
    foreground_victim =
      spawn(fn ->
        try do
          DatabaseAdmission.execute_owner(
            uuid,
            :foreground,
            fn -> :never end,
            :infinity,
            {:victim, :foreground}
          )
        catch
          :exit, _ -> :ok
        end
      end)

    assert_receive {^hook_ref, :enqueued, fg_ref, :foreground, {:victim, :foreground},
                    ^foreground_victim},
                   5_000

    # Real replication catalog path (command_as :replication). Identity is a
    # classified read and no longer occupies the writer scheduler.
    replication_helper =
      spawn(fn ->
        try do
          _ =
            DatabaseCatalog.command_as(
              uuid,
              :replication,
              {:command, :put,
               %{
                 document_id: "repl-#{System.unique_integer([:positive])}",
                 body: %{"n" => 1}
               }}
            )
        catch
          :exit, _ -> :ok
        end
      end)

    assert_receive {^hook_ref, :enqueued, repl_ref, :replication, _op, ^replication_helper},
                   5_000

    {fg_ref, repl_ref, [foreground_victim, replication_helper]}
  end

  defp kill_queued_victims!(uuid, victims) do
    stats_before = fetch_stats!(uuid)

    for victim <- victims do
      Process.exit(victim, :kill)
    end

    for victim <- victims do
      mon = Process.monitor(victim)
      assert_receive {:DOWN, ^mon, :process, ^victim, _}, 2_000
    end

    Eventual.eventually(
      fn ->
        stats = fetch_stats!(uuid)

        stats.queued_foreground == stats_before.queued_foreground - 1 and
          stats.queued_replication == stats_before.queued_replication - 1
      end,
      timeout: 15_000,
      message: "killed queued victims were not removed from admission queues"
    )
  end

  defp refute_hook_grants!(hook_ref, refs, timeout) do
    for ref <- refs do
      refute_receive {^hook_ref, :granted, ^ref, _, _}, timeout
    end
  end

  defp refute_victim_probe_grants!(probe_ref) do
    grants = drain_grants(probe_ref, 200)

    refute Enum.any?(grants, fn {_class, op} -> op == {:victim, :foreground} end),
           "killed queued foreground victim received a grant: #{inspect(grants)}"

    refute Enum.any?(grants, fn {_class, op} -> op == :identity end),
           "killed queued replication helper received a grant: #{inspect(grants)}"
  end

  defp assert_after_kill_successor!(uuid) do
    successor =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(
          uuid,
          :foreground,
          fn -> :after_kill end,
          :infinity,
          :after_kill
        )
      end)

    assert :after_kill = Task.await(successor, 5_000)
  end

  @spec assert_timeout_race_clean!(binary(), reference()) :: :ok
  def assert_timeout_race_clean!(uuid, hook_ref) do
    with_suspended_replication_workers!(uuid, fn ->
      do_assert_timeout_race_clean!(uuid, hook_ref)
    end)
  end

  defp do_assert_timeout_race_clean!(uuid, hook_ref) do
    await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)
    flush_hook_mailbox!(hook_ref)

    parent = self()
    body_ref = make_ref()
    holder_gate = make_ref()
    racer_gate = make_ref()
    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, holder_gate, uuid})

    holder =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(uuid, :foreground, fn -> :held end)
      end)

    assert_receive {^holder_gate, :before_begin, holder_executor}, 5_000

    # Caller stays alive across a finite GenServer.call timeout that races the grant:
    # grant reaches before_begin, then the acquire call times out and cancel cleans up.
    racer =
      spawn(fn ->
        result =
          try do
            DatabaseAdmission.execute_owner(
              uuid,
              :foreground,
              fn ->
                send(parent, {body_ref, :executed})
                :raced
              end,
              100,
              :race_victim
            )
          catch
            :exit, reason -> {:exit, reason}
          end

        send(parent, {:race_result, result})
      end)

    assert_receive {^hook_ref, :enqueued, racer_ref, :foreground, :race_victim, ^racer}, 5_000
    await_stats(uuid, &(&1.queued_foreground >= 1), timeout: 5_000)

    :ok = Application.put_env(:vial_keeper, :admitted_command_sync, {parent, racer_gate, uuid})
    send(holder_executor, {:go, holder_gate})
    assert :held = Task.await(holder, 5_000)

    assert_receive {^hook_ref, :granted, ^racer_ref, :foreground, :race_victim}, 5_000
    assert_receive {^racer_gate, :before_begin, racer_executor}, 5_000

    assert_receive {:race_result, result}, 5_000
    refute_receive {^body_ref, :executed}, 0
    refute result == :raced

    await_stats(uuid, &(&1.total_occupancy == 0 and &1.queued_foreground == 0), timeout: 15_000)

    send(racer_executor, {:go, racer_gate})
    Application.delete_env(:vial_keeper, :admitted_command_sync)

    racer_mon = Process.monitor(racer)
    assert_receive {:DOWN, ^racer_mon, :process, ^racer, _}, 2_000

    successor =
      Task.async(fn ->
        DatabaseAdmission.execute_owner(
          uuid,
          :foreground,
          fn -> :after_timeout end,
          :infinity,
          :after_timeout
        )
      end)

    assert :after_timeout = Task.await(successor, 5_000)
    :ok
  end

  defp spawn_foreground_floods(uuid, flood_count) do
    for index <- 1..flood_count do
      Task.async(fn -> flood_foreground(uuid, index) end)
    end
  end

  defp flood_foreground(uuid, index) do
    DatabaseAdmission.execute_owner(uuid, :foreground, fn -> {:flood, index} end)
  end

  defp schedule_counts(schedule) do
    Map.new(ServiceClass.classes(), fn class ->
      {class, Enum.count(schedule, &(&1 == class))}
    end)
  end
end
