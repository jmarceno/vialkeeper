defmodule VialKeeper.Probe do
  @moduledoc """
  Always-available, low-overhead performance counters for hot code paths.

  Each probe in the closed vocabulary below owns a fixed slot range in one
  `:counters` array: the total native-time duration and a log-scale latency
  histogram (whose sum is the call count). Recording a probe is one
  `:persistent_term` read, two monotonic clock reads, and two lock-free
  counter increments; there is no process messaging, ETS, map lookup, or
  allocation beyond one small tuple on the hot path.

  ## Tiers

    * `:standard` — layer entry points, runtime process hops, storage and
      SQLite phases: a handful of probes per request. Enabled by default.
    * `:detail` — per-statement (`:connection` area) and per-value
      (`:codec` area) probes, which can fire dozens of times per request.
      Disabled by default; enable them while profiling.

  The enabled tiers come from `config :vial_keeper, :performance_probe_tiers`
  (default `[:standard]`) at boot and can be changed at run time with
  `enable/1` and `disable/1`. A probe in a disabled tier costs one
  `:persistent_term` read. Changing tiers rewrites a `:persistent_term`, which
  makes the runtime scan every process once: it is an operator action, not
  something to toggle per request.

  `config :vial_keeper, :performance_probes, false` removes every probe from
  the compiled code (`measure/2` expands to its body).

  Counters accumulate from boot (or the last `reset/0`). Callers that need a
  window take two `snapshot/0`s and `diff/2` them. Probes nest (for example
  `:sqlite_step` runs inside `:storage_get_document`), so durations are
  inclusive and area totals are not exclusive.

  A probe whose body raises is not recorded: the hot path has no `try`.

  The module depends on nothing else in VialKeeper, so every layer, including
  core JSON and storage code, can carry probes without a layering cycle.
  `VialKeeper.Observability.Dashboard` exposes the counters.
  """

  @compiled Application.compile_env(:vial_keeper, :performance_probes, true)
  @state_key {__MODULE__, :state}
  @tiers [:standard, :detail]

  # Histogram bucket upper bounds in nanoseconds; the last bucket is open.
  @bucket_bounds_ns [
    500,
    1_000,
    2_000,
    4_000,
    8_000,
    16_000,
    32_000,
    64_000,
    128_000,
    256_000,
    512_000,
    1_024_000,
    4_096_000,
    16_384_000,
    65_536_000
  ]
  @bucket_count length(@bucket_bounds_ns) + 1
  # Slot layout per probe: total duration, then one slot per bucket.
  @stride 1 + @bucket_count

  # Mirrors the closed vocabularies of Instrumentation.Mutation and
  # Instrumentation.SQLite (kept literal to avoid compile-time dependencies;
  # a test asserts they match).
  @mutation_phases [
    :validation,
    :canonical_encode,
    :strict_decode,
    :attachment_manifest,
    :catalog_route,
    :owner_queue,
    :transaction_begin,
    :fact_reads,
    :revision_hash,
    :revision_writes,
    :change_log,
    :attachment_metadata,
    :transaction_commit,
    :search_flush,
    :change_notifier
  ]
  @sqlite_phases [
    :document_lookup,
    :document_leaves,
    :revision_lookup,
    :bulk_prepare,
    :bulk_finalize,
    :changes_identity,
    :changes_fetch,
    :changes_decode,
    :changes_has_more,
    :query_prepare_request,
    :query_identity,
    :query_index_catalog,
    :query_plan,
    :query_candidates,
    :query_filter,
    :query_sort,
    :query_cursor,
    :query_project,
    :transaction_begin,
    :transaction_commit,
    :transaction_rollback
  ]

  @areas [
    http: [:http_request, :http_body_read, :http_response_encode],
    service: [:documents_get, :documents_bulk_write, :changes_read, :query_execute],
    runtime: [
      :catalog_route,
      :read_pool_execute,
      :admission_execute,
      :owner_command,
      :read_worker_job,
      :read_pool_complete
    ],
    storage: [
      :storage_get_document,
      :storage_bulk_mutation,
      :storage_read_changes,
      :storage_execute_query
    ],
    mutation: Enum.map(@mutation_phases, &:"mutation_#{&1}"),
    sqlite_phase: Enum.map(@sqlite_phases, &:"sqlite_#{&1}"),
    codec: [:term_encode, :term_decode, :json_canonical_encode, :json_strict_decode],
    connection: [:sqlite_step, :sqlite_exec]
  ]
  @detail_areas [:codec, :connection]

  @probes Enum.flat_map(@areas, fn {_area, probes} -> probes end)

  if length(Enum.uniq(@probes)) != length(@probes) do
    raise CompileError, description: "duplicate performance probe names"
  end

  @typedoc "A probe name from the closed vocabulary (see `probes/0`)."
  @type probe :: atom()

  @typedoc "A probe tier."
  @type tier :: :standard | :detail

  defmodule Stats do
    @moduledoc "Counters for one probe: call count, total nanoseconds, histogram buckets."
    @enforce_keys [:count, :total_ns, :buckets]
    defstruct [:count, :total_ns, :buckets]

    @type t :: %__MODULE__{
            count: non_neg_integer(),
            total_ns: non_neg_integer(),
            buckets: [non_neg_integer()]
          }
  end

  @typedoc "Counters for one probe; durations are nanoseconds."
  @type stats :: Stats.t()

  @typedoc "Counters for every probe that has been recorded at least once."
  @type snapshot :: %{probe() => stats()}

  @typedoc "Opaque value returned by `start/1` while a probe's tier is enabled."
  @type started :: {:counters.counters_ref(), integer()}

  @doc """
  Measures `body` under `probe`, which must be a literal probe name.

      Probe.measure :sqlite_step do
        step(conn, statement, [])
      end
  """
  if @compiled do
    defmacro measure(probe, do: body) when is_atom(probe) do
      index = index!(probe)
      tier = tier_of!(probe)

      quote do
        case unquote(__MODULE__).start(unquote(tier)) do
          nil ->
            unquote(body)

          started ->
            result = unquote(body)
            unquote(__MODULE__).stop(unquote(index), started)
            result
        end
      end
    end
  else
    defmacro measure(probe, do: body) when is_atom(probe) do
      _ = index!(probe)
      body
    end
  end

  @doc """
  Starts a measurement for a probe of `tier`; `nil` when the tier is disabled.
  Called by the code `measure/2` expands to.
  """
  @spec start(tier()) :: started() | nil
  if @compiled do
    def start(:standard) do
      case :persistent_term.get(@state_key, nil) do
        {counters, true, _detail} -> {counters, System.monotonic_time()}
        _ -> nil
      end
    end

    def start(:detail) do
      case :persistent_term.get(@state_key, nil) do
        {counters, _standard, true} -> {counters, System.monotonic_time()}
        _ -> nil
      end
    end
  else
    def start(_tier), do: nil
  end

  @doc "Finishes a measurement started by `start/1` for the probe at slot `index`."
  @spec stop(non_neg_integer(), started()) :: :ok
  def stop(index, {counters, started}),
    do: record(counters, index, System.monotonic_time() - started)

  @doc """
  Records the time since `started` (from `start/1`) under `probe`, for call
  sites whose probe name is computed at run time.
  """
  @spec record_since(probe(), started()) :: :ok
  def record_since(probe, {counters, started}),
    do: record(counters, index(probe), System.monotonic_time() - started)

  @doc """
  Records an already measured native-time `duration` under a `:standard`
  probe, for call sites that time the work anyway (phase instrumentation).
  """
  @spec add(probe(), integer()) :: :ok
  if @compiled do
    def add(probe, duration) do
      case :persistent_term.get(@state_key, nil) do
        {counters, true, _detail} -> record(counters, index(probe), duration)
        _ -> :ok
      end
    end
  else
    def add(_probe, _duration), do: :ok
  end

  @doc """
  Creates the counter storage once per node; later calls keep existing counts
  and tier settings. Initial tiers come from `:performance_probe_tiers`.
  """
  @spec install() :: :ok
  def install do
    case :persistent_term.get(@state_key, nil) do
      nil ->
        tiers = Application.get_env(:vial_keeper, :performance_probe_tiers, [:standard])
        counters = :counters.new(length(@probes) * @stride, [:write_concurrency])
        :persistent_term.put(@state_key, {counters, :standard in tiers, :detail in tiers})

      {_counters, _standard, _detail} ->
        :ok
    end
  end

  @doc "Enables a probe tier (an operator action; see the module docs)."
  @spec enable(tier()) :: :ok
  def enable(tier) when tier in @tiers, do: set_tier(tier, true)

  @doc "Disables a probe tier; existing counts are kept."
  @spec disable(tier()) :: :ok
  def disable(tier) when tier in @tiers, do: set_tier(tier, false)

  @doc "The enabled tiers (empty when probes are compiled out or not installed)."
  @spec enabled_tiers() :: [tier()]
  def enabled_tiers do
    case :persistent_term.get(@state_key, nil) do
      {_counters, standard, detail} when @compiled ->
        for {tier, true} <- [standard: standard, detail: detail], do: tier

      _ ->
        []
    end
  end

  @doc "Zeroes every counter."
  @spec reset() :: :ok
  def reset do
    case :persistent_term.get(@state_key, nil) do
      {counters, _standard, _detail} ->
        Enum.each(1..(length(@probes) * @stride), &:counters.put(counters, &1, 0))

      nil ->
        :ok
    end
  end

  @doc "Reads every probe with a non-zero count."
  @spec snapshot() :: snapshot()
  def snapshot do
    case :persistent_term.get(@state_key, nil) do
      {counters, _standard, _detail} -> read(counters)
      nil -> %{}
    end
  end

  @doc "Counters recorded between two snapshots (`later - earlier`)."
  @spec diff(snapshot(), snapshot()) :: snapshot()
  def diff(earlier, later) do
    Enum.reduce(later, %{}, fn {probe, stats}, acc ->
      base = Map.get(earlier, probe, %Stats{count: 0, total_ns: 0, buckets: empty_buckets()})
      count = stats.count - base.count

      if count > 0 do
        Map.put(acc, probe, %Stats{
          count: count,
          total_ns: stats.total_ns - base.total_ns,
          buckets: Enum.zip_with(stats.buckets, base.buckets, &(&1 - &2))
        })
      else
        acc
      end
    end)
  end

  @doc """
  JSON-ready summary of a snapshot (or diff), keyed by probe name.

  `p50_le_ns` and `p99_le_ns` are histogram upper bounds: the true percentile
  is at most that value. They are `nil` when the percentile falls in the open
  last bucket.
  """
  @spec summarize(snapshot()) :: %{binary() => map()}
  def summarize(snapshot) do
    Map.new(snapshot, fn {probe, stats} ->
      {Atom.to_string(probe),
       %{
         "area" => Atom.to_string(area_of(probe)),
         "tier" => Atom.to_string(tier_of(probe)),
         "count" => stats.count,
         "total_ns" => stats.total_ns,
         "mean_ns" => div(stats.total_ns, stats.count),
         "p50_le_ns" => bucket_percentile(stats, 0.50),
         "p99_le_ns" => bucket_percentile(stats, 0.99)
       }}
    end)
  end

  @doc "The closed probe vocabulary in slot order."
  @spec probes() :: [probe()]
  def probes, do: @probes

  @doc "Probes grouped by layer, outermost first."
  @spec areas() :: keyword([probe()])
  def areas, do: @areas

  @doc "Upper bounds (nanoseconds) of the histogram buckets; the last bucket is open."
  @spec bucket_bounds_ns() :: [pos_integer()]
  def bucket_bounds_ns, do: @bucket_bounds_ns

  @doc "Probe name for a mutation instrumentation phase."
  @spec mutation_probe(atom()) :: probe()
  for phase <- @mutation_phases do
    def mutation_probe(unquote(phase)), do: unquote(:"mutation_#{phase}")
  end

  @doc "Probe name for a SQLite instrumentation phase."
  @spec sqlite_phase_probe(atom()) :: probe()
  for phase <- @sqlite_phases do
    def sqlite_phase_probe(unquote(phase)), do: unquote(:"sqlite_#{phase}")
  end

  for {probe, index} <- Enum.with_index(@probes) do
    defp index(unquote(probe)), do: unquote(index)
  end

  for {area, probes} <- @areas, probe <- probes do
    defp area_of(unquote(probe)), do: unquote(area)

    defp tier_of(unquote(probe)),
      do: unquote(if area in @detail_areas, do: :detail, else: :standard)
  end

  defp index!(probe) do
    case Enum.find_index(@probes, &(&1 == probe)) do
      nil -> raise CompileError, description: "unknown performance probe #{inspect(probe)}"
      index -> index
    end
  end

  defp tier_of!(probe) do
    area = Enum.find_value(@areas, fn {area, probes} -> if probe in probes, do: area end)
    if area in @detail_areas, do: :detail, else: :standard
  end

  defp record(counters, index, duration) when duration >= 0 do
    base = index * @stride + 1
    :counters.add(counters, base, duration)
    :counters.add(counters, base + 1 + bucket(duration), 1)
  end

  # A clock that steps backwards is not a measurement.
  defp record(_counters, _index, _duration), do: :ok

  # Binary search over the bucket bounds, converted to native time units at
  # compile time (native time is nanoseconds on Linux).
  @native_bounds @bucket_bounds_ns
                 |> Enum.map(&System.convert_time_unit(&1, :nanosecond, :native))
                 |> List.to_tuple()

  defp bucket(duration), do: bucket(duration, 0, @bucket_count - 1)

  defp bucket(_duration, low, high) when low >= high, do: low

  defp bucket(duration, low, high) do
    middle = div(low + high, 2)

    if duration <= elem(@native_bounds, middle),
      do: bucket(duration, low, middle),
      else: bucket(duration, middle + 1, high)
  end

  defp bucket_percentile(%{count: count, buckets: buckets}, fraction) do
    target = Float.ceil(count * fraction)

    buckets
    |> Enum.scan(&(&1 + &2))
    |> Enum.find_index(&(&1 >= target))
    |> then(&Enum.at(@bucket_bounds_ns, &1))
  end

  @indexed_probes Enum.with_index(@probes)

  defp read(counters) do
    Enum.reduce(@indexed_probes, %{}, fn {probe, index}, acc ->
      base = index * @stride + 1
      buckets = Enum.map(1..@bucket_count, &:counters.get(counters, base + &1))

      case Enum.sum(buckets) do
        0 ->
          acc

        count ->
          total = System.convert_time_unit(:counters.get(counters, base), :native, :nanosecond)
          Map.put(acc, probe, %Stats{count: count, total_ns: total, buckets: buckets})
      end
    end)
  end

  defp empty_buckets, do: List.duplicate(0, @bucket_count)

  defp set_tier(tier, value) do
    case :persistent_term.get(@state_key, nil) do
      {counters, standard, detail} = current ->
        updated =
          case tier do
            :standard -> {counters, value, detail}
            :detail -> {counters, standard, value}
          end

        if updated != current, do: :persistent_term.put(@state_key, updated)
        :ok

      nil ->
        :ok
    end
  end
end
