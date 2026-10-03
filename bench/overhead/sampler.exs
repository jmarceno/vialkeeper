defmodule VialKeeper.Benchmarks.Overhead.Sampler do
  @moduledoc """
  Paired, rotating, adaptively stopped sample collection.

  Each sample runs every variant once on the same prepared input. The timed
  region contains only the variant call: inputs are built by `prepare` before
  any timer starts. Variant order rotates through all positions and reverses
  on alternate cycles, so no variant always runs first, last, or right after a
  particular neighbour.

  Timing uses `System.monotonic_time/0` in native units converted to
  nanoseconds. A variant may instead report its own duration by returning
  `{:timed, nanoseconds, extra}` (used by out-of-process native controls that
  time themselves); otherwise the call must return `:ok`.

  Collection stops at the first of:

    * every compared variant's paired-ratio CI half-width is at most
      `target_ci_pct` (after at least `min_iterations`),
    * `max_iterations` samples,
    * the wall-clock `budget_ms` for the case.

  No garbage collection is forced: a long-lived storage owner runs with a warm
  heap, so forcing a collection would charge heap regrowth to the timed region.
  Global GC counts and reductions are recorded per sample instead.
  """

  alias VialKeeper.Benchmarks.Overhead.Stats

  @check_every 10

  @type variant :: atom()
  @type token :: {:warmup | :sample, non_neg_integer()}

  @doc """
  Runs warmup and measured samples.

  Options (all required): `:variants` (ordered list), `:reference` (variant the
  stop rule compares against), `:prepare` (`token -> input`), `:invoke`
  (`variant, input -> :ok | {:timed, ns, extra}`), `:warmup`,
  `:min_iterations`, `:max_iterations`, `:target_ci_pct`, `:budget_ms`.
  """
  @spec run(keyword()) :: map()
  def run(opts) do
    variants = Keyword.fetch!(opts, :variants)
    prepare = Keyword.fetch!(opts, :prepare)
    invoke = Keyword.fetch!(opts, :invoke)
    warmup = Keyword.fetch!(opts, :warmup)

    Enum.each(sequence(warmup), fn absolute ->
      input = prepare.({:warmup, absolute})

      variants
      |> order(absolute)
      |> Enum.each(fn variant -> checked_invoke(invoke, variant, input) end)
    end)

    started = System.monotonic_time(:millisecond)
    collect(opts, variants, prepare, invoke, warmup, started, [])
  end

  @doc "Rotated (and alternately reversed) variant order for an absolute sample index."
  @spec order([variant()], non_neg_integer()) :: [variant()]
  def order(variants, absolute) do
    count = length(variants)
    shift = rem(absolute, count)
    rotated = Enum.drop(variants, shift) ++ Enum.take(variants, shift)
    if rem(div(absolute, count), 2) == 1, do: Enum.reverse(rotated), else: rotated
  end

  defp collect(opts, variants, prepare, invoke, warmup, started, samples) do
    count = length(samples)

    case stop_reason(opts, variants, samples, count, started) do
      nil ->
        absolute = warmup + count
        input = prepare.({:sample, absolute})

        sample =
          variants
          |> order(absolute)
          |> Map.new(fn variant -> {variant, timed(invoke, variant, input)} end)
          |> Map.put(:__order__, order(variants, absolute))

        collect(opts, variants, prepare, invoke, warmup, started, [sample | samples])

      reason ->
        ordered = Enum.reverse(samples)

        %{
          samples: ordered,
          stop_reason: reason,
          elapsed_ms: System.monotonic_time(:millisecond) - started
        }
    end
  end

  defp stop_reason(opts, variants, samples, count, started) do
    cond do
      count >= Keyword.fetch!(opts, :max_iterations) ->
        "max_iterations"

      count > 0 and
          System.monotonic_time(:millisecond) - started >= Keyword.fetch!(opts, :budget_ms) ->
        "time_budget"

      count >= Keyword.fetch!(opts, :min_iterations) and rem(count, @check_every) == 0 and
          converged?(opts, variants, samples) ->
        "converged"

      true ->
        nil
    end
  end

  defp converged?(opts, variants, samples) do
    reference = Keyword.fetch!(opts, :reference)
    target = Keyword.fetch!(opts, :target_ci_pct)
    reference_ns = Enum.map(samples, &get_in(&1, [reference, :ns]))

    variants
    |> Enum.reject(&(&1 == reference))
    |> Enum.all?(fn variant ->
      ratios = Stats.paired_ratios(reference_ns, Enum.map(samples, &get_in(&1, [variant, :ns])))
      width = Stats.relative_half_width_pct(Stats.median_ci(ratios), Stats.median(ratios))
      is_number(width) and width <= target
    end)
  end

  defp timed(invoke, variant, input) do
    {reductions_before, _} = :erlang.statistics(:exact_reductions)
    {gcs_before, _, _} = :erlang.statistics(:garbage_collection)
    started = System.monotonic_time()
    result = invoke.(variant, input)
    finished = System.monotonic_time()
    {reductions_after, _} = :erlang.statistics(:exact_reductions)
    {gcs_after, _, _} = :erlang.statistics(:garbage_collection)

    base = %{
      reductions: reductions_after - reductions_before,
      gcs: gcs_after - gcs_before
    }

    case result do
      :ok ->
        Map.put(base, :ns, System.convert_time_unit(finished - started, :native, :nanosecond))

      {:timed, ns, extra} when is_integer(ns) and is_map(extra) ->
        base |> Map.put(:ns, ns) |> Map.put(:extra, extra)

      other ->
        raise "benchmark variant #{variant} returned #{inspect(other)}"
    end
  end

  defp checked_invoke(invoke, variant, input) do
    case invoke.(variant, input) do
      :ok -> :ok
      {:timed, _ns, _extra} -> :ok
      other -> raise "benchmark variant #{variant} returned #{inspect(other)}"
    end
  end

  defp sequence(0), do: []
  defp sequence(count), do: 0..(count - 1)
end
