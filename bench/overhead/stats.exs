defmodule VialKeeper.Benchmarks.Overhead.Stats do
  @moduledoc """
  Order-preserving statistics for paired benchmark samples.

  Samples are kept in collection order everywhere: paired deltas and ratios are
  computed sample-by-sample (`candidate[i]` against `reference[i]`), never from
  sorted copies. Percentiles use linear interpolation between closest ranks, so
  an even-sized median is the mean of the two middle values.

  Confidence intervals are percentile bootstrap intervals of the median with a
  fixed seed, which makes a report reproducible from its raw samples.
  """

  @bootstrap_resamples 1_000
  @bootstrap_seed {1_729, 4_104, 13_832}

  @type samples :: [number()]

  @doc "Interpolated percentile (`fraction` in 0..1) of unsorted samples."
  @spec percentile(samples(), float()) :: float() | nil
  def percentile([], _fraction), do: nil

  def percentile(samples, fraction) when fraction >= 0 and fraction <= 1 do
    samples |> Enum.sort() |> List.to_tuple() |> sorted_percentile(fraction)
  end

  @doc "Interpolated median of unsorted samples."
  @spec median(samples()) :: float() | nil
  def median(samples), do: percentile(samples, 0.5)

  @doc "Median absolute deviation from the median."
  @spec mad(samples()) :: float() | nil
  def mad([]), do: nil

  def mad(samples) do
    center = median(samples)
    samples |> Enum.map(&abs(&1 - center)) |> median()
  end

  @doc "Population coefficient of variation in percent."
  @spec cv_pct(samples()) :: float() | nil
  def cv_pct([]), do: nil

  def cv_pct(samples) do
    count = length(samples)
    mean = Enum.sum(samples) / count

    if mean == 0 do
      0.0
    else
      variance = Enum.reduce(samples, 0.0, fn value, acc -> acc + (value - mean) ** 2 end) / count
      :math.sqrt(variance) / mean * 100
    end
  end

  @doc "Sample-by-sample differences `candidate[i] - reference[i]`."
  @spec paired_deltas(samples(), samples()) :: samples()
  def paired_deltas(reference, candidate) when length(reference) == length(candidate),
    do: Enum.zip_with(candidate, reference, &(&1 - &2))

  @doc "Sample-by-sample ratios `candidate[i] / reference[i]`."
  @spec paired_ratios(samples(), samples()) :: samples()
  def paired_ratios(reference, candidate) when length(reference) == length(candidate),
    do: Enum.zip_with(candidate, reference, fn value, base -> value / max(base, 1) end)

  @doc """
  Percentile-bootstrap confidence interval of the median.

  Returns `{low, high}` for the given two-sided `confidence` (default 0.95).
  Uses a private, fixed-seed random state so results are reproducible and the
  caller's `:rand` state is untouched.
  """
  @spec median_ci(samples(), float()) :: {float(), float()} | nil
  def median_ci(samples, confidence \\ 0.95)
  def median_ci([], _confidence), do: nil
  def median_ci([value], _confidence), do: {value * 1.0, value * 1.0}

  def median_ci(samples, confidence) do
    values = List.to_tuple(samples)
    count = tuple_size(values)
    state = :rand.seed_s(:exsss, @bootstrap_seed)

    {medians, _state} =
      Enum.map_reduce(1..@bootstrap_resamples, state, fn _, state ->
        {resample, state} = resample(values, count, state)
        {resample |> Enum.sort() |> List.to_tuple() |> sorted_percentile(0.5), state}
      end)

    sorted = medians |> Enum.sort() |> List.to_tuple()
    tail = (1 - confidence) / 2
    {sorted_percentile(sorted, tail), sorted_percentile(sorted, 1 - tail)}
  end

  @doc """
  Relative half-width of a confidence interval around `center`, in percent.

  This is the stopping metric: a ratio of 2.40 with CI {2.37, 2.43} has a
  relative half-width of 1.25%.
  """
  @spec relative_half_width_pct({number(), number()} | nil, number() | nil) :: float() | nil
  def relative_half_width_pct(nil, _center), do: nil
  def relative_half_width_pct(_ci, nil), do: nil
  def relative_half_width_pct(_ci, center) when center == 0, do: nil

  def relative_half_width_pct({low, high}, center),
    do: (high - low) / 2 / abs(center) * 100

  @doc "Summary of one variant's samples (nanoseconds per sample)."
  @spec summarize(samples(), pos_integer()) :: map()
  def summarize(samples, operations_per_sample) do
    median = median(samples)
    {ci_low, ci_high} = median_ci(samples)
    count = length(samples)

    %{
      "count" => count,
      "min_ns" => Enum.min(samples),
      "max_ns" => Enum.max(samples),
      "mean_ns" => round_float(Enum.sum(samples) / count),
      "median_ns" => round_float(median),
      "median_ci95_ns" => [round_float(ci_low), round_float(ci_high)],
      "p90_ns" => round_float(percentile(samples, 0.90)),
      # Tail percentiles are only meaningful with enough samples to rank them.
      "p99_ns" => if(count >= 100, do: round_float(percentile(samples, 0.99))),
      "mad_ns" => round_float(mad(samples)),
      "cv_pct" => round_float(cv_pct(samples)),
      "operations_per_sample" => operations_per_sample,
      "median_ns_per_operation" => round_float(median / operations_per_sample),
      "median_operations_per_second" => round_float(operations_per_sample * 1.0e9 / median)
    }
  end

  @doc "Paired comparison of `candidate` against `reference`."
  @spec compare(samples(), samples()) :: map()
  def compare(reference, candidate) do
    deltas = paired_deltas(reference, candidate)
    ratios = paired_ratios(reference, candidate)
    delta_median = median(deltas)
    ratio_median = median(ratios)
    ratio_ci = median_ci(ratios)
    {delta_low, delta_high} = median_ci(deltas)
    {ratio_low, ratio_high} = ratio_ci

    %{
      "paired_delta_median_ns" => round_float(delta_median),
      "paired_delta_ci95_ns" => [round_float(delta_low), round_float(delta_high)],
      "paired_ratio_median" => round_float(ratio_median, 4),
      "paired_ratio_ci95" => [round_float(ratio_low, 4), round_float(ratio_high, 4)],
      "paired_ratio_ci_half_width_pct" =>
        round_float(relative_half_width_pct(ratio_ci, ratio_median)),
      "overhead_pct" => round_float((ratio_median - 1) * 100)
    }
  end

  @doc "Rounds floats for reports; passes `nil` through."
  @spec round_float(number() | nil, non_neg_integer()) :: float() | nil
  def round_float(value, digits \\ 2)
  def round_float(nil, _digits), do: nil
  def round_float(value, digits), do: Float.round(value * 1.0, digits)

  defp resample(values, count, state) do
    Enum.map_reduce(1..count, state, fn _, state ->
      {index, state} = :rand.uniform_s(count, state)
      {elem(values, index - 1), state}
    end)
  end

  defp sorted_percentile(sorted, fraction) do
    count = tuple_size(sorted)
    rank = fraction * (count - 1)
    lower = floor(rank)
    upper = min(lower + 1, count - 1)
    weight = rank - lower
    elem(sorted, lower) * (1 - weight) + elem(sorted, upper) * weight
  end
end
