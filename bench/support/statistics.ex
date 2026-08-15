defmodule VialKeeper.Bench.Statistics do
  @moduledoc "Latency percentiles, throughput, and size helpers for data-backed runners."

  alias VialKeeper.Runtime.DatabaseCatalog

  @spec summarize([number()], pos_integer()) :: map()
  def summarize(samples, operation_count)
      when is_list(samples) and is_integer(operation_count) and operation_count > 0 do
    values = Enum.sort(samples)
    count = length(values)

    if count == 0 do
      %{
        "sample_us" => [],
        "median_us" => nil,
        "p50_us" => nil,
        "p90_us" => nil,
        "p95_us" => nil,
        "p99_us" => nil
      }
    else
      median = percentile(values, 0.50)

      %{
        "sample_us" => values,
        "mean_us" => Float.round(Enum.sum(values) / count, 2),
        "median_us" => median,
        "p50_us" => median,
        "p90_us" => percentile(values, 0.90),
        "p95_us" => percentile(values, 0.95),
        "p99_us" => percentile(values, 0.99),
        "median_us_per_operation" => Float.round(median / operation_count, 2),
        "median_operations_per_second" =>
          Float.round(operation_count * 1_000_000 / max(median, 1), 2)
      }
    end
  end

  @spec percentile([number()], float()) :: number()
  def percentile([], _fraction), do: nil

  def percentile(values, fraction) when is_list(values) and is_float(fraction) do
    index = max(1, ceil(length(values) * fraction)) - 1
    Enum.at(values, index)
  end

  @spec directory_bytes(Path.t()) :: non_neg_integer()
  def directory_bytes(path) when is_binary(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} ->
        size

      {:ok, %{type: :directory}} ->
        child_bytes(path)

      _ ->
        0
    end
  end

  @spec runtime_metadata() :: map()
  def runtime_metadata do
    %{
      "elixir_version" => System.version(),
      "otp_release" => :erlang.system_info(:otp_release) |> to_string(),
      "schedulers" => :erlang.system_info(:schedulers_online),
      "dirty_cpu_schedulers" => dirty_schedulers(:dirty_cpu_schedulers_online),
      "memory_total_bytes" => :erlang.memory(:total)
    }
  end

  @spec per_sec(number(), number()) :: float()
  def per_sec(_count, elapsed) when elapsed <= 0, do: 0.0
  def per_sec(count, elapsed), do: Float.round(count * 1_000_000 / elapsed, 2)

  @spec mib_per_sec(number(), number()) :: float()
  def mib_per_sec(_bytes, elapsed) when elapsed <= 0, do: 0.0

  def mib_per_sec(bytes, elapsed) do
    Float.round(bytes / 1_048_576 * 1_000_000 / elapsed, 2)
  end

  @spec file_size(Path.t()) :: non_neg_integer()
  def file_size(path) when is_binary(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  @spec bundle_bytes(binary()) :: non_neg_integer()
  def bundle_bytes(uuid) when is_binary(uuid) do
    case DatabaseCatalog.bundle_root(uuid) do
      {:ok, root} -> directory_bytes(root)
      _ -> 0
    end
  end

  @spec cas_bytes(binary()) :: non_neg_integer()
  def cas_bytes(uuid) when is_binary(uuid) do
    case DatabaseCatalog.bundle_root(uuid) do
      {:ok, root} -> directory_bytes(Path.join(root, "blobs"))
      _ -> 0
    end
  end

  @spec snapshot_bytes(binary(), :bundle | :cas, atom()) :: non_neg_integer()
  def snapshot_bytes(uuid, :bundle, _label) when is_binary(uuid) do
    bundle_bytes(uuid)
  end

  def snapshot_bytes(uuid, :cas, _label) when is_binary(uuid) do
    cas_bytes(uuid)
  end

  @spec times(integer(), (-> term())) :: :ok
  def times(n, fun) when is_integer(n) and n > 0 and is_function(fun, 0) do
    Enum.each(1..n, fn _ -> fun.() end)
    :ok
  end

  def times(_n, fun) when is_function(fun, 0), do: :ok

  @spec timed_us((-> term())) :: integer()
  def timed_us(fun) when is_function(fun, 0) do
    {elapsed, _} = :timer.tc(fun)
    elapsed
  end

  @spec sample_us(pos_integer(), (-> term())) :: [integer()]
  def sample_us(n, fun) when is_integer(n) and n > 0 and is_function(fun, 0) do
    Enum.map(1..n, fn _ -> timed_us(fun) end)
  end

  defp child_bytes(path) do
    case File.ls(path) do
      {:ok, entries} -> Enum.reduce(entries, 0, &(&2 + directory_bytes(Path.join(path, &1))))
      {:error, _} -> 0
    end
  end

  defp dirty_schedulers(key) do
    :erlang.system_info(key)
  rescue
    ArgumentError -> nil
  end
end
