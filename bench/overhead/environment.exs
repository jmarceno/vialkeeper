defmodule VialKeeper.Benchmarks.Overhead.Environment do
  @moduledoc """
  Records the conditions a report was measured under.

  Two reports are comparable only when these match: same CPU, same clock
  source, same affinity, same VM scheduler flags, same SQLite build, and the
  same source revision. Fields that cannot be read on a platform are `nil`.
  """

  @doc "Returns runtime and host metadata for the report."
  @spec metadata(map()) :: map()
  def metadata(sqlite) do
    %{
      "elixir" => System.version(),
      "otp" => :erlang.system_info(:otp_release) |> to_string(),
      "erts" => :erlang.system_info(:version) |> to_string(),
      "jit" => :erlang.system_info(:emu_flavor) |> to_string(),
      "sqlite" => sqlite,
      "os" => :os.type() |> inspect(),
      "schedulers_online" => :erlang.system_info(:schedulers_online),
      "dirty_cpu_schedulers_online" => :erlang.system_info(:dirty_cpu_schedulers_online),
      "dirty_io_schedulers" => :erlang.system_info(:dirty_io_schedulers),
      "scheduler_bind_type" => :erlang.system_info(:scheduler_bind_type) |> inspect(),
      "elixir_erl_options" => System.get_env("ELIXIR_ERL_OPTIONS"),
      "erl_flags" => System.get_env("ERL_FLAGS"),
      "clock_source" =>
        read_trimmed("/sys/devices/system/clocksource/clocksource0/current_clocksource"),
      "cpu_model" => cpu_model(),
      "cpus_allowed" => proc_status_field("Cpus_allowed_list"),
      "cpu_governor" => read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
      "native_time_unit_per_second" => System.convert_time_unit(1, :second, :native),
      "git_revision" => git(["rev-parse", "HEAD"]),
      "git_dirty" => git_dirty?()
    }
  end

  defp cpu_model do
    case File.read("/proc/cpuinfo") do
      {:ok, contents} ->
        contents
        |> String.split("\n")
        |> Enum.find_value(fn line ->
          case String.split(line, ":", parts: 2) do
            [key, value] -> if String.trim(key) == "model name", do: String.trim(value)
            _ -> nil
          end
        end)

      {:error, _} ->
        nil
    end
  end

  defp proc_status_field(field) do
    case File.read("/proc/self/status") do
      {:ok, contents} ->
        contents
        |> String.split("\n")
        |> Enum.find_value(fn line ->
          case String.split(line, ":", parts: 2) do
            [^field, value] -> String.trim(value)
            _ -> nil
          end
        end)

      {:error, _} ->
        nil
    end
  end

  defp read_trimmed(path) do
    case File.read(path) do
      {:ok, contents} -> String.trim(contents)
      {:error, _} -> nil
    end
  end

  defp git(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {output, 0} -> String.trim(output)
      _ -> nil
    end
  end

  defp git_dirty? do
    case git(["status", "--porcelain", "--untracked-files=no"]) do
      nil -> nil
      output -> output != ""
    end
  end
end
