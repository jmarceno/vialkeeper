defmodule VialKeeper.Bench.CLI do
  @moduledoc "Mix entrypoints for data-backed benchmark commands."

  alias VialKeeper.Bench.{FTS, PerformanceDiagnostics, Prepare, Root, Stress, Torture}

  @benchmark_options [:profile, :output, :warmup, :iterations]

  @spec run_data([binary()]) :: :ok
  def run_data(argv) do
    argv = strip_dashes(argv)

    case argv do
      ["configure" | rest] -> configure(rest)
      ["status" | rest] -> status(rest)
      ["prepare", name | rest] -> prepare(name, rest)
      ["clean", name | rest] -> clean(name, rest)
      ["help"] -> success(data_usage())
      [] -> fail(data_usage())
      other -> fail("unknown bench.data arguments: #{inspect(other)}\n\n#{data_usage()}")
    end
  end

  @spec run_fts([binary()]) :: :ok
  def run_fts(argv),
    do: run_benchmark(&FTS.run/1, argv, "fts", [:stall_timeout_ms | @benchmark_options])

  @spec run_diagnostics([binary()]) :: :ok
  def run_diagnostics(argv) do
    run_benchmark(
      &PerformanceDiagnostics.run/1,
      argv,
      "diagnostics",
      @benchmark_options ++
        [
          :section,
          :document_mode,
          :counts,
          :batch_sizes,
          :search_batch_sizes,
          :attachment_sizes,
          :attachment_mode,
          :attachment_chunk_sizes,
          :attachment_concurrency
        ]
    )
  end

  @spec run_stress([binary()]) :: :ok
  def run_stress(argv),
    do:
      run_benchmark(
        &Stress.run/1,
        argv,
        "stress",
        @benchmark_options ++ [:max_concurrency, :diagnostic_ceiling_seconds, :stall_timeout_ms]
      )

  @spec run_torture([binary()]) :: :ok
  def run_torture(argv),
    do:
      run_benchmark(&Torture.run/1, argv, "torture", [
        :limit,
        :stall_timeout_ms | @benchmark_options
      ])

  defp configure(argv) do
    {opts, positional, invalid} =
      OptionParser.parse(argv,
        strict: [root: :string, reuse_existing: :boolean, help: :boolean]
      )

    cond do
      opts[:help] ->
        success(data_usage())

      positional != [] or invalid != [] ->
        fail("invalid configure arguments")

      not is_binary(opts[:root]) ->
        fail("--root is required and must be absolute")

      true ->
        result =
          Root.configure(opts[:root], reuse_existing: opts[:reuse_existing] || false)

        case result do
          {:ok, context} ->
            Mix.shell().info("configured benchmark root #{context.root} (id #{context.root_id})")
            :ok

          {:error, message} ->
            fail(message)
        end
    end
  end

  defp status(argv) do
    case parse_common!(argv, []) do
      :help ->
        success(data_usage())

      opts ->
        case Prepare.status(opts) do
          {:ok, status} ->
            print_status(status)
            :ok

          {:error, message} ->
            fail(message)
        end
    end
  end

  defp prepare(name, argv) do
    case parse_common!(argv, [:profile, :max_concurrency, :stall_timeout_ms]) do
      :help ->
        success(data_usage())

      opts ->
        case Prepare.prepare(name, opts) do
          {:ok, result} ->
            Mix.shell().info("prepared #{result["dataset"]} at #{result["path"]}")
            :ok

          {:error, message} ->
            fail(message)
        end
    end
  end

  defp clean(name, argv) do
    case parse_common!(argv, []) do
      :help ->
        success(data_usage())

      opts ->
        case Prepare.clean(name, opts) do
          :ok ->
            Mix.shell().info("removed dataset #{name}")
            :ok

          {:error, message} ->
            fail(message)
        end
    end
  end

  defp run_benchmark(fun, argv, name, extra) do
    case parse_common!(argv, extra) do
      :help ->
        success(data_usage())

      opts ->
        case fun.(opts) do
          {:ok, path} when is_binary(path) ->
            Mix.shell().info("#{name} report written to #{path}")
            :ok

          {:ok, %{}} = ok ->
            Mix.shell().info("#{name} benchmark completed")
            ok

          {:error, message} ->
            fail(message)
        end
    end
  end

  defp parse_common!(argv, extra) do
    {opts, positional, invalid} =
      OptionParser.parse(argv, strict: allowed_switch_types(extra))

    cond do
      opts[:help] ->
        :help

      positional != [] or invalid != [] ->
        fail("invalid arguments: #{inspect(positional ++ invalid)}")

      opts[:root] ->
        fail("benchmark commands do not accept --root; use mix bench.data configure")

      true ->
        maybe_profile(Keyword.delete(opts, :root))
    end
  end

  defp allowed_switch_types(extra) do
    base = [help: :boolean, root: :string]

    extra
    |> Enum.reduce(base, fn
      :profile, acc ->
        Keyword.put(acc, :profile, :string)

      :output, acc ->
        Keyword.put(acc, :output, :string)

      :warmup, acc ->
        Keyword.put(acc, :warmup, :integer)

      :iterations, acc ->
        Keyword.put(acc, :iterations, :integer)

      :max_concurrency, acc ->
        Keyword.put(acc, :max_concurrency, :integer)

      :diagnostic_ceiling_seconds, acc ->
        Keyword.put(acc, :diagnostic_ceiling_seconds, :integer)

      :section, acc ->
        Keyword.put(acc, :section, :string)

      :document_mode, acc ->
        Keyword.put(acc, :document_mode, :string)

      :counts, acc ->
        Keyword.put(acc, :counts, :string)

      :batch_sizes, acc ->
        Keyword.put(acc, :batch_sizes, :string)

      :search_batch_sizes, acc ->
        Keyword.put(acc, :search_batch_sizes, :string)

      :attachment_sizes, acc ->
        Keyword.put(acc, :attachment_sizes, :string)

      :attachment_mode, acc ->
        Keyword.put(acc, :attachment_mode, :string)

      :attachment_chunk_sizes, acc ->
        Keyword.put(acc, :attachment_chunk_sizes, :string)

      :attachment_concurrency, acc ->
        Keyword.put(acc, :attachment_concurrency, :string)

      :limit, acc ->
        Keyword.put(acc, :limit, :integer)

      :stall_timeout_ms, acc ->
        Keyword.put(acc, :stall_timeout_ms, :integer)

      _, acc ->
        acc
    end)
  end

  defp maybe_profile(opts) do
    case opts[:profile] do
      nil ->
        Keyword.delete(opts, :help)

      value ->
        case VialKeeper.Bench.Registry.profile(value) do
          {:ok, profile} -> Keyword.put(Keyword.delete(opts, :help), :profile, profile)
          {:error, message} -> fail(message)
        end
    end
  end

  defp print_status(status) do
    Mix.shell().info("benchmark root:     #{status["root"]}")
    Mix.shell().info("canonical root:     #{status["canonical_root"]}")
    Mix.shell().info("free space bytes:   #{status["free_bytes"]}")
    Mix.shell().info("")

    Enum.each(status["datasets"], fn dataset ->
      estimate =
        if dataset["source_bytes_estimated?"],
          do: "estimated",
          else: "pinned"

      Mix.shell().info(
        "#{dataset["name"]} #{dataset["version"]}: #{dataset["state"]} " <>
          "source=#{dataset["expected_source_bytes"]} (#{estimate}) " <>
          "local=#{dataset["local_bytes"]} " <>
          "working~=#{dataset["estimated_working_bytes"]}"
      )
    end)
  end

  defp strip_dashes(["--" | rest]), do: rest
  defp strip_dashes(argv), do: argv

  defp data_usage do
    """
    mix bench.data configure --root /mnt/other/downloads/vialkeeper [--reuse-existing]
    mix bench.data status
    mix bench.data prepare trec-covid|pmc|simplewiki|open-images [--profile standard|smoke|1k|10k] [--max-concurrency 1..16]
    mix bench.data clean trec-covid|pmc|simplewiki|open-images
    """
  end

  defp success(message) do
    Mix.shell().info(message)
    :ok
  end

  @spec fail(binary()) :: no_return()
  defp fail(message) when is_binary(message) do
    Mix.raise(message)
  end
end
