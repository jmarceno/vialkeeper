defmodule VialKeeper.Bench.Reports do
  @moduledoc "JSON report writer constrained to `<bench-root>/reports/`."

  alias VialKeeper.AtomicWrite
  alias VialKeeper.Bench.{Root, Statistics}

  @spec write(Root.t(), binary(), map(), keyword()) :: {:ok, Path.t()} | {:error, binary()}
  def write(%Root{} = context, default_name, report, opts \\ []) do
    with {:ok, path} <- output_path(context, default_name, opts[:output]),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <-
           (case AtomicWrite.write(path, JSON.encode!(report) <> "\n") do
              :ok -> :ok
              {:error, reason} -> {:error, "failed to write report #{path}: #{inspect(reason)}"}
            end) do
      {:ok, path}
    end
  end

  @spec envelope(Root.t(), map(), map()) :: map()
  def envelope(%Root{} = context, extras, results) when is_map(extras) and is_map(results) do
    Map.merge(
      %{
        "schema_version" => 1,
        "git_revision" => git_revision(),
        "runtime" => Statistics.runtime_metadata(),
        "benchmark_root" => context.root,
        "root_id" => context.root_id,
        "approved_parent" => context.approved_parent
      },
      extras
    )
    |> Map.put("results", results)
  end

  defp output_path(context, default_name, nil) do
    Root.report_path(context, default_name)
  end

  defp output_path(context, _default_name, output) when is_binary(output) do
    expanded = Path.expand(output)

    with {:ok, reports} <- Root.reports_dir(context) do
      if Root.descendant?(expanded, reports) or expanded == reports do
        {:ok, expanded}
      else
        {:error, "--output must resolve under #{reports}"}
      end
    end
  end

  defp git_revision do
    case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      {sha, 0} -> String.trim(sha)
      _ -> "unknown"
    end
  end
end
