defmodule VialKeeper.Quality.ReachSmells.JustifiedSuppressions do
  @moduledoc """
  Requires a `# quality:reason ...` justification next to Credo, Reach, and
  Dialyzer suppressions. The quality tool tree is excluded.
  """

  @behaviour Reach.Smell.Check

  alias Reach.Smell.Finding
  alias Reach.Smell.Source
  alias VialKeeper.Quality.ReachSmells.SourcePaths

  @kind :vial_keeper_unjustified_suppression
  @reason_prefix "# quality:reason "

  @impl true
  def kinds, do: [@kind]

  @impl true
  def run(project) do
    project
    |> Source.module_files()
    |> Enum.reject(&SourcePaths.quality?/1)
    |> Enum.flat_map(&scan_file/1)
  end

  @spec scan_file(Path.t()) :: [Finding.t()]
  def scan_file(file) do
    lines =
      file
      |> File.read!()
      |> String.split("\n")
      |> Enum.with_index(1)

    comment_findings(file, lines) ++ dialyzer_findings(file, lines)
  end

  defp comment_findings(file, lines) do
    Enum.flat_map(lines, fn {line, number} ->
      trimmed = String.trim(line)

      cond do
        String.starts_with?(trimmed, "# credo:disable") and not justified?(lines, number) ->
          [finding(file, number, "Credo suppression requires an adjacent # quality:reason")]

        String.starts_with?(trimmed, "# reach:disable") and not justified?(lines, number) ->
          [finding(file, number, "Reach suppression requires an adjacent # quality:reason")]

        true ->
          []
      end
    end)
  end

  defp dialyzer_findings(file, lines) do
    Enum.flat_map(lines, fn {line, number} ->
      if String.match?(String.trim(line), ~r/^@dialyzer\b/) and not justified?(lines, number) do
        [finding(file, number, "@dialyzer requires an adjacent # quality:reason")]
      else
        []
      end
    end)
  end

  defp justified?(lines, number) do
    Enum.any?(adjacent_lines(lines, number), &reason?/1)
  end

  defp adjacent_lines(lines, number) do
    for offset <- [-1, 0, 1],
        {line, _n} <- List.wrap(Enum.find(lines, fn {_line, n} -> n == number + offset end)) do
      String.trim(line)
    end
  end

  defp reason?(line), do: String.starts_with?(line, @reason_prefix) and String.length(line) > 18

  defp finding(file, number, message) do
    Finding.new(kind: @kind, message: message, location: "#{file}:#{number}")
  end
end
