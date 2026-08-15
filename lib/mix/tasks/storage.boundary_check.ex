defmodule Mix.Tasks.Storage.BoundaryCheck do
  @moduledoc """
  Runs the repository's backend-physical reference boundary scan.

  This task is intentionally separate from the normal fast test loop because it
  walks repository source and documentation files. It is included in the full
  validation alias and can be run directly when storage-boundary files change.
  """

  use Mix.Task

  alias VialKeeper.Storage.BoundaryGuard

  @shortdoc "Checks backend-physical references stay inside their boundary"

  @switches [root: :string]

  @impl Mix.Task
  @spec run([binary()]) :: :ok
  def run(args) do
    {options, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if positional != [] or invalid != [] do
      Mix.raise("invalid arguments: #{inspect(positional ++ invalid)}\n\n#{usage()}")
    end

    root = Path.expand(options[:root] || File.cwd!())
    findings = BoundaryGuard.scan(root)

    if findings == [] do
      Mix.shell().info("Storage boundary check passed for #{root}")
      :ok
    else
      Mix.shell().error("Storage boundary check found #{length(findings)} finding(s):")

      Enum.each(findings, fn finding ->
        Mix.shell().error(
          "  #{finding.path}:#{finding.line} [#{finding.pattern}] #{finding.excerpt}"
        )
      end)

      Mix.raise("backend-physical references escaped the approved boundary")
    end
  end

  defp usage do
    "mix storage.boundary_check [--root PATH]"
  end
end
