defmodule Mix.Tasks.Vialkeeper.Backup.Manifest do
  @moduledoc """
  Generates a verified backup generation manifest for a closed `.vialkeeper` bundle.

  The bundle must already be closed (`POST /v1/databases/:uuid/close`) and
  pass the task's offline logical and physical integrity check.
  The manifest is written atomically next to the bundle as
  `<bundle>.manifest.json`.

  No product backup API is created. This task wraps the same offline logic
  as `VialKeeper.Backup.Manifest` for operator convenience.

  ## Usage

      mix vialkeeper.backup.manifest /path/to/bundle.vialkeeper \\
        --source-path notes.vialkeeper \\
        [--generation-id <uuid>] \\
        [--manifest-path /path/to/manifest.json]

  Storage versions and database UUID are read from the bundle's
  `database.sqlite3`; operator-supplied identity metadata is not accepted.

  The manifest includes `Diagnostics.runtime/0`, bundle sizes, SHA-256 of
  `database.sqlite3` and of `blobs/`, and the post-close integrity result.
  See `VialKeeper Recovery Strategy` MAINT-006 and `Operations.md`.
  """

  use Mix.Task

  alias VialKeeper.Backup.Manifest
  alias VialKeeper.Storage.SQLite.BackupManifest, as: SQLiteBackup

  @shortdoc "Generates a backup generation manifest for a closed bundle"

  @switches [
    source_path: :string,
    generation_id: :string,
    manifest_path: :string
  ]

  @impl Mix.Task
  @spec run([binary()]) :: :ok
  def run(args) do
    {options, positional, invalid} = OptionParser.parse(args, strict: @switches)

    if invalid != [] do
      Mix.raise("invalid arguments: #{inspect(invalid)}\n\n#{usage()}")
    end

    case positional do
      [bundle_path] ->
        bundle_path = Path.expand(bundle_path)
        generate(bundle_path, options)

      _ ->
        Mix.raise("bundle path is required\n\n#{usage()}")
    end
  end

  defp generate(bundle_path, options) do
    source_path = options[:source_path] || Path.basename(bundle_path)
    generation_id = options[:generation_id]
    manifest_path = options[:manifest_path]

    attrs =
      %{source_path: source_path}
      |> maybe_put(:generation_id, generation_id)
      |> maybe_put(:manifest_path, manifest_path)

    case SQLiteBackup.write(bundle_path, attrs) do
      {:ok, manifest} ->
        target = manifest_path || Manifest.manifest_path_for_bundle(bundle_path)
        Mix.shell().info("Backup manifest written to #{target}")

        Mix.shell().info(
          "generation_id=#{manifest["generation_id"]} database_uuid=#{manifest["database_uuid"]}"
        )

        :ok

      {:error, %VialKeeper.Error{} = error} ->
        Mix.raise(
          "cannot write backup manifest: #{error.code} #{error.message} #{inspect(error.details)}"
        )
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp usage do
    "mix vialkeeper.backup.manifest BUNDLE_PATH --source-path PATH [--generation-id UUID] [--manifest-path PATH]"
  end
end
