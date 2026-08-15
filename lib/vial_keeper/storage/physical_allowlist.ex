defmodule VialKeeper.Storage.PhysicalAllowlist do
  @moduledoc """
  Approved locations for SQLite/Exqlite physical details.

  Outside this allowlist, product, runtime, domain, and documentation sources
  must not embed engine names, SQL transaction modes, PRAGMA, rowid, or the
  `database.sqlite3` artifact name. Classified physical test and support files
  are listed explicitly.
  """

  @path_prefixes [
    "lib/vial_keeper/storage/sqlite/",
    "priv/sqlite/",
    "test/physical/sqlite/",
    "test/vial_keeper/storage/sqlite/",
    "bench/"
  ]

  @path_files [
    "lib/vial_keeper/observability/instrumentation/sqlite.ex",
    "lib/vial_keeper/storage/boundary_guard.ex",
    "lib/vial_keeper/storage/opaque_handle.ex",
    "lib/vial_keeper/storage/opaque_handle/server.ex",
    "lib/vial_keeper/storage/physical_allowlist.ex",
    "lib/vial_keeper/storage/ports.ex",
    "lib/vial_keeper/storage/sentinel/context.ex",
    "bench/sqlite_exqlite_overhead_benchmark.exs",
    "bench/product_benchmark.exs",
    "bench/README.md",
    "config/config.exs",
    "test/storage/boundary_guard_test.exs"
  ]

  @classified_physical_tests [
    "test/physical/sqlite/portability_test.exs",
    "test/physical/sqlite/storage_mode_test.exs",
    "test/physical/sqlite/v1_conformance_test.exs",
    "test/physical/sqlite/full_text_indexes_test.exs",
    "test/physical/sqlite/integrity_test.exs",
    "test/runtime/file_lease_os_process_test.exs",
    "test/runtime/file_lease_test.exs",
    "test/end_to_end/hot_journal_recovery_test.exs",
    "test/end_to_end/offline_copy_test.exs",
    "test/observability/sqlite_probe_test.exs",
    "test/contract/term_blob_test.exs",
    "test/physical/sqlite/read_pool_connection_test.exs",
    "test/physical/sqlite/read_snapshot_test.exs"
  ]

  @classified_physical_support [
    "test/support/storage/temp_database.ex",
    "test/support/storage/adapter_case.ex"
  ]

  @doc "Path prefixes where physical SQLite/Exqlite details are allowed."
  @spec path_prefixes() :: [binary()]
  def path_prefixes, do: @path_prefixes

  @doc "Exact relative paths that are physically allowed."
  @spec path_files() :: [binary()]
  def path_files, do: @path_files

  @doc """
  Relative test paths classified as SQLite-physical rather than backend-neutral.
  """
  @spec classified_physical_tests() :: [binary()]
  def classified_physical_tests, do: @classified_physical_tests

  @doc "Test support modules that intentionally expose SQLite bundle helpers."
  @spec classified_physical_support() :: [binary()]
  def classified_physical_support, do: @classified_physical_support

  @doc "Returns true when `relative_path` may contain physical SQLite details."
  @spec allowed_path?(binary()) :: boolean()
  def allowed_path?(relative_path) when is_binary(relative_path) do
    normalized = relative_path |> String.replace("\\", "/") |> String.trim_leading("./")

    Enum.any?(@path_files, &(&1 == normalized)) or
      Enum.any?(@path_prefixes, &String.starts_with?(normalized, &1)) or
      Enum.any?(@classified_physical_tests, &(&1 == normalized)) or
      Enum.any?(@classified_physical_support, &(&1 == normalized))
  end
end
