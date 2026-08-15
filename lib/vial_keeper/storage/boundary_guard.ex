defmodule VialKeeper.Storage.BoundaryGuard do
  @moduledoc """
  Static physical-reference guard for the storage boundary.

  Scans product source and documentation for SQLite/Exqlite/SQL physical markers outside
  `VialKeeper.Storage.PhysicalAllowlist`. Findings are returned to the caller so
  every exception is explicit and reviewable.
  """

  alias VialKeeper.Storage.PhysicalAllowlist

  @type finding :: %{
          path: binary(),
          line: pos_integer(),
          pattern: atom(),
          excerpt: binary()
        }

  @patterns [
    {:storage_sqlite, ~r/\bVialKeeper\.Storage\.SQLite\b|\bStorage\.SQLite\b/},
    {:exqlite, ~r/\bExqlite\b/},
    {:database_sqlite3, ~r/database\.sqlite3/},
    {:sqlite_path, ~r/\bsqlite_path\b/},
    {:validate_sqlite, ~r/\bvalidate_sqlite!?\b/},
    {:transaction_sql,
     ~r/["']\s*(?:BEGIN(?:\s+(?:EXCLUSIVE|IMMEDIATE|DEFERRED))?|COMMIT|ROLLBACK)\s*["']/i},
    {:pragma, ~r/\bPRAGMA\s+[A-Za-z_]/i},
    {:sql_string,
     ~r/(?:^\s*|["'])\s*(?:SELECT\s+.+\s+FROM|INSERT\s+INTO|UPDATE\s+[A-Za-z_][\w.]*\s+SET|DELETE\s+FROM|CREATE\s+(?:TABLE|INDEX|VIRTUAL|TRIGGER)|DROP\s+(?:TABLE|INDEX|TRIGGER)|ALTER\s+TABLE)\b/i}
  ]

  @scan_roots ["lib", "bench", "priv"]
  @scan_extensions ~w(.ex .exs .sql .md .toml)

  @doc "Approved physical marker patterns scanned by the guard."
  @spec patterns() :: [{atom(), Regex.t()}]
  def patterns, do: @patterns

  @doc """
  Scans product source and documentation for physical markers outside the
  allowlist.

  Returns findings sorted by path and line. Product documentation at the repo
  root (`README.md`, `Operations.md`) is included so product-model leaks are
  checked together with source code. Tests are intentionally excluded: physical
  test coverage is classified separately by `PhysicalAllowlist`.
  """
  @spec scan(binary()) :: [finding()]
  def scan(root \\ File.cwd!()) when is_binary(root) do
    root
    |> scan_targets()
    |> Enum.flat_map(&scan_file(root, &1))
    |> Enum.sort_by(&{&1.path, &1.line, &1.pattern})
  end

  @doc "Returns findings whose relative path equals `relative_path`."
  @spec findings_for([finding()], binary()) :: [finding()]
  def findings_for(findings, relative_path) when is_list(findings) and is_binary(relative_path) do
    Enum.filter(findings, &(&1.path == relative_path))
  end

  @doc "Returns unique relative paths that contain findings."
  @spec leaking_paths([finding()]) :: [binary()]
  def leaking_paths(findings) when is_list(findings) do
    findings |> Enum.map(& &1.path) |> Enum.uniq() |> Enum.sort()
  end

  defp scan_targets(root) do
    rooted =
      @scan_roots
      |> Enum.map(&Path.join(root, &1))
      |> Enum.filter(&File.dir?/1)
      |> Enum.flat_map(&list_files/1)

    docs =
      for name <- ["README.md", "Operations.md"],
          path = Path.join(root, name),
          File.regular?(path),
          do: path

    Enum.uniq(rooted ++ docs)
  end

  defp list_files(dir) do
    dir
    |> File.ls!()
    |> Enum.flat_map(fn name ->
      path = Path.join(dir, name)

      cond do
        File.dir?(path) -> list_files(path)
        File.regular?(path) and allowed_extension?(path) -> [path]
        true -> []
      end
    end)
  end

  defp allowed_extension?(path) do
    Path.extname(path) in @scan_extensions
  end

  defp scan_file(root, absolute_path) do
    relative = Path.relative_to(absolute_path, root)

    if PhysicalAllowlist.allowed_path?(relative) do
      []
    else
      scan_disallowed_file(absolute_path, relative)
    end
  end

  defp scan_disallowed_file(absolute_path, relative) do
    absolute_path
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.flat_map(&line_findings(relative, &1))
  end

  defp line_findings(relative, {line, line_no}) do
    for {pattern, regex} <- @patterns,
        Regex.match?(regex, line) do
      %{
        path: relative,
        line: line_no,
        pattern: pattern,
        excerpt: String.trim(line)
      }
    end
  end
end
