defmodule VialKeeper.Bench.Root do
  @moduledoc """
  Two-sided external benchmark-root identity and path containment.

  Dataset bytes, generated databases, staging, caches, and reports never live
  in the repository. Every data-backed command verifies a repo-local pointer
  against a destination marker under a canonical descendant of
  `/mnt/other/downloads/` before any network request or large write.
  """

  alias VialKeeper.AtomicWrite
  alias VialKeeper.Bench.DiskSpace
  alias VialKeeper.PathSafety
  alias VialKeeper.UUID

  @approved_parent "/mnt/other/downloads"
  @default_root "/mnt/other/downloads/vialkeeper"
  @schema_version 1
  @project "vialkeeper"
  @pointer_basename ".vialkeeper-bench-root"
  @marker_basename ".vialkeeper-bench-root.json"
  @layout_dirs ["datasets", "staging", "work", "cache", "reports"]
  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @type t :: %__MODULE__{
          root: binary(),
          root_id: binary(),
          pointer_path: binary(),
          marker_path: binary(),
          repo_root: binary(),
          approved_parent: binary()
        }

  defstruct [
    :root,
    :root_id,
    :pointer_path,
    :marker_path,
    :repo_root,
    :approved_parent
  ]

  @doc "Hard-coded approved parent used by production commands."
  @spec approved_parent() :: binary()
  def approved_parent, do: @approved_parent

  @doc "Fixed default root used by self-contained dataset benchmark runners."
  @spec default_root() :: binary()
  def default_root, do: @default_root

  @spec pointer_basename() :: binary()
  def pointer_basename, do: @pointer_basename

  @spec marker_basename() :: binary()
  def marker_basename, do: @marker_basename

  @spec layout_dirs() :: [binary()]
  def layout_dirs, do: @layout_dirs

  @doc """
  Creates or adopts a benchmark root.

  `opts` may inject `:approved_parent` and `:repo_root` for tests. Production
  Mix commands must not pass those keys and must not read them from the
  environment or extra CLI flags.
  """
  @spec configure(binary(), keyword()) :: {:ok, t()} | {:error, binary()}
  def configure(root, opts \\ []) when is_binary(root) and is_list(opts) do
    with {:ok, env} <- environment(opts),
         :ok <- reject_relative(root),
         :ok <- reject_dotdot(root),
         {:ok, canonical} <- canonicalize_existing_prefix(root),
         :ok <- reject_forbidden_locations(canonical, env),
         :ok <- ensure_writable_location(canonical),
         :ok <- verify_free_space_query(canonical, opts),
         {:ok, root_id} <- adopt_or_initialize(canonical, env, opts) do
      context = build_context(canonical, root_id, env)
      :ok = ensure_layout(context)
      {:ok, context}
    end
  end

  @doc "Loads and verifies both sides of the root identity."
  @spec load(keyword()) :: {:ok, t()} | {:error, binary()}
  def load(opts \\ []) when is_list(opts) do
    with {:ok, env} <- environment(opts),
         {:ok, pointer} <- read_pointer(env.pointer_path),
         :ok <- reject_relative(pointer["root"]),
         :ok <- reject_dotdot(pointer["root"]),
         {:ok, canonical} <- canonicalize_existing_prefix(pointer["root"]),
         :ok <- reject_forbidden_locations(canonical, env),
         :ok <- same_path(canonical, pointer["root"]),
         context <- build_context(canonical, pointer["root_id"], env),
         :ok <- verify_marker(context),
         :ok <- verify_free_space_query(canonical, opts) do
      {:ok, context}
    end
  end

  @doc "Loads the configured root or initializes the fixed default benchmark root."
  @spec load_or_configure(keyword()) :: {:ok, t()} | {:error, binary()}
  def load_or_configure(opts \\ []) when is_list(opts) do
    case load(opts) do
      {:ok, _context} = ok ->
        ok

      {:error, "benchmark root is not configured; run mix bench.data configure --root PATH"} ->
        configure(@default_root, Keyword.put(opts, :reuse_existing, true))

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Resolves a relative path under the verified root."
  @spec resolve(t(), binary() | [binary()]) :: {:ok, binary()} | {:error, binary()}
  def resolve(%__MODULE__{} = context, relative) when is_binary(relative) do
    resolve(context, Path.split(relative))
  end

  def resolve(%__MODULE__{} = context, parts) when is_list(parts) do
    with :ok <- validate_segments(parts),
         joined <- Path.join([context.root | parts]),
         :ok <- reject_dotdot(joined),
         {:ok, expanded} <- expand_without_escape(joined),
         :ok <- assert_within(expanded, context.root),
         :ok <- reject_symlinks(expanded) do
      {:ok, expanded}
    end
  end

  @spec datasets_dir(t()) :: {:ok, binary()} | {:error, binary()}
  def datasets_dir(context), do: resolve(context, "datasets")

  @spec staging_dir(t()) :: {:ok, binary()} | {:error, binary()}
  def staging_dir(context), do: resolve(context, "staging")

  @spec work_dir(t()) :: {:ok, binary()} | {:error, binary()}
  def work_dir(context), do: resolve(context, "work")

  @spec cache_dir(t()) :: {:ok, binary()} | {:error, binary()}
  def cache_dir(context), do: resolve(context, "cache")

  @spec reports_dir(t()) :: {:ok, binary()} | {:error, binary()}
  def reports_dir(context), do: resolve(context, "reports")

  @spec dataset_path(t(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def dataset_path(context, name, version) when is_binary(name) and is_binary(version) do
    resolve(context, ["datasets", name, version])
  end

  @spec staging_path(t(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def staging_path(context, name, unique_id) when is_binary(name) and is_binary(unique_id) do
    resolve(context, ["staging", name <> "-" <> unique_id])
  end

  @spec work_run_path(t(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def work_run_path(context, benchmark, run_id)
      when is_binary(benchmark) and is_binary(run_id) do
    resolve(context, ["work", benchmark, run_id])
  end

  @spec cache_path(t(), binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def cache_path(context, name, version) when is_binary(name) and is_binary(version) do
    resolve(context, ["cache", name, version])
  end

  @spec report_path(t(), binary()) :: {:ok, binary()} | {:error, binary()}
  def report_path(context, filename) when is_binary(filename) do
    resolve(context, ["reports", filename])
  end

  @doc """
  Recursively deletes one verified dataset directory.

  Refuses to delete the benchmark root, anything outside `datasets/`, or an
  unknown dataset name.
  """
  @spec remove_dataset!(t(), binary(), binary()) :: :ok | {:error, binary()}
  def remove_dataset!(context, name, version) when is_binary(name) and is_binary(version) do
    with {:ok, datasets} <- datasets_dir(context),
         {:ok, path} <- dataset_path(context, name, version),
         :ok <- assert_strict_descendant(path, datasets),
         :ok <- reject_symlinks(path) do
      case File.rm_rf(path) do
        {:ok, _} -> :ok
        {:error, reason, _} -> {:error, "failed to remove dataset #{name}: #{inspect(reason)}"}
      end
    end
  end

  @doc "Recursively deletes one verified work-run directory."
  @spec remove_work_run!(t(), binary()) :: :ok | {:error, binary()}
  def remove_work_run!(context, path) when is_binary(path) do
    with {:ok, work} <- work_dir(context),
         {:ok, expanded} <- expand_without_escape(path),
         :ok <- assert_strict_descendant(expanded, work),
         :ok <- reject_symlinks(expanded) do
      case File.rm_rf(expanded) do
        {:ok, _} -> :ok
        {:error, reason, _} -> {:error, "failed to remove work directory: #{inspect(reason)}"}
      end
    end
  end

  @spec descendant?(binary(), binary()) :: boolean()
  def descendant?(path, root) when is_binary(path) and is_binary(root) do
    path = Path.expand(path)
    root = String.trim_trailing(Path.expand(root), "/")
    path != root and String.starts_with?(path, root <> "/")
  end

  defp environment(opts) do
    approved = Keyword.get(opts, :approved_parent, @approved_parent)

    with :ok <- validate_approved_parent(approved),
         repo_root <- detect_repo_root(opts),
         pointer_path <- Keyword.get(opts, :pointer_path, Path.join(repo_root, @pointer_basename)) do
      {:ok,
       %{
         approved_parent: Path.expand(approved),
         repo_root: Path.expand(repo_root),
         pointer_path: Path.expand(pointer_path)
       }}
    end
  end

  defp validate_approved_parent(parent) when is_binary(parent) do
    expanded = Path.expand(parent)

    cond do
      Path.type(parent) != :absolute ->
        {:error, "approved parent must be an absolute path"}

      expanded != String.trim_trailing(expanded, "/") ->
        {:error, "approved parent is invalid"}

      true ->
        :ok
    end
  end

  defp validate_approved_parent(_), do: {:error, "approved parent must be an absolute path"}

  defp detect_repo_root(opts) do
    cond do
      is_binary(opts[:repo_root]) ->
        opts[:repo_root]

      function_exported?(Mix.Project, :project_file, 0) and Mix.Project.project_file() ->
        Mix.Project.project_file() |> Path.dirname()

      true ->
        File.cwd!()
    end
  end

  defp reject_relative(path) when is_binary(path) do
    if Path.type(path) == :absolute do
      :ok
    else
      {:error, "benchmark root must be an absolute path"}
    end
  end

  defp reject_relative(_), do: {:error, "benchmark root must be an absolute path"}

  defp reject_dotdot(path) when is_binary(path) do
    if Enum.any?(Path.split(path), &(&1 in ["..", "."])) do
      {:error, "benchmark paths must not contain '.' or '..' segments"}
    else
      :ok
    end
  end

  defp canonicalize_existing_prefix(path) do
    expanded = Path.expand(path)

    with :ok <- reject_symlinks(expanded) do
      {:ok, expanded}
    end
  end

  defp reject_forbidden_locations(canonical, env) do
    approved = String.trim_trailing(env.approved_parent, "/")
    repo = String.trim_trailing(env.repo_root, "/")
    locs = %{approved: approved, repo: repo, home: home_dir(), tmp: tmp_dir(), cwd: cwd_dir()}

    with :ok <- require_approved_descendant(canonical, locs.approved),
         :ok <- reject_inside_repo(canonical, locs.repo),
         :ok <- reject_contains_repo(canonical, locs.repo),
         :ok <- reject_tmp(canonical, locs),
         :ok <- reject_home(canonical, locs) do
      reject_cwd(canonical, locs)
    end
  end

  defp require_approved_descendant(canonical, approved) do
    if descendant?(canonical, approved) do
      :ok
    else
      {:error, "benchmark root must be a canonical descendant of #{approved}/ (got #{canonical})"}
    end
  end

  defp reject_inside_repo(canonical, repo) do
    if canonical == repo or descendant?(canonical, repo) do
      {:error, "benchmark root must not be inside the VialKeeper repository"}
    else
      :ok
    end
  end

  defp reject_contains_repo(canonical, repo) do
    if descendant?(repo, canonical) do
      {:error, "benchmark root must not contain the VialKeeper repository"}
    else
      :ok
    end
  end

  defp reject_tmp(canonical, locs) do
    if production_parent?(locs.approved) and
         (canonical == locs.tmp or descendant?(canonical, locs.tmp)) do
      {:error, "benchmark root must not be under /tmp or the system temporary directory"}
    else
      :ok
    end
  end

  defp reject_home(canonical, locs) do
    if production_parent?(locs.approved) and is_binary(locs.home) and
         (canonical == locs.home or descendant?(canonical, locs.home)) do
      {:error, "benchmark root must not be under the home directory"}
    else
      :ok
    end
  end

  defp reject_cwd(canonical, locs) do
    if production_parent?(locs.approved) and canonical == locs.cwd do
      {:error, "benchmark root must not be the current working directory"}
    else
      :ok
    end
  end

  defp tmp_dir, do: String.trim_trailing(Path.expand(System.tmp_dir!()), "/")
  defp cwd_dir, do: String.trim_trailing(Path.expand(File.cwd!()), "/")

  defp production_parent?(approved) do
    String.trim_trailing(approved, "/") == @approved_parent
  end

  defp home_dir do
    case System.get_env("HOME") do
      path when is_binary(path) and path != "" -> String.trim_trailing(Path.expand(path), "/")
      _ -> nil
    end
  end

  defp reject_symlinks(path) do
    if PathSafety.no_symlink_components?(path) do
      :ok
    else
      {:error, "benchmark paths must not contain symlink components"}
    end
  end

  defp ensure_writable_location(path) do
    parent = existing_ancestor(path)

    case File.stat(parent) do
      {:ok, %File.Stat{access: access}} when access in [:read_write, :write] ->
        :ok

      {:ok, _} ->
        probe_write(parent)

      {:error, reason} ->
        {:error, "benchmark root parent is not accessible: #{inspect(reason)}"}
    end
  end

  defp probe_write(dir) do
    probe = Path.join(dir, ".vialkeeper-bench-write-probe-#{System.unique_integer([:positive])}")

    case File.write(probe, "ok") do
      :ok ->
        _ = File.rm(probe)
        :ok

      {:error, reason} ->
        {:error, "benchmark root is not writable: #{inspect(reason)}"}
    end
  end

  defp existing_ancestor(path) do
    if File.exists?(path) do
      path
    else
      existing_ancestor(Path.dirname(path))
    end
  end

  defp verify_free_space_query(path, opts) do
    fun = Keyword.get(opts, :available_bytes_fun, &DiskSpace.available_bytes/1)
    ancestor = existing_ancestor(path)

    case fun.(ancestor) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 ->
        :ok

      {:error, reason} ->
        {:error, "could not query free space for #{ancestor}: #{format_reason(reason)}"}
    end
  end

  defp adopt_or_initialize(canonical, env, opts) do
    marker = Path.join(canonical, @marker_basename)

    cond do
      File.exists?(marker) ->
        reuse_existing(canonical, marker, env, opts)

      File.exists?(canonical) ->
        case empty_directory?(canonical) do
          true -> initialize_new(canonical, env)
          false -> {:error, "refusing to adopt non-empty unmarked directory #{canonical}"}
          {:error, reason} -> {:error, reason}
        end

      true ->
        initialize_new(canonical, env)
    end
  end

  defp reuse_existing(canonical, marker, env, opts) do
    if Keyword.get(opts, :reuse_existing, false) do
      with {:ok, payload} <- read_json(marker),
           :ok <- validate_marker_payload(payload),
           :ok <- write_pointer(env.pointer_path, canonical, payload["root_id"]) do
        {:ok, payload["root_id"]}
      end
    else
      {:error,
       "directory #{canonical} is already a VialKeeper benchmark root; pass --reuse-existing"}
    end
  end

  defp initialize_new(canonical, env) do
    root_id = UUID.v4()

    with :ok <- File.mkdir_p(canonical),
         :ok <- reject_symlinks(canonical),
         :ok <-
           write_marker(Path.join(canonical, @marker_basename), root_id),
         :ok <- write_pointer(env.pointer_path, canonical, root_id) do
      {:ok, root_id}
    end
  end

  defp empty_directory?(path) do
    case File.ls(path) do
      {:ok, []} -> true
      {:ok, _} -> false
      {:error, :enoent} -> true
      {:error, reason} -> {:error, "cannot list #{path}: #{inspect(reason)}"}
    end
  end

  defp write_marker(path, root_id) do
    payload = %{
      "schema_version" => @schema_version,
      "project" => @project,
      "root_id" => root_id
    }

    write_json(path, payload)
  end

  defp write_pointer(path, root, root_id) do
    payload = %{
      "schema_version" => @schema_version,
      "root" => root,
      "root_id" => root_id
    }

    write_json(path, payload)
  end

  defp write_json(path, payload) do
    case AtomicWrite.write(path, JSON.encode!(payload) <> "\n") do
      :ok -> :ok
      {:error, reason} -> {:error, "failed to write #{path}: #{inspect(reason)}"}
    end
  end

  defp read_pointer(path) do
    with {:ok, payload} <- read_json(path),
         :ok <- validate_pointer_payload(payload) do
      {:ok, payload}
    else
      {:error, :enoent} ->
        {:error, "benchmark root is not configured; run mix bench.data configure --root PATH"}

      other ->
        other
    end
  end

  defp read_json(path) do
    case File.read(path) do
      {:ok, contents} ->
        case JSON.decode(contents) do
          {:ok, payload} when is_map(payload) -> {:ok, payload}
          {:ok, _} -> {:error, "#{path} is not a JSON object"}
          {:error, _} -> {:error, "#{path} is not valid JSON"}
        end

      {:error, :enoent} ->
        {:error, :enoent}

      {:error, reason} ->
        {:error, "failed to read #{path}: #{inspect(reason)}"}
    end
  end

  defp validate_pointer_payload(payload) do
    with :ok <- require_schema(payload),
         :ok <- require_string(payload, "root") do
      require_uuid(payload, "root_id")
    end
  end

  defp validate_marker_payload(payload) do
    with :ok <- require_schema(payload),
         :ok <- require_uuid(payload, "root_id") do
      case payload["project"] do
        @project -> :ok
        other -> {:error, "benchmark marker project is #{inspect(other)}, expected #{@project}"}
      end
    end
  end

  defp require_schema(payload) do
    case payload["schema_version"] do
      @schema_version -> :ok
      other -> {:error, "unsupported benchmark root schema_version #{inspect(other)}"}
    end
  end

  defp require_string(payload, key) do
    case payload[key] do
      value when is_binary(value) and value != "" -> :ok
      _ -> {:error, "benchmark identity is missing #{key}"}
    end
  end

  defp require_uuid(payload, key) do
    case payload[key] do
      value when is_binary(value) ->
        if Regex.match?(@uuid_pattern, value) do
          :ok
        else
          {:error, "benchmark #{key} is not a UUID"}
        end

      _ ->
        {:error, "benchmark identity is missing #{key}"}
    end
  end

  defp verify_marker(context) do
    with {:ok, payload} <- read_marker(context),
         :ok <- validate_marker_payload(payload) do
      matching_root_id(payload, context.root_id)
    end
  end

  defp read_marker(context) do
    case read_json(context.marker_path) do
      {:ok, payload} -> {:ok, payload}
      {:error, :enoent} -> {:error, "benchmark root marker is missing at #{context.marker_path}"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp matching_root_id(payload, root_id) do
    if payload["root_id"] == root_id do
      :ok
    else
      {:error, "benchmark root marker UUID does not match the repo pointer"}
    end
  end

  defp same_path(canonical, stored) do
    if Path.expand(stored) == canonical do
      :ok
    else
      {:error, "stale benchmark pointer: stored #{stored}, canonical #{canonical}"}
    end
  end

  defp build_context(canonical, root_id, env) do
    %__MODULE__{
      root: canonical,
      root_id: root_id,
      pointer_path: env.pointer_path,
      marker_path: Path.join(canonical, @marker_basename),
      repo_root: env.repo_root,
      approved_parent: env.approved_parent
    }
  end

  defp ensure_layout(context) do
    Enum.reduce_while(@layout_dirs, :ok, fn dir, :ok ->
      mkdir_layout_dir(context, dir)
    end)
  end

  defp mkdir_layout_dir(context, dir) do
    case resolve(context, dir) do
      {:ok, path} -> mkdir_or_halt(path)
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp mkdir_or_halt(path) do
    case File.mkdir_p(path) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, "failed to create #{path}: #{inspect(reason)}"}}
    end
  end

  defp validate_segments(parts) do
    Enum.reduce_while(parts, :ok, fn part, :ok ->
      cond do
        not is_binary(part) or part == "" ->
          {:halt, {:error, "benchmark path segment is empty"}}

        part in ["/", "..", "."] ->
          {:halt, {:error, "benchmark path segment is illegal: #{inspect(part)}"}}

        Path.type(part) == :absolute ->
          {:halt, {:error, "benchmark path segment must not be absolute"}}

        String.contains?(part, "\\") ->
          {:halt, {:error, "benchmark path segment contains a backslash"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp expand_without_escape(path) do
    expanded = Path.expand(path)

    if Enum.any?(Path.split(path), &(&1 in ["..", "."])) do
      {:error, "benchmark paths must not contain '.' or '..' segments"}
    else
      {:ok, expanded}
    end
  end

  defp assert_within(path, root) do
    root = Path.expand(root)

    if path == root or descendant?(path, root) do
      :ok
    else
      {:error, "path #{path} escapes benchmark root #{root}"}
    end
  end

  defp assert_strict_descendant(path, root) do
    if descendant?(path, root) do
      :ok
    else
      {:error, "path #{path} is not a descendant of #{root}"}
    end
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(reason), do: inspect(reason)
end
