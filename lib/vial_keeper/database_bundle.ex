defmodule VialKeeper.DatabaseBundle do
  @moduledoc """
  Validated portable database bundle path contract.

  The portable database unit is a closed `.vialkeeper` directory containing
  backend-owned data, `blobs/`, and `tmp/`. Physical data artifacts are owned
  by the selected storage backend, not by this generic container.
  """

  alias VialKeeper.PathSafety

  @enforce_keys [:root, :blobs_path, :tmp_path]
  defstruct [:root, :blobs_path, :tmp_path]

  @type t :: %__MODULE__{
          root: binary(),
          blobs_path: binary(),
          tmp_path: binary()
        }

  @blobs_dir "blobs"
  @tmp_dir "tmp"

  # Architecture §attachment upload: pending protection lasts 24 hours; abandoned
  # tmp files older than this cutoff are removed on bundle open.
  @pending_tmp_expiry_ms 86_400_000

  @doc "Creates a new bundle directory tree at `root`."
  @spec create(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def create(root) when is_binary(root) do
    with {:ok, bundle} <- validate_root_path(root),
         :ok <- mkdir(bundle.root),
         :ok <- mkdir(bundle.blobs_path),
         :ok <- mkdir(bundle.tmp_path) do
      {:ok, bundle}
    end
  end

  @doc "Validates an existing bundle directory."
  @spec open(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def open(root) when is_binary(root), do: validate(root)

  @spec validate(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def validate(root) when is_binary(root) do
    with {:ok, bundle} <- validate_root_path(root),
         :ok <- ensure_directory(bundle.root, bundle.root),
         :ok <- ensure_directory(bundle.blobs_path, bundle.root),
         :ok <- ensure_directory(bundle.tmp_path, bundle.root) do
      {:ok, bundle}
    end
  end

  @doc """
  Prepares an existing bundle for runtime open.

  Ensures `blobs/` and `tmp/` exist, rejects symlink escapes, and removes
  abandoned temporary files older than #{@pending_tmp_expiry_ms} ms.
  Backend data validation happens through the selected storage backend.
  """
  @spec prepare_for_open(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def prepare_for_open(root) when is_binary(root) do
    with {:ok, bundle} <- validate_root_path(root),
         :ok <- ensure_directory(bundle.root, bundle.root),
         :ok <- ensure_or_create_directory(bundle.blobs_path, bundle.root),
         :ok <- ensure_or_create_directory(bundle.tmp_path, bundle.root),
         :ok <- cleanup_abandoned_tmp(bundle) do
      {:ok, bundle}
    end
  end

  @spec blobs_path(t()) :: binary()
  def blobs_path(%__MODULE__{blobs_path: path}), do: path

  @spec tmp_path(t()) :: binary()
  def tmp_path(%__MODULE__{tmp_path: path}), do: path

  @spec root(t()) :: binary()
  def root(%__MODULE__{root: path}), do: path

  defp validate_root_path(root) do
    abs = Path.expand(root)

    if PathSafety.no_symlink_components?(abs) do
      {:ok,
       %__MODULE__{
         root: abs,
         blobs_path: Path.join(abs, @blobs_dir),
         tmp_path: Path.join(abs, @tmp_dir)
       }}
    else
      {:error, VialKeeper.Error.integrity_violation("bundle path contains symlinks")}
    end
  end

  defp ensure_or_create_directory(path, root) do
    cond do
      File.dir?(path) and PathSafety.no_symlink_components?(path) and
          PathSafety.within_root?(path, root) ->
        :ok

      File.dir?(path) ->
        symlink_escape_error(path)

      File.exists?(path) ->
        {:error, VialKeeper.Error.invalid_request("bundle component must be a directory")}

      true ->
        mkdir(path)
    end
  end

  defp ensure_directory(path, root) do
    cond do
      File.dir?(path) and PathSafety.no_symlink_components?(path) and
          PathSafety.within_root?(path, root) ->
        :ok

      File.dir?(path) and
          (not PathSafety.no_symlink_components?(path) or not PathSafety.within_root?(path, root)) ->
        symlink_escape_error(path)

      File.exists?(path) ->
        {:error, VialKeeper.Error.invalid_request("bundle component must be a directory")}

      true ->
        {:error, VialKeeper.Error.invalid_request("bundle component is missing")}
    end
  end

  defp mkdir(path) do
    case File.mkdir_p(path) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, VialKeeper.Error.internal_error("cannot create bundle path: #{inspect(reason)}")}
    end
  end

  defp symlink_escape_error(path) do
    {:error, VialKeeper.Error.integrity_violation("bundle path escapes root: #{path}")}
  end

  defp cleanup_abandoned_tmp(%__MODULE__{tmp_path: tmp_path}) do
    cutoff_ms = System.system_time(:millisecond) - @pending_tmp_expiry_ms

    case File.ls(tmp_path) do
      {:ok, names} ->
        Enum.each(names, &maybe_remove_stale_tmp(Path.join(tmp_path, &1), cutoff_ms))
        :ok

      {:error, _} ->
        :ok
    end
  end

  defp maybe_remove_stale_tmp(path, cutoff_ms) do
    case File.lstat(path, time: :posix) do
      {:ok, %File.Stat{type: type, mtime: mtime}}
      when type in [:regular, :directory] and is_integer(mtime) ->
        if mtime * 1000 < cutoff_ms, do: File.rm_rf(path)

      _ ->
        :ok
    end
  end
end
