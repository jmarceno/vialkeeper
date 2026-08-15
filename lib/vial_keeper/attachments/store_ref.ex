defmodule VialKeeper.Attachments.StoreRef do
  @moduledoc "Storage-neutral attachment location and capability reference."

  alias VialKeeper.DatabaseBundle
  alias VialKeeper.Error
  alias VialKeeper.PathSafety

  @enforce_keys [:mode, :blobs_path, :tmp_path, :allowed_roots]
  defstruct [:mode, :bundle_root, :blobs_path, :tmp_path, :allowed_roots]

  @type mode :: :bundle_local | :external_read_only
  @type t :: %__MODULE__{
          mode: mode(),
          bundle_root: binary() | nil,
          blobs_path: binary(),
          tmp_path: binary() | nil,
          allowed_roots: [binary()]
        }

  @doc "Creates a read/write reference for a validated database bundle."
  @spec bundle_local(DatabaseBundle.t() | binary()) :: t()
  def bundle_local(%DatabaseBundle{} = bundle) do
    %__MODULE__{
      mode: :bundle_local,
      bundle_root: bundle.root,
      blobs_path: bundle.blobs_path,
      tmp_path: bundle.tmp_path,
      allowed_roots: [bundle.root]
    }
  end

  def bundle_local(root) when is_binary(root) do
    root = Path.expand(root)

    bundle_local(%DatabaseBundle{
      root: root,
      blobs_path: Path.join(root, "blobs"),
      tmp_path: Path.join(root, "tmp")
    })
  end

  @doc "Creates an external CAS reference that has no write or temporary-file capability."
  @spec external_read_only(binary(), [binary()]) :: t()
  def external_read_only(blobs_path, allowed_roots)
      when is_binary(blobs_path) and is_list(allowed_roots) do
    %__MODULE__{
      mode: :external_read_only,
      bundle_root: nil,
      blobs_path: Path.expand(blobs_path),
      tmp_path: nil,
      allowed_roots: Enum.map(allowed_roots, &Path.expand/1)
    }
  end

  @doc "Normalizes a legacy bundle root or validates a store reference before I/O."
  @spec normalize(t() | binary()) :: {:ok, t()} | {:error, Error.t()}
  def normalize(%__MODULE__{} = ref), do: validate(ref)
  def normalize(root) when is_binary(root), do: validate(bundle_local(root))
  def normalize(_), do: {:error, Error.invalid_request("attachment store reference is invalid")}

  @doc "Returns whether the reference permits mutation."
  @spec writable?(t()) :: boolean()
  def writable?(%__MODULE__{mode: :bundle_local}), do: true
  def writable?(%__MODULE__{mode: :external_read_only}), do: false

  @doc "Rejects operations that require write or temporary-file capability."
  @spec ensure_writable(t()) :: :ok | {:error, Error.t()}
  def ensure_writable(%__MODULE__{mode: :bundle_local}), do: :ok

  def ensure_writable(%__MODULE__{mode: :external_read_only}),
    do: {:error, Error.shadow_attachment_store_read_only("external attachment store is read-only")}

  @doc "Returns true when the path is within one of the allowed canonical roots."
  @spec within_allowed_root?(t(), binary()) :: boolean()
  def within_allowed_root?(%__MODULE__{allowed_roots: roots}, path) do
    Enum.any?(roots, &PathSafety.within_root?(path, &1))
  end

  defp validate(%__MODULE__{mode: :bundle_local, bundle_root: root} = ref)
       when is_binary(root) do
    if PathSafety.no_symlink_components?(root) and
         PathSafety.within_root?(ref.blobs_path, root) and
         PathSafety.within_root?(ref.tmp_path, root) do
      {:ok, ref}
    else
      {:error, Error.integrity_violation("bundle attachment paths escape the bundle root")}
    end
  end

  defp validate(
         %__MODULE__{
           mode: :external_read_only,
           blobs_path: blobs_path,
           tmp_path: nil,
           allowed_roots: roots
         } = ref
       ) do
    cond do
      Path.type(blobs_path) != :absolute or blobs_path == "" ->
        {:error, Error.invalid_request("external attachment path must be absolute")}

      Path.basename(blobs_path) != "blobs" ->
        {:error, Error.invalid_request("external attachment path must name the blobs directory")}

      roots == [] or not Enum.all?(roots, &(Path.type(&1) == :absolute)) ->
        {:error, Error.invalid_request("external attachment roots are invalid")}

      not within_allowed_root?(ref, blobs_path) ->
        {:error, Error.invalid_request("external attachment path is outside allowed roots")}

      not PathSafety.no_symlink_components?(blobs_path) ->
        {:error, Error.integrity_violation("external attachment path contains a symlink")}

      true ->
        {:ok, ref}
    end
  end

  defp validate(_), do: {:error, Error.invalid_request("attachment store reference is invalid")}
end
