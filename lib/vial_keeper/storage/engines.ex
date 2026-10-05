defmodule VialKeeper.Storage.Engines do
  @moduledoc """
  Storage engines a host can select with `[storage].engine` in `host.toml`.

  This is the one place that maps an engine name to its physical backend
  module; runtime code still resolves the configured backend through
  `VialKeeper.Storage.Registry`. It also tells offline tooling (backup
  manifests) which engine artifact a closed bundle holds.
  """

  @engines %{
    "turso" => VialKeeper.Storage.Turso.Adapter,
    "sqlite" => VialKeeper.Storage.SQLite.Adapter
  }
  @default "turso"

  @doc "Engine name a host runs when its configuration names none."
  @spec default() :: binary()
  def default, do: @default

  @doc "Engine names a host may select, sorted."
  @spec names() :: [binary()]
  def names, do: @engines |> Map.keys() |> Enum.sort()

  @doc "Returns the backend module for a host-selected engine name."
  @spec backend(term()) :: {:ok, module()} | :error
  def backend(name) when is_binary(name), do: Map.fetch(@engines, name)
  def backend(_name), do: :error

  @doc "Every engine's connection driver (`VialKeeper.Storage.SQLite.Driver`)."
  @spec drivers() :: [module()]
  def drivers, do: [VialKeeper.Storage.Turso.Driver, VialKeeper.Storage.SQLite.Driver.Rusqlite]

  @doc "Data artifact file names of every engine."
  @spec artifact_names() :: [binary()]
  def artifact_names, do: Enum.map(drivers(), & &1.artifact_name())

  @doc """
  Returns the data artifact file name present in `bundle_path`, or `:error`
  when the bundle holds no engine artifact.
  """
  @spec bundle_artifact(binary()) :: {:ok, binary()} | :error
  def bundle_artifact(bundle_path) when is_binary(bundle_path) do
    case Enum.find(artifact_names(), &File.regular?(Path.join(bundle_path, &1))) do
      nil -> :error
      name -> {:ok, name}
    end
  end

  @doc "Engine sidecar files present in `bundle_path`; a closed bundle has none."
  @spec hot_sidecars(binary()) :: [binary()]
  def hot_sidecars(bundle_path) when is_binary(bundle_path) do
    for driver <- drivers(),
        suffix <- driver.sidecar_suffixes(),
        path = Path.join(bundle_path, driver.artifact_name() <> suffix),
        File.exists?(path),
        do: path
  end
end
