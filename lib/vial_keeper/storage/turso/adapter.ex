defmodule VialKeeper.Storage.Turso.Adapter do
  @moduledoc """
  Turso storage backend, the default engine.

  Turso speaks the SQLite dialect, so this backend is the SQLite adapter and
  its SQL modules running on `VialKeeper.Storage.Turso.Driver`. Open adapters
  are `VialKeeper.Storage.SQLite.Adapter` structs whose `driver` is the Turso
  driver; every port reads the driver from the struct. This module only owns
  what selects the engine: create and open, the artifact name, the ownership
  lease path and the capability probe. Every other function delegates.

  Bundles are engine-specific: a bundle holding a SQLite artifact fails to
  open here with `unsupported_format`, and the other way round.
  """
  @behaviour VialKeeper.Storage.Adapter

  alias VialKeeper.Storage.SQLite.Adapter, as: SQLiteAdapter
  alias VialKeeper.Storage.Turso.Driver

  # Engine selection stays here; everything else works on an open adapter of
  # either engine and delegates. The delegated set is the adapter behaviour
  # plus the SQLite adapter's other public entry points.
  @engine_owned [create: 2, open: 2]
  @extra [
    apply_bulk_mutation: 2,
    apply_local_mutation: 2,
    capabilities_report: 1,
    cleanup_expired_pending_blobs: 1,
    compact_retention: 1,
    diff_revisions: 2,
    get_revision_chains: 2,
    import_revision_chains: 2,
    integrity_check: 1,
    interrupt_reader: 1,
    open_reader: 1,
    open_writer: 1,
    port: 1,
    port_modules: 0,
    read_change_page: 4,
    reset_writer_caches: 1,
    resolve_conflict: 2,
    run_transaction: 2,
    stored_identity: 1,
    to_context: 1,
    transaction_port: 0,
    writer_capabilities: 1
  ]

  @delegated (VialKeeper.Storage.Adapter.behaviour_info(:callbacks) -- @engine_owned) ++ @extra

  for {name, arity} <- @delegated do
    args = Macro.generate_arguments(arity, __MODULE__)
    defdelegate unquote(name)(unquote_splicing(args)), to: SQLiteAdapter
  end

  @doc "Creates a Turso database at `path`."
  @spec create(binary(), map()) :: {:ok, SQLiteAdapter.t()} | {:error, VialKeeper.Error.t()}
  def create(path, options \\ %{}), do: SQLiteAdapter.create(path, options, Driver)

  @doc "Opens the Turso database at `path`."
  @spec open(binary(), map()) :: {:ok, SQLiteAdapter.t()} | {:error, VialKeeper.Error.t()}
  def open(path, _options \\ %{}), do: SQLiteAdapter.open_with_driver(path, Driver)

  @doc "Returns the Turso data artifact path inside a bundle root."
  @spec artifact_path(binary()) :: binary()
  def artifact_path(bundle_root) when is_binary(bundle_root),
    do: SQLiteAdapter.artifact_path(bundle_root, Driver)

  @doc "Starts the ownership lease for the Turso artifact under `bundle_root`."
  @spec start_ownership(binary()) :: GenServer.on_start()
  def start_ownership(bundle_root) when is_binary(bundle_root),
    do: SQLiteAdapter.start_ownership(bundle_root, Driver)

  @doc "Validates required Turso runtime capabilities."
  @spec validate_capabilities!() :: binary()
  def validate_capabilities!, do: Driver.validate_capabilities!()

  @doc "Returns opaque Turso capability metadata."
  @spec capabilities_report() :: map()
  def capabilities_report, do: Driver.capabilities_report()
end
