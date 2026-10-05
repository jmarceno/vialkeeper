defmodule VialKeeper.TestBackend do
  @moduledoc """
  The storage backend the suite runs on (`VIALKEEPER_TEST_ENGINE`), shaped
  like `VialKeeper.Storage.SQLite.Adapter`.

  Tests that create or open catalog bundles directly alias this module instead
  of a concrete engine adapter. Create, open and the artifact path go to the
  configured backend; every other function works on an open adapter of either
  SQLite-dialect engine and delegates to the SQLite adapter.
  """

  alias VialKeeper.Storage.Registry
  alias VialKeeper.Storage.SQLite.Adapter, as: SQLiteAdapter

  @engine_owned [
    create: 1,
    create: 2,
    open: 1,
    open: 2,
    artifact_path: 1,
    start_ownership: 1,
    validate_capabilities!: 0,
    capabilities_report: 0
  ]

  for {name, arity} <- SQLiteAdapter.__info__(:functions), {name, arity} not in @engine_owned do
    args = Macro.generate_arguments(arity, __MODULE__)
    defdelegate unquote(name)(unquote_splicing(args)), to: SQLiteAdapter
  end

  for {name, arity} <- @engine_owned do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(name)(unquote_splicing(args)),
      do: Registry.backend().unquote(name)(unquote_splicing(args))
  end

  @doc "The connection driver of the configured engine."
  @spec driver() :: module()
  def driver do
    case Registry.backend() do
      VialKeeper.Storage.Turso.Adapter -> VialKeeper.Storage.Turso.Driver
      _sqlite -> VialKeeper.Storage.SQLite.Driver.Rusqlite
    end
  end

  @doc "The configured engine's name."
  @spec engine() :: binary()
  def engine, do: driver().engine()
end
