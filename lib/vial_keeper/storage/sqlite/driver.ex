defmodule VialKeeper.Storage.SQLite.Driver do
  @moduledoc """
  Engine driver behind the shared SQL modules.

  The SQLite-dialect SQL under `VialKeeper.Storage.SQLite.*` runs on any
  driver that implements this behaviour. The first group of callbacks is the
  NIF function set (one call per statement, same term encoding). The second
  group holds what differs between engines: connection pragmas, the write
  transaction statement, the artifact and sidecar file names, writer
  capabilities and the startup probe.

  `VialKeeper.Storage.SQLite.Connection` is the only caller of the statement
  callbacks; it keeps the driver in the connection handle.

  `use VialKeeper.Storage.SQLite.Driver, native: Native` declares the
  behaviour and delegates the statement callbacks to the NIF module `Native`;
  a NIF module itself does `use VialKeeper.Storage.SQLite.Driver, :nif` after
  `use Rustler` to get the stubs Rustler replaces.
  """

  @type conn :: reference()
  @type value :: integer() | float() | binary() | nil
  @type param :: value() | {:blob, binary()}
  @type reason :: binary() | atom()
  @type storage_mode :: :disk | :memory

  @callback open(path :: binary(), flags :: integer()) :: {:ok, conn()} | {:error, reason()}
  @callback close(conn()) :: :ok | {:error, reason()}
  @callback cancel(conn()) :: :ok
  @callback set_busy_timeout(conn(), timeout_ms :: integer()) :: :ok
  @callback execute(conn(), sql :: binary()) :: :ok | {:error, reason()}
  @callback query(conn(), sql :: binary(), [param()]) :: {:ok, [[value()]]} | {:error, reason()}
  @callback query_inline(conn(), sql :: binary(), [param()]) ::
              {:ok, [[value()]]} | {:error, reason()} | :contended
  @callback last_insert_rowid(conn()) :: {:ok, integer()} | {:error, reason()}
  @callback serialize(conn()) :: {:ok, binary()} | {:error, reason()}

  @nif_functions [
    open: 2,
    close: 1,
    cancel: 1,
    set_busy_timeout: 2,
    execute: 2,
    query: 3,
    query_inline: 3,
    last_insert_rowid: 1,
    serialize: 1
  ]

  defmacro __using__(:nif) do
    quote do
      alias VialKeeper.Storage.SQLite.Driver

      @doc "Opens `path` (a file name, or `file:` URI where supported) with SQLite open `flags`."
      @spec open(binary(), integer()) :: {:ok, Driver.conn()} | {:error, Driver.reason()}
      def open(_path, _flags), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Closes the connection; later calls answer `{:error, :closed}`."
      @spec close(Driver.conn()) :: :ok | {:error, Driver.reason()}
      def close(_conn), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Wakes a caller waiting on a lock and interrupts the running statement, where supported."
      @spec cancel(Driver.conn()) :: :ok
      def cancel(_conn), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Sets how long a statement waits for another connection's lock (default 2000 ms)."
      @spec set_busy_timeout(Driver.conn(), integer()) :: :ok
      def set_busy_timeout(_conn, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Runs SQL text that may contain several statements; returns no rows."
      @spec execute(Driver.conn(), binary()) :: :ok | {:error, Driver.reason()}
      def execute(_conn, _sql), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Runs one statement on a dirty IO scheduler and returns every row."
      @spec query(Driver.conn(), binary(), [Driver.param()]) ::
              {:ok, [[Driver.value()]]} | {:error, Driver.reason()}
      def query(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Like `query/3` on the calling scheduler, or `:contended` without running anything."
      @spec query_inline(Driver.conn(), binary(), [Driver.param()]) ::
              {:ok, [[Driver.value()]]} | {:error, Driver.reason()} | :contended
      def query_inline(_conn, _sql, _params), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Returns the rowid of the connection's last insert."
      @spec last_insert_rowid(Driver.conn()) :: {:ok, integer()} | {:error, Driver.reason()}
      def last_insert_rowid(_conn), do: :erlang.nif_error(:nif_not_loaded)

      @doc "Returns the main database as an in-memory image, or `{:error, :unsupported}`."
      @spec serialize(Driver.conn()) :: {:ok, binary()} | {:error, Driver.reason()}
      def serialize(_conn), do: :erlang.nif_error(:nif_not_loaded)
    end
  end

  defmacro __using__(native: native) do
    quote bind_quoted: [native: native, functions: @nif_functions] do
      @behaviour VialKeeper.Storage.SQLite.Driver

      for {name, arity} <- functions do
        args = Macro.generate_arguments(arity, __MODULE__)
        @impl true
        defdelegate unquote(name)(unquote_splicing(args)), to: native
      end
    end
  end

  @doc "Engine name recorded in `db_meta.storage_engine`."
  @callback engine() :: binary()

  @doc "File name of the data artifact inside a bundle root."
  @callback artifact_name() :: binary()

  @doc "Suffixes of the sidecar files next to the artifact that a clean close removes when empty."
  @callback sidecar_suffixes() :: [binary()]

  @doc "Statement that opens a concurrent write transaction (`run_concurrent/2`)."
  @callback begin_concurrent() :: binary()

  @doc "How many times a serial write transaction retries a `:write_conflict` itself."
  @callback serial_conflict_retries() :: non_neg_integer()

  @doc "Sets the persistent and per-connection pragmas of a read-write connection."
  @callback configure(handle :: term(), storage_mode()) :: :ok | {:error, term()}

  @doc "Sets the pragmas of a snapshot reader connection."
  @callback configure_reader(handle :: term()) :: :ok | {:error, term()}

  @doc "Checks the pragmas `configure/2` set; anything else is an unsupported file."
  @callback valid_pragmas?(handle :: term(), storage_mode()) :: boolean()

  @doc """
  Opens the artifact of a closed bundle read-only for offline tooling, without
  recovering or rewriting it.
  """
  @callback open_closed_artifact(path :: binary()) :: {:ok, term()} | {:error, term()}

  @doc "Removes what `open_closed_artifact/1` left next to the artifact after its close."
  @callback release_closed_artifact(path :: binary()) :: :ok

  @doc "How many writer connections may run at once and how the sequence ledger persists."
  @callback writer_capabilities(storage_mode()) :: %{
              max_writers: pos_integer(),
              sequence_persistence: :separate_connection | :none
            }

  @doc "Fails fast when the engine build lacks a required capability."
  @callback validate_capabilities!() :: binary()

  @doc "Opaque engine capability metadata for diagnostics."
  @callback capabilities_report() :: map()
end
