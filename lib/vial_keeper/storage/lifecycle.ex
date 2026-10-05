defmodule VialKeeper.Storage.Lifecycle do
  @moduledoc """
  Backend-agnostic lifecycle entry points, including snapshot readers.

  Runtime and shared code call this module instead of a physical engine.
  Reader open/close stay on the lifecycle port; SQLite connection details
  never cross this boundary.
  """

  alias VialKeeper.Storage.BackendContext
  alias VialKeeper.Storage.Ports.Access
  alias VialKeeper.Storage.Ports.Errors

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @doc """
  Opens a readonly snapshot reader for `context`.

  Disk SQLite returns a distinct reader context. Memory backends and the
  sentinel return `{:error, :unsupported_readers}` so callers route reads
  through the writer connection.
  """
  @spec open_reader(BackendContext.t()) ::
          {:ok, BackendContext.t()}
          | {:error, :unsupported_readers}
          | {:error, VialKeeper.Error.t()}
  def open_reader(%BackendContext{} = context) do
    case Access.port(context, :lifecycle).open_reader(context) do
      {:error, :unsupported_readers} -> {:error, :unsupported_readers}
      other -> Errors.wrap(other)
    end
  end

  @doc "Closes a reader context opened by `open_reader/1`."
  @spec close_reader(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  def close_reader(%BackendContext{} = context) do
    Errors.wrap(Access.port(context, :lifecycle).close_reader(context))
  end

  @doc """
  Opens an extra read-write connection for a writer slot or the sequence
  ledger. Backends without extra writers (memory, `:memory:` SQLite and the
  sentinel) return `{:error, :unsupported_writers}`.
  """
  @spec open_writer(BackendContext.t()) ::
          {:ok, BackendContext.t()}
          | {:error, :unsupported_writers}
          | {:error, VialKeeper.Error.t()}
  def open_writer(%BackendContext{} = context) do
    case Access.port(context, :lifecycle).open_writer(context) do
      {:ok, %BackendContext{} = writer} ->
        {:ok, %{writer | identity: context.identity, bundle_root: context.bundle_root}}

      {:error, :unsupported_writers} ->
        {:error, :unsupported_writers}

      other ->
        Errors.wrap(other)
    end
  end

  @doc "Closes a writer context opened by `open_writer/1`."
  @spec close_writer(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  def close_writer(%BackendContext{} = context) do
    Errors.wrap(Access.port(context, :lifecycle).close_writer(context))
  end

  @doc "Clears the per-connection caches a writer process holds for `context`."
  @spec reset_writer_caches(BackendContext.t()) :: :ok
  def reset_writer_caches(%BackendContext{} = context) do
    Access.port(context, :lifecycle).reset_writer_caches(context)
  end

  @doc """
  Returns the backend's writer capabilities: `max_writers` and
  `sequence_persistence`.
  """
  @spec writer_capabilities(BackendContext.t()) :: %{
          max_writers: pos_integer(),
          sequence_persistence: :separate_connection | :none
        }
  def writer_capabilities(%BackendContext{} = context) do
    capabilities = Access.port(context, :lifecycle).capabilities(context)

    %{
      max_writers: Map.get(capabilities, :max_writers, 1),
      sequence_persistence: Map.get(capabilities, :sequence_persistence, :none)
    }
  end

  @doc "Interrupts a statement running on a reader, when supported by the backend."
  @spec interrupt_reader(BackendContext.t()) :: :ok | :unsupported
  def interrupt_reader(%BackendContext{} = context) do
    Access.port(context, :lifecycle).interrupt_reader(context)
  end
end
