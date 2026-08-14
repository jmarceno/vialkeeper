defmodule ElixirDB.Storage.Lifecycle do
  @moduledoc """
  Backend-agnostic lifecycle entry points, including snapshot readers.

  Runtime and shared code call this module instead of a physical engine.
  Reader open/close stay on the lifecycle port; SQLite connection details
  never cross this boundary.
  """

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Ports.Errors

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @doc """
  Opens a readonly snapshot reader for `context`.

  Disk SQLite returns a distinct reader context. Memory backends and the
  sentinel return `{:error, :unsupported_readers}` so callers route reads
  through the writer connection.
  """
  @spec open_reader(BackendContext.t()) ::
          {:ok, BackendContext.t()} | {:error, :unsupported_readers} | {:error, ElixirDB.Error.t()}
  def open_reader(%BackendContext{} = context) do
    case Access.port(context, :lifecycle).open_reader(context) do
      {:error, :unsupported_readers} -> {:error, :unsupported_readers}
      other -> Errors.wrap(other)
    end
  end

  @doc "Closes a reader context opened by `open_reader/1`."
  @spec close_reader(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  def close_reader(%BackendContext{} = context) do
    Errors.wrap(Access.port(context, :lifecycle).close_reader(context))
  end

  @doc "Interrupts a statement running on a reader, when supported by the backend."
  @spec interrupt_reader(BackendContext.t()) :: :ok | :unsupported
  def interrupt_reader(%BackendContext{} = context) do
    Access.port(context, :lifecycle).interrupt_reader(context)
  end
end
