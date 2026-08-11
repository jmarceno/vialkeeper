defmodule ElixirDB.Storage.OpaqueHandle do
  @moduledoc """
  Opaque handle for storage backends.

  Shared and runtime code may pass handles but must not unwrap them or read
  physical fields such as connection references. Payloads live in a private ETS
  table owned by `ElixirDB.Storage.OpaqueHandle.Server`. Unwrap/replace/drop
  succeed only when the caller stack includes a backend Context module
  (`SQLite.Context`, `Memory.Context`, or `Sentinel.Context`), so dictionary
  introspection, bare `GenServer.call/2`, and direct MFA calls from shared code
  cannot recover a live adapter. Reach also forbids those MFAs outside backends.
  """

  @enforce_keys [:id]
  defstruct [:id]

  @type t :: %__MODULE__{id: reference()}

  alias ElixirDB.Storage.OpaqueHandle.Server

  @doc false
  @spec ensure_table!() :: :ok
  def ensure_table! do
    case Process.whereis(Server) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        {:ok, _pid} = Server.start_link([])
        :ok
    end
  end

  @doc "Wraps a backend-private term in an opaque handle."
  @spec wrap(term()) :: t()
  def wrap(term) do
    ensure_table!()
    Server.wrap(term)
  end

  @doc false
  @spec unwrap(t()) :: term()
  def unwrap(%__MODULE__{} = handle) do
    ensure_table!()

    case Server.unwrap(handle) do
      {:ok, term} ->
        term

      {:error, :missing} ->
        raise ArgumentError, "storage opaque handle is no longer valid"

      {:error, :forbidden} ->
        raise ArgumentError, "storage opaque handle unwrap is not permitted for this caller"
    end
  end

  @doc false
  @spec replace(t(), term()) :: t()
  def replace(%__MODULE__{} = handle, term) do
    ensure_table!()

    case Server.replace(handle, term) do
      {:ok, ^handle} ->
        handle

      {:error, :missing} ->
        raise ArgumentError, "storage opaque handle is no longer valid"

      {:error, :forbidden} ->
        raise ArgumentError, "storage opaque handle replace is not permitted for this caller"
    end
  end

  @doc false
  @spec drop(t()) :: :ok
  def drop(%__MODULE__{} = handle) do
    ensure_table!()

    case Server.drop(handle) do
      :ok ->
        :ok

      {:error, :forbidden} ->
        raise ArgumentError, "storage opaque handle drop is not permitted for this caller"
    end
  end
end
