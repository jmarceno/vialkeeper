defmodule VialKeeper.Storage.OpaqueHandle do
  @moduledoc """
  Opaque handle for storage backends.

  Shared and runtime code may pass handles but must not unwrap them or read
  physical fields such as connection references. Payloads live in a private ETS
  table owned by `VialKeeper.Storage.OpaqueHandle.Server`. The general
  unwrap/replace/drop operations require a backend Context module in the caller
  stack. Backend Context modules use a lower-overhead unwrap operation that the
  Reach architecture rules forbid everywhere else. This is an internal
  architecture boundary, not a security sandbox for arbitrary BEAM code.
  """

  @enforce_keys [:id]
  defstruct [:id]

  @type t :: %__MODULE__{id: reference()}

  alias VialKeeper.Storage.OpaqueHandle.Server

  @doc "Creates the private ETS table used to store opaque handle payloads."
  @spec ensure_table!() :: :ok
  def ensure_table! do
    case Process.whereis(Server) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        raise "storage opaque-handle server is not running"
    end
  end

  @doc "Wraps a backend-private term in an opaque handle."
  @spec wrap(term()) :: t()
  def wrap(term) do
    ensure_table!()
    Server.wrap(term)
  end

  @doc "Returns the payload for an opaque handle after checking the caller stack."
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

  @doc "Returns the payload for an opaque handle from a backend Context module."
  @spec backend_unwrap(t()) :: term()
  def backend_unwrap(%__MODULE__{} = handle) do
    ensure_table!()

    case Server.backend_unwrap(handle) do
      {:ok, term} -> term
      {:error, :missing} -> raise ArgumentError, "storage opaque handle is no longer valid"
    end
  end

  @doc "Replaces the payload stored for an opaque handle."
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

  @doc "Drops an opaque handle and its stored payload."
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
