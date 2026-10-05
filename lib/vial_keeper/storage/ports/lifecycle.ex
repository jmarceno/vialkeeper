defmodule VialKeeper.Storage.Ports.Lifecycle do
  @moduledoc """
  Backend lifecycle port: create, open, close, identity, and configuration.

  All successful create/open paths return an opaque
  `VialKeeper.Storage.BackendContext`. Shared code must not inspect
  `:backend_ref`.
  """

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @callback create(binary(), map()) :: result(BackendContext.t())
  @callback open(binary(), map()) :: result(BackendContext.t())
  @callback close(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback open_reader(BackendContext.t()) ::
              {:ok, BackendContext.t()}
              | {:error, :unsupported_readers}
              | {:error, VialKeeper.Error.t()}
  @callback close_reader(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback interrupt_reader(BackendContext.t()) :: :ok | :unsupported
  @callback open_writer(BackendContext.t()) ::
              {:ok, BackendContext.t()}
              | {:error, :unsupported_writers}
              | {:error, VialKeeper.Error.t()}
  @callback close_writer(BackendContext.t()) :: :ok | {:error, VialKeeper.Error.t()}
  @callback reset_writer_caches(BackendContext.t()) :: :ok
  @callback identity(BackendContext.t()) :: result(map())
  @callback update_config(BackendContext.t(), map()) :: result(map())
  @doc """
  Reports backend capabilities. Every backend reports `max_writers` (a
  positive integer) and `sequence_persistence` (`:separate_connection` or
  `:none`).
  """
  @callback capabilities(BackendContext.t()) :: map()
end
