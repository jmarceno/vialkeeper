defmodule ElixirDB.Storage.Ports.Lifecycle do
  @moduledoc """
  Backend lifecycle port: create, open, close, identity, and configuration.

  All successful create/open paths return an opaque
  `ElixirDB.Storage.BackendContext`. Shared code must not inspect
  `:backend_ref`.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}

  @callback create(binary(), map()) :: result(BackendContext.t())
  @callback open(binary(), map()) :: result(BackendContext.t())
  @callback close(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback open_reader(BackendContext.t()) ::
              {:ok, BackendContext.t()}
              | {:error, :unsupported_readers}
              | {:error, ElixirDB.Error.t()}
  @callback close_reader(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback identity(BackendContext.t()) :: result(map())
  @callback update_config(BackendContext.t(), map()) :: result(map())
  @callback capabilities(BackendContext.t()) :: map()
end
