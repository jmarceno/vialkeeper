defmodule ElixirDB.Storage.Sentinel.Transaction do
  @moduledoc """
  SQL-free transaction port for the sentinel backend.

  Atomicity is process-local: the function runs to completion or returns an
  error without engine transaction text.
  """
  @behaviour ElixirDB.Storage.Ports.Transaction

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.Sentinel.Adapter

  @impl true
  def run(%BackendContext{backend_ref: %Adapter{}} = context, fun) when is_function(fun, 1) do
    Errors.wrap(fun.(context))
  end

  def run(%BackendContext{}, _fun) do
    {:error, ElixirDB.Error.internal_error("backend context is not a sentinel adapter")}
  end
end
