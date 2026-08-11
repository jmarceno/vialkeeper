defmodule ElixirDB.Storage.Ports.Ownership do
  @moduledoc """
  Single-database runtime ownership port.

  Backends acquire exclusive ownership for a bundle root. Failures surface as
  typed `database_in_use` (or related) errors — never engine lock text.
  """

  @callback start_ownership(binary()) :: GenServer.on_start()
end
