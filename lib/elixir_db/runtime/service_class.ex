defmodule ElixirDB.Runtime.ServiceClass do
  @moduledoc false

  @type t :: :foreground | :subscription | :replication | :maintenance

  @classes [:foreground, :subscription, :replication, :maintenance]

  @spec classes() :: [t()]
  def classes, do: @classes

  @spec valid?(term()) :: boolean()
  def valid?(class), do: class in @classes
end
