defmodule VialKeeper.HTTP.RouterSpecs do
  @moduledoc """
  Plug.Router callback specs shared by HTTP and Web UI routers.

  `use Plug.Router` generates `init/1`, `call/2`, `match/2`, and `dispatch/2`
  without typespecs. This module attaches the Plug callback contracts.
  """

  defmacro __using__(_opts) do
    quote do
      @spec init(Plug.opts()) :: Plug.opts()
      @spec call(Plug.Conn.t(), Plug.opts()) :: Plug.Conn.t()
      @spec match(Plug.Conn.t(), Plug.opts()) :: Plug.Conn.t()
      @spec dispatch(Plug.Conn.t(), Plug.opts()) :: Plug.Conn.t()
    end
  end
end
