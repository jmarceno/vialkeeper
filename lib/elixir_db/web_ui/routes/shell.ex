defmodule ElixirDB.WebUI.Routes.Shell do
  @moduledoc """
  Anonymous console shell served at `/ui`.

  The shell is static product chrome only. Authenticated content loads into the
  application root after bootstrap dispatches `elixirdb:start`.
  """

  alias ElixirDB.WebUI.{Layout, Response}

  @doc """
  Renders the inert public shell document.
  """
  @spec call(Plug.Conn.t()) :: Plug.Conn.t()
  def call(conn), do: Response.html(conn, 200, Layout.shell())
end
