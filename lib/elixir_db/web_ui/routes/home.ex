defmodule ElixirDB.WebUI.Routes.Home do
  @moduledoc """
  Authenticated home fragment for the embedded administration console.

  The fragment proves the auth boundary and provides a bounded landing surface
  that calls only application facades for dashboard data.
  """

  alias ElixirDB.WebUI.{Components, HTML, Response}

  @doc """
  Renders the authenticated home fragment.
  """
  @spec call(Plug.Conn.t()) :: Plug.Conn.t()
  def call(conn) do
    Response.fragment(conn, [
      "<section class=\"stack\" aria-labelledby=\"home-heading\">\n",
      Components.page_header("Console", "Administration surface for this ElixirDB node."),
      "  <div class=\"panel\">\n",
      "    <h2 id=\"home-heading\">Ready</h2>\n",
      "    <p class=\"muted\">",
      HTML.escape("Use the navigation to manage databases and related services."),
      "</p>\n",
      "  </div>\n",
      "</section>\n"
    ])
  end
end
