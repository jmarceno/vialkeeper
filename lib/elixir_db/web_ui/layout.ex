defmodule ElixirDB.WebUI.Layout do
  @moduledoc """
  Full-page shell markup for the embedded administration console.

  The anonymous shell is intentionally inert: it contains product chrome,
  same-origin asset references, an empty application root, and a hidden bearer
  token form. It must not render database, registration, or configuration state.
  """

  alias ElixirDB.WebUI.HTML

  @htmx_config ~s({"allowEval":false,"allowScriptTags":false,"historyCacheSize":0,"includeIndicatorStyles":false,"selfRequestsOnly":true})

  @doc """
  Renders the public console shell document.
  """
  @spec shell() :: iodata()
  def shell do
    [
      "<!DOCTYPE html>\n",
      "<html lang=\"en\">\n",
      "<head>\n",
      "  <meta charset=\"utf-8\">\n",
      "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n",
      "  <meta name=\"htmx-config\" content='",
      @htmx_config,
      "'>\n",
      "  <title>",
      HTML.escape("ElixirDB"),
      "</title>\n",
      "  <link rel=\"stylesheet\" href=\"/ui/assets/app.css\">\n",
      "  <script src=\"/ui/assets/htmx.min.js\" defer></script>\n",
      "  <script src=\"/ui/assets/auth-bootstrap.js\" defer></script>\n",
      "</head>\n",
      "<body>\n",
      "  <div class=\"shell\">\n",
      "    <nav class=\"nav\" aria-label=\"Console\">\n",
      "      <p class=\"brand\">",
      HTML.escape("ElixirDB"),
      "</p>\n",
      "      <ul>\n",
      nav_item("Home", "/ui/fragments/home"),
      nav_item("Databases", "/ui/fragments/databases"),
      nav_item("Federation", "/ui/fragments/federation"),
      nav_item("Materialized views", "/ui/fragments/materialized-views"),
      nav_item("Observability", "/ui/fragments/observability"),
      "      </ul>\n",
      "      <p class=\"row\" style=\"margin-top:1rem\">\n",
      "        <button type=\"button\" class=\"secondary\" data-elixirdb-logout>Log out</button>\n",
      "      </p>\n",
      "    </nav>\n",
      "    <main class=\"main\">\n",
      "      <div id=\"app\"\n",
      "           hx-get=\"/ui/fragments/home\"\n",
      "           hx-trigger=\"elixirdb:start from:body\"\n",
      "           hx-target=\"this\"\n",
      "           hx-swap=\"innerHTML\"></div>\n",
      "      <form id=\"auth-form\" class=\"auth-form panel\" autocomplete=\"off\">\n",
      "        <h1>Sign in</h1>\n",
      "        <p class=\"muted\">Enter a bearer token to load the administration console.</p>\n",
      "        <label>\n",
      "          Bearer token\n",
      "          <input type=\"password\" name=\"bearer_token\" autocomplete=\"off\" spellcheck=\"false\" required>\n",
      "        </label>\n",
      "        <p class=\"row\" style=\"margin-top:0.85rem\">\n",
      "          <button type=\"submit\">Continue</button>\n",
      "        </p>\n",
      "      </form>\n",
      "    </main>\n",
      "  </div>\n",
      "</body>\n",
      "</html>\n"
    ]
  end

  defp nav_item(label, path) do
    [
      "        <li><a href=\"#\" hx-get=\"",
      HTML.attr(path),
      "\" hx-target=\"#app\" hx-swap=\"innerHTML\">",
      HTML.escape(label),
      "</a></li>\n"
    ]
  end
end
