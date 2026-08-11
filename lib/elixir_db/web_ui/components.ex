defmodule ElixirDB.WebUI.Components do
  @moduledoc """
  Reusable HTML fragments for the embedded administration console.

  Components render iodata and escape dynamic values through
  `ElixirDB.WebUI.HTML`.
  """

  alias ElixirDB.Error
  alias ElixirDB.WebUI.HTML

  @doc """
  Renders a page heading and optional muted supporting sentence.
  """
  @spec page_header(String.t(), String.t() | nil) :: iodata()
  def page_header(title, subtitle \\ nil) do
    [
      "<header class=\"stack\">\n",
      "  <h1>",
      HTML.escape(title),
      "</h1>\n",
      if(subtitle,
        do: ["  <p class=\"muted\">", HTML.escape(subtitle), "</p>\n"],
        else: []
      ),
      "</header>\n"
    ]
  end

  @doc """
  Renders a safe domain-error block with stable code and escaped message.
  """
  @spec error_block(Error.t()) :: iodata()
  def error_block(%Error{} = error) do
    [
      "<div class=\"error-block\" role=\"alert\" aria-live=\"polite\">\n",
      "  <p><strong>",
      HTML.escape(to_string(error.code)),
      "</strong></p>\n",
      "  <p>",
      HTML.escape(error.message || "request failed"),
      "</p>\n",
      "</div>\n"
    ]
  end

  @doc """
  Renders a status badge whose meaning is also present in text.
  """
  @spec status_badge(String.t(), :ok | :warn | :danger | atom()) :: iodata()
  def status_badge(label, kind \\ :ok) do
    class =
      case kind do
        :ok -> "status status-ok"
        :warn -> "status status-warn"
        :danger -> "status status-danger"
        _ -> "status"
      end

    ["<span class=\"", class, "\">", HTML.escape(label), "</span>"]
  end

  @doc """
  Renders a simple table from headers and row iodata lists.
  """
  @spec table(String.t() | nil, [String.t()], [[iodata()]]) :: iodata()
  def table(caption \\ nil, headers, rows) do
    [
      "<div class=\"table-scroll\">\n",
      "<table>\n",
      if(caption, do: ["  <caption>", HTML.escape(caption), "</caption>\n"], else: []),
      "  <thead><tr>",
      Enum.map(headers, fn header -> ["<th scope=\"col\">", HTML.escape(header), "</th>"] end),
      "</tr></thead>\n",
      "  <tbody>\n",
      Enum.map(rows, fn cells ->
        ["    <tr>", Enum.map(cells, fn cell -> ["<td>", cell, "</td>"] end), "</tr>\n"]
      end),
      "  </tbody>\n",
      "</table>\n",
      "</div>\n"
    ]
  end

  @doc """
  Renders pagination controls that keep opaque bookmarks out of the URL bar.
  """
  @spec pagination(String.t(), String.t() | nil, String.t()) :: iodata()
  def pagination(action, bookmark, label \\ "Next page") do
    if is_binary(bookmark) and bookmark != "" do
      [
        "<form class=\"row\" hx-get=\"",
        HTML.attr(action),
        "\" hx-target=\"#app\" hx-swap=\"innerHTML\">\n",
        "  <input type=\"hidden\" name=\"bookmark\" value=\"",
        HTML.attr(bookmark),
        "\">\n",
        "  <button type=\"submit\">",
        HTML.escape(label),
        "</button>\n",
        "</form>\n"
      ]
    else
      []
    end
  end

  @doc """
  Renders a labeled form field.
  """
  @spec field(String.t(), iodata()) :: iodata()
  def field(label, control) do
    ["<label>", HTML.escape(label), control, "</label>\n"]
  end
end
