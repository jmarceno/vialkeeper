defmodule ElixirDB.WebUI.Response do
  @moduledoc """
  HTML response helpers for the embedded Web UI.

  UI fragments use `text/html` and defensive browser headers. They intentionally
  avoid the machine JSON envelope owned by `ElixirDB.HTTP.Response`.
  """

  import Plug.Conn

  alias ElixirDB.Error
  alias ElixirDB.WebUI.Components
  alias ElixirDB.WebUI.HTML

  @security_headers [
    {"content-security-policy",
     "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"},
    {"x-content-type-options", "nosniff"},
    {"referrer-policy", "no-referrer"},
    {"x-frame-options", "DENY"},
    {"cache-control", "no-store"}
  ]

  @doc """
  Sends a full HTML document or fragment with UI security headers.
  """
  @spec html(Plug.Conn.t(), pos_integer(), iodata()) :: Plug.Conn.t()
  def html(conn, status, body) when is_integer(status) do
    conn
    |> put_resp_content_type("text/html")
    |> put_security_headers()
    |> send_resp(status, body)
  end

  @doc """
  Sends a successful HTML fragment for HTMX swaps.
  """
  @spec fragment(Plug.Conn.t(), iodata()) :: Plug.Conn.t()
  def fragment(conn, body), do: html(conn, 200, body)

  @doc """
  Renders a safe HTML error fragment from an `ElixirDB.Error`.
  """
  @spec error_fragment(Plug.Conn.t(), Error.t()) :: Plug.Conn.t()
  def error_fragment(conn, %Error{} = error) do
    status = if is_integer(error.http_status), do: error.http_status, else: 400

    html(conn, status, Components.error_block(error))
  end

  @doc """
  Optionally sets an HTMX trigger header with a non-sensitive event payload.
  """
  @spec put_hx_trigger(Plug.Conn.t(), String.t() | map()) :: Plug.Conn.t()
  def put_hx_trigger(conn, trigger) when is_binary(trigger) do
    put_resp_header(conn, "hx-trigger", trigger)
  end

  def put_hx_trigger(conn, trigger) when is_map(trigger) do
    put_resp_header(conn, "hx-trigger", HTML.encode_json(trigger))
  end

  @doc """
  Puts the standard UI security headers on a connection.
  """
  @spec put_security_headers(Plug.Conn.t()) :: Plug.Conn.t()
  def put_security_headers(conn) do
    Enum.reduce(@security_headers, conn, fn {name, value}, acc ->
      put_resp_header(acc, name, value)
    end)
  end
end
