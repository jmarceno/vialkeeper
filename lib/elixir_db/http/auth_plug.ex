defmodule ElixirDB.HTTP.AuthPlug do
  @moduledoc """
  Bearer-token authentication for the HTTP API (`AUTH-001`).

  A single chokepoint on the top-level router. When `[auth] enabled = true`,
  every request must present a valid `Authorization: Bearer <token>` header.
  Tokens are SHA-256 hashed and constant-time compared against the digests
  configured in `host.toml`.

  Failures are indistinguishable (`AUTH-004`): a missing header, a malformed
  header, and a wrong token all yield the same `unauthorized` response, so the
  error cannot be used to probe which case occurred.
  """

  @behaviour Plug

  import Plug.Conn
  alias ElixirDB.HTTP.Response

  @bearer_prefix "Bearer "

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    # Read auth configuration on each request rather than in init/1, so the
    # plug observes the current host.toml-derived env without depending on the
    # Router's init_mode and without a recompile to pick up changed tokens.
    auth = Application.get_env(:elixir_db, :auth, [])

    if Keyword.get(auth, :enabled, false) do
      token_digests = Keyword.get(auth, :token_digests, [])

      with [header | _] <- get_req_header(conn, "authorization"),
           token when is_binary(token) <- extract_bearer(header),
           digest <- digest(token),
           true <- matches_any?(digest, token_digests) do
        conn
      else
        _ -> unauthorized(conn)
      end
    else
      conn
    end
  end

  # A missing or non-Bearer header must produce the same response as a wrong
  # token (AUTH-004). `extract_bearer/1` returns `nil` for anything that is not
  # exactly "<prefix><token>", which collapses into the same failure branch.
  defp extract_bearer(header) when is_binary(header) do
    if String.starts_with?(header, @bearer_prefix) do
      case header |> String.slice(byte_size(@bearer_prefix)..-1//1) |> String.trim() do
        "" -> nil
        token -> token
      end
    else
      nil
    end
  end

  defp digest(token) when is_binary(token) do
    :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)
  end

  # Constant-time comparison against each configured digest. Comparing
  # digests (fixed-length hex) rather than raw tokens keeps the comparison
  # time independent of the supplied credential length.
  defp matches_any?(digest, token_digests) when is_binary(digest) and is_list(token_digests) do
    Enum.any?(token_digests, fn configured ->
      is_binary(configured) and Plug.Crypto.secure_compare(digest, configured)
    end)
  end

  defp unauthorized(conn) do
    conn
    |> Response.error(ElixirDB.Error.unauthorized())
    |> halt()
  end
end
