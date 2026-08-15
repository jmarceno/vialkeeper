defmodule VialKeeper.ReleaseCommands do
  @moduledoc """
  Operator commands exposed through the OTP release launcher (`bin/vial_keeper`).

  `token/0` generates a bearer token and its SHA-256 digest (`AUTH-002`):
  print the raw token once for the operator to use as the `Authorization:
  Bearer` value, and the digest to paste into `host.toml`'s `[auth] tokens`.
  """

  @doc """
  Generates a 256-bit bearer token and prints it together with its SHA-256
  hex digest. The token is shown once.
  """
  @spec token :: :ok
  def token do
    {raw, digest} = generate_token()
    IO.puts("token:  #{raw}")
    IO.puts("digest: #{digest}")
    IO.puts("")
    IO.puts("Use the token value as: Authorization: Bearer #{raw}")
    IO.puts("Paste the digest into host.toml: tokens = [\"#{digest}\"]")
    :ok
  end

  @doc """
  Generates a 256-bit bearer token and returns `{raw, sha256_hex_digest}`.
  Public so the command can be exercised without capturing stdout.
  """
  @spec generate_token :: {raw :: String.t(), digest :: String.t()}
  def generate_token do
    raw = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
    digest = :crypto.hash(:sha256, raw) |> Base.encode16(case: :lower)
    {raw, digest}
  end
end
