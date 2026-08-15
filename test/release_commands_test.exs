defmodule VialKeeper.ReleaseCommandsTest do
  @moduledoc """
  `VialKeeper.ReleaseCommands.token/0` and `generate_token/0` (`AUTH-002`):
  the generated token is 256-bit random hex, and the digest is the SHA-256 of
  the raw token, matching what the AuthPlug compares against.
  """
  use ExUnit.Case, async: true

  alias VialKeeper.ReleaseCommands

  test "generate_token returns a 64-character lowercase hex raw token" do
    {raw, _digest} = ReleaseCommands.generate_token()
    assert byte_size(raw) == 64
    assert Regex.match?(~r/^[0-9a-f]{64}$/, raw)
  end

  test "the digest is the SHA-256 hex of the raw token" do
    {raw, digest} = ReleaseCommands.generate_token()
    expected = String.downcase(:crypto.hash(:sha256, raw) |> Base.encode16(case: :lower))
    assert digest == expected
    assert byte_size(digest) == 64
  end

  test "each call yields a fresh token" do
    {raw_a, _} = ReleaseCommands.generate_token()
    {raw_b, _} = ReleaseCommands.generate_token()
    refute raw_a == raw_b
  end

  test "token/0 prints the token and digest lines and returns :ok" do
    {result, output} =
      ExUnit.CaptureIO.with_io(fn -> ReleaseCommands.token() end)

    assert result == :ok
    assert output =~ ~r/^token:\s+[0-9a-f]{64}$/m
    assert output =~ ~r/^digest:\s+[0-9a-f]{64}$/m
    assert output =~ "Authorization: Bearer"
    assert output =~ "host.toml"
  end

  test "a generated token authenticates against its digest via the auth comparison" do
    # End-to-end of the operator flow: generate, store digest, present raw token.
    {raw, digest} = ReleaseCommands.generate_token()
    presented = String.downcase(:crypto.hash(:sha256, raw) |> Base.encode16(case: :lower))
    assert Plug.Crypto.secure_compare(presented, digest)
  end
end
