defmodule VialKeeper.Query.PrefixBoundsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Query.PrefixBounds

  test "returns the next Unicode-scalar upper bound" do
    assert {:ok, %{lower: "ab", upper: "ac"}} = PrefixBounds.bounds("ab")
    assert {:ok, %{lower: "é", upper: "ê"}} = PrefixBounds.bounds("é")
    assert {:ok, %{lower: "😀", upper: "😁"}} = PrefixBounds.bounds("😀")
  end

  test "skips the UTF-16 surrogate range" do
    prefix = <<0xED, 0x9F, 0xBF>>
    assert {:ok, %{lower: ^prefix, upper: <<0xEE, 0x80, 0x80>>}} = PrefixBounds.bounds(prefix)
  end

  test "returns an open upper bound at the maximum scalar" do
    prefix = <<0xF4, 0x8F, 0xBF, 0xBF>>
    assert {:ok, %{lower: ^prefix, upper: nil}} = PrefixBounds.bounds(prefix)
  end

  test "carries and truncates at Unicode scalar boundaries" do
    prefix = "a" <> <<0xF4, 0x8F, 0xBF, 0xBF>>
    assert {:ok, %{lower: ^prefix, upper: "b"}} = PrefixBounds.bounds(prefix)

    prefix = "a" <> <<0xED, 0x9F, 0xBF>>

    assert {:ok, %{lower: ^prefix, upper: "a" <> <<0xEE, 0x80, 0x80>>}} =
             PrefixBounds.bounds(prefix)
  end

  test "rejects empty and invalid UTF-8 prefixes" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = PrefixBounds.bounds("")
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = PrefixBounds.bounds(<<255>>)
  end
end
