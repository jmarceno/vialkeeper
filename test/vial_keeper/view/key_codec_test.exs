defmodule VialKeeper.View.KeyCodecTest do
  @moduledoc "Ordering and encoding tests for declarative view keys."
  use ExUnit.Case, async: true

  alias VialKeeper.View.KeyCodec

  @fixtures [
    %{id: "null-before-false", left: [nil], right: [false], order: :lt},
    %{id: "false-before-true", left: [false], right: [true], order: :lt},
    %{id: "true-before-number", left: [true], right: [1], order: :lt},
    %{id: "negative-before-positive", left: [-1.0], right: [1.0], order: :lt},
    %{id: "signed-zero-normalized", left: [-0.0], right: [0.0], order: :eq},
    %{id: "number-before-string", left: [42], right: ["42"], order: :lt},
    %{id: "prefix-escape", left: ["a\u0000b"], right: ["a"], order: :gt},
    %{
      id: "composite-order",
      left: [1, "a"],
      right: [1, "b"],
      order: :lt
    }
  ]

  @encoded_fixtures %{
    "null" => <<0>>,
    "false" => <<1>>,
    "true" => <<2>>,
    "pos1.0" => <<0x03, 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>,
    "neg1.0" => <<0x03, 0x40, 0x0F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>,
    "zero" => <<0x03, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>,
    "negzero" => <<0x03, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00>>,
    "nul_string" => <<0x04, 0x61, 0x00, 0x01, 0x62, 0x00, 0x00>>,
    "composite" => <<0x03, 0xBF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x61, 0x00, 0x00>>
  }

  test "fixture vectors preserve ordering contract" do
    for %{left: left, right: right, order: expected, id: id} <- @fixtures do
      assert KeyCodec.compare(left, right) == expected, "ordering mismatch for #{id}"

      {:ok, left_encoded} = KeyCodec.encode(left)
      {:ok, right_encoded} = KeyCodec.encode(right)
      assert KeyCodec.compare_encoded(left_encoded, right_encoded) == expected
    end
  end

  test "fixture vectors match exact encoded bytes" do
    assert {:ok, null} = KeyCodec.encode([nil])
    assert null == @encoded_fixtures["null"]

    assert {:ok, false_bin} = KeyCodec.encode([false])
    assert false_bin == @encoded_fixtures["false"]

    assert {:ok, true_bin} = KeyCodec.encode([true])
    assert true_bin == @encoded_fixtures["true"]

    assert {:ok, pos} = KeyCodec.encode([1.0])
    assert pos == @encoded_fixtures["pos1.0"]

    assert {:ok, neg} = KeyCodec.encode([-1.0])
    assert neg == @encoded_fixtures["neg1.0"]

    assert {:ok, zero} = KeyCodec.encode([0.0])
    assert zero == @encoded_fixtures["zero"]

    assert {:ok, negzero} = KeyCodec.encode([-0.0])
    assert negzero == @encoded_fixtures["negzero"]

    assert {:ok, nul_string} = KeyCodec.encode(["a\u0000b"])
    assert nul_string == @encoded_fixtures["nul_string"]

    assert {:ok, composite} = KeyCodec.encode([1, "a"])
    assert composite == @encoded_fixtures["composite"]
  end

  test "rejects unsupported component types" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = KeyCodec.encode([%{}])
  end

  test "rejects oversized integers without raising" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = KeyCodec.encode([10 ** 400])

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             KeyCodec.encode([Bitwise.bsl(1, 1024) - 1])
  end
end
