defmodule VialKeeper.JSON.CanonicalPropertiesTest do
  @moduledoc "Canonical JSON encode/decode stability for validated JSON values."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.JSON.{Canonical, StrictDecoder}

  property "encode, decode, and recanonicalize are stable" do
    check all(value <- json_value(), max_runs: 40) do
      assert {:ok, encoded} = Canonical.encode(value)
      assert {:ok, decoded} = StrictDecoder.decode(encoded)
      assert {:ok, recanonical} = Canonical.encode(decoded)
      assert recanonical == encoded
    end
  end

  property "decode_encoded matches StrictDecoder for integer and string trees" do
    check all(value <- json_value(), max_runs: 40) do
      assert {:ok, encoded} = Canonical.encode(value)
      assert {:ok, decoded} = StrictDecoder.decode(encoded)
      assert {:ok, ^decoded} = Canonical.decode_encoded(value, encoded)
    end
  end

  test "float members still round-trip through StrictDecoder" do
    body = %{"n" => 1.0}
    assert {:ok, encoded} = Canonical.encode(body)
    assert encoded == "{\"n\":1}"
    assert {:ok, %{"n" => 1}} = Canonical.decode_encoded(body, encoded)
  end

  defp json_value do
    StreamData.tree(json_scalar(), fn child ->
      StreamData.one_of([
        StreamData.list_of(child, max_length: 4),
        StreamData.map_of(
          StreamData.string(:alphanumeric, min_length: 1, max_length: 8),
          child,
          max_length: 4
        )
      ])
    end)
  end

  defp json_scalar do
    StreamData.one_of([
      StreamData.constant(nil),
      StreamData.boolean(),
      StreamData.integer(-1_000..1_000),
      StreamData.string(:alphanumeric, max_length: 24)
    ])
  end
end
