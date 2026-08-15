defmodule VialKeeper.View.BookmarkCodecTest do
  @moduledoc "Adversarial coverage for signed view bookmark decoding."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Error
  alias VialKeeper.JSON.{Canonical, StrictDecoder}
  alias VialKeeper.TestSupport.GarbageGenerators
  alias VialKeeper.View.BookmarkCodec

  describe "decode/2" do
    test "rejects non-binary input" do
      for input <- [nil, 42, %{}, []] do
        assert {:error,
                %Error{
                  code: :invalid_bookmark,
                  message: "view bookmark must be an opaque string"
                }} = BookmarkCodec.decode(input)
      end
    end

    test "rejects malformed base64, non-JSON bytes, and non-object JSON" do
      inputs = [
        "not base64url!",
        encode_bytes(<<0xFF, 0xFE, 0xFD>>),
        encode_bytes("null"),
        encode_bytes("[]")
      ]

      for input <- inputs do
        assert_invalid(BookmarkCodec.decode(input))
      end
    end

    test "rejects missing, non-binary, and tampered checksums" do
      unsigned = valid_unsigned()

      for map <- [
            unsigned,
            Map.put(unsigned, "checksum", 42),
            valid_wire() |> Map.update!("checksum", &flip_first_hex/1)
          ] do
        assert_invalid(BookmarkCodec.decode(encode_map(map)))
      end
    end

    test "checksum binds every payload byte" do
      assert {:ok, encoded} = BookmarkCodec.encode(valid_payload())
      bytes = Base.url_decode64!(encoded, padding: false)
      {offset, length} = :binary.match(bytes, "doc-1")

      tampered =
        binary_part(bytes, 0, offset) <>
          "x" <>
          binary_part(bytes, offset + 1, byte_size(bytes) - offset - 1)

      assert length == byte_size("doc-1")
      assert_invalid(BookmarkCodec.decode(encode_bytes(tampered)))
    end

    test "rejects invalid versions and required fields" do
      invalid_changes = [
        {"version", 0},
        {"version", 2},
        {"version", "1"},
        {"definition_digest", 42},
        {"indexed_through", -1},
        {"indexed_through", "3"},
        {"indexed_through", 1.5},
        {"key_sort", nil},
        {"key_sort", 42},
        {"document_id", nil},
        {"document_id", 42}
      ]

      for {key, value} <- invalid_changes do
        assert_invalid(BookmarkCodec.decode(valid_unsigned() |> Map.put(key, value) |> sign()))
      end

      for key <- ["definition_digest", "key_sort", "document_id"] do
        assert_invalid(BookmarkCodec.decode(valid_unsigned() |> Map.delete(key) |> sign()))
      end
    end

    test "reports an expected-value mismatch as stale" do
      assert {:ok, encoded} = BookmarkCodec.encode(valid_payload())

      assert {:error,
              %Error{
                code: :invalid_bookmark,
                message: "view bookmark is stale"
              }} = BookmarkCodec.decode(encoded, %{"definition_digest" => "other"})
    end

    test "round trips with matching expected values" do
      payload = valid_payload()
      assert {:ok, encoded} = BookmarkCodec.encode(payload)

      assert {:ok, decoded} =
               BookmarkCodec.decode(encoded, %{
                 "definition_digest" => payload["definition_digest"],
                 "indexed_through" => payload["indexed_through"]
               })

      assert decoded == Map.put(payload, "version", 1) |> Map.put("checksum", decoded["checksum"])
      assert decoded["version"] == 1
    end
  end

  @tag :slow
  property "decode/2 never raises for arbitrary public input" do
    input_generator =
      StreamData.one_of([
        StreamData.binary(max_length: 512),
        GarbageGenerators.junk_term()
      ])

    check all(input <- input_generator, max_runs: 400) do
      result = BookmarkCodec.decode(input, %{})
      assert match?({:ok, %{}}, result) or match?({:error, %Error{}}, result)
    end
  end

  defp valid_payload do
    %{
      "definition_digest" => String.duplicate("a", 64),
      "indexed_through" => 42,
      "key_sort" => "WyJvcGVuIl0",
      "document_id" => "doc-1"
    }
  end

  defp valid_unsigned, do: Map.put(valid_payload(), "version", 1)

  defp valid_wire do
    assert {:ok, encoded} = BookmarkCodec.encode(valid_payload())
    assert {:ok, wire} = StrictDecoder.decode(Base.url_decode64!(encoded, padding: false))
    wire
  end

  defp sign(unsigned) do
    checksum =
      unsigned
      |> Canonical.encode!()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    unsigned
    |> Map.put("checksum", checksum)
    |> encode_map()
  end

  defp encode_map(map), do: map |> Canonical.encode!() |> encode_bytes()
  defp encode_bytes(bytes), do: Base.url_encode64(bytes, padding: false)

  defp flip_first_hex(<<first, rest::binary>>) do
    replacement = if first == ?0, do: ?1, else: ?0
    <<replacement, rest::binary>>
  end

  defp assert_invalid(result) do
    assert {:error, %Error{code: :invalid_bookmark}} = result
  end
end
