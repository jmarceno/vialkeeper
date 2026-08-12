defmodule ElixirDB.Replication.WireCompressionTest do
  @moduledoc "Bounded Zstandard JSON encode/decode contract for replication envelopes."

  use ExUnit.Case, async: true

  alias ElixirDB.Error
  alias ElixirDB.Replication.{WireCompression, ZstdFrame}

  @decoded_limit 4_096

  test "round-trips map and list envelopes and compresses tiny JSON" do
    map = %{"ok" => true, "n" => 1}
    list = [%{"id" => "a"}, %{"id" => "b"}]

    assert {:ok, encoded_map} = WireCompression.encode_json(map, @decoded_limit)
    assert {:ok, encoded_list} = WireCompression.encode_json(list, @decoded_limit)
    assert {:ok, encoded_tiny} = WireCompression.encode_json(%{}, @decoded_limit)

    assert encoded_tiny.uncompressed_length == 2
    assert encoded_tiny.compressed_length == byte_size(encoded_tiny.body)
    assert encoded_tiny.body != "{}"
    assert String.starts_with?(encoded_tiny.body, <<0x28, 0xB5, 0x2F, 0xFD>>)

    assert {:ok, ^map} = decode(encoded_map, expect: :map)
    assert {:ok, ^list} = decode(encoded_list, expect: :list)
    assert {:ok, %{}} = decode(encoded_tiny, expect: :map_or_list)
  end

  test "accepts the exact decoded boundary and rejects one extra byte" do
    json = ~s({"k":"v"})
    limit = byte_size(json)

    assert {:ok, encoded} = WireCompression.encode_json(%{"k" => "v"}, limit)
    assert encoded.uncompressed_length == limit
    assert {:ok, %{"k" => "v"}} = decode(encoded, decoded_limit: limit)

    assert {:error, %Error{code: :payload_too_large}} =
             WireCompression.encode_json(%{"k" => "v"}, limit - 1)
  end

  test "accepts the exact encoded boundary and rejects one extra byte" do
    decoded_limit = 256
    encoded_limit = WireCompression.encoded_limit(decoded_limit)
    assert {:ok, encoded} = WireCompression.encode_json(%{"k" => "v"}, decoded_limit)
    assert encoded.compressed_length <= encoded_limit
    assert {:ok, %{"k" => "v"}} = decode(encoded, decoded_limit: decoded_limit)

    exact = :binary.copy(<<0>>, encoded_limit)
    over = exact <> <<0>>

    assert {:error, %Error{code: :invalid_request}} =
             WireCompression.decode_json(exact,
               decoded_limit: decoded_limit,
               headers: headers(1),
               expect: :map
             )

    assert {:error, %Error{code: :payload_too_large}} =
             WireCompression.decode_json(over,
               decoded_limit: decoded_limit,
               headers: headers(1),
               expect: :map
             )
  end

  test "rejects missing and non-canonical uncompressed-length headers" do
    assert {:ok, encoded} = WireCompression.encode_json(%{"a" => 1}, @decoded_limit)

    assert {:error, %Error{code: :invalid_request}} =
             WireCompression.decode_json(encoded.body,
               decoded_limit: @decoded_limit,
               headers: [
                 {"content-encoding", "zstd"},
                 {"content-type", "application/json"}
               ],
               expect: :map
             )

    for value <- ["01", "+1", "1 ", " 1", "1.0", "-1", ""] do
      assert {:error, %Error{code: :invalid_request}} =
               WireCompression.decode_json(encoded.body,
                 decoded_limit: @decoded_limit,
                 headers: headers(value),
                 expect: :map
               )
    end
  end

  test "rejects declared frame size mismatch before decompress" do
    huge_declared = 1_099_511_627_776

    frame =
      crafted_frame(
        content_size: huge_declared,
        fcs_flag: 3,
        payload: <<0, 1, 2, 3, 4, 5, 6, 7>>
      )

    assert {:ok, %{content_size: ^huge_declared}} = ZstdFrame.validate(frame)

    baseline = process_memory()

    assert {:error, %Error{code: :invalid_request}} =
             WireCompression.decode_json(frame,
               decoded_limit: @decoded_limit,
               headers: headers(8),
               expect: :map
             )

    assert process_memory() <= baseline + 256 * 1024
  end

  test "rejects incomplete frames and extra frames" do
    assert {:ok, encoded} = WireCompression.encode_json(%{"a" => 1}, @decoded_limit)
    truncated = binary_part(encoded.body, 0, max(byte_size(encoded.body) - 1, 1))

    assert {:error, %Error{code: :invalid_request}} =
             decode(%{encoded | body: truncated})

    assert {:error, %Error{code: :invalid_request}} =
             decode(%{encoded | body: encoded.body <> encoded.body})
  end

  test "rejects malformed JSON after valid decompression" do
    json = "null"
    compressed = :ezstd.compress(json, 1)

    assert {:error, %Error{code: :invalid_request}} =
             WireCompression.decode_json(compressed,
               decoded_limit: @decoded_limit,
               headers: headers(byte_size(json)),
               expect: :map_or_list
             )

    compressed_text = :ezstd.compress("{not-json", 1)

    assert {:error, %Error{code: :invalid_request}} =
             WireCompression.decode_json(compressed_text,
               decoded_limit: @decoded_limit,
               headers: headers(byte_size("{not-json")),
               expect: :map
             )
  end

  test "rejects a decompression-bomb declaration without allocating past the limit" do
    bomb_length = 50_000_000
    frame = crafted_frame(content_size: bomb_length, fcs_flag: 3, payload: <<1, 2, 3, 4>>)
    baseline = process_memory()

    assert {:error, %Error{code: :payload_too_large}} =
             WireCompression.decode_json(frame,
               decoded_limit: @decoded_limit,
               headers: headers(bomb_length),
               expect: :map
             )

    assert process_memory() <= baseline + 256 * 1024
  end

  test "enforces JSON nesting through StrictDecoder" do
    nested = nested_array(5)
    assert {:ok, encoded} = WireCompression.encode_json(nested, @decoded_limit)

    assert {:error, %Error{code: :resource_limit}} =
             WireCompression.decode_json(encoded.body,
               decoded_limit: @decoded_limit,
               headers: headers(encoded.uncompressed_length),
               expect: :list,
               max_depth: 2
             )
  end

  test "maps NIF failures to typed errors without leaking low-level text" do
    frame = crafted_frame(content_size: 4, fcs_flag: 2, payload: <<9, 9, 9, 9>>, block_type: 2)

    assert {:ok, %{content_size: 4}} = ZstdFrame.validate(frame)

    assert {:error, %Error{code: :invalid_request, message: message}} =
             WireCompression.decode_json(frame,
               decoded_limit: @decoded_limit,
               headers: headers(4),
               expect: :map
             )

    refute message =~ "ezstd"
    refute message =~ "nif"
    refute message =~ "ErlangError"
  end

  test "rejects expected shape mismatches" do
    assert {:ok, encoded} = WireCompression.encode_json([1, 2], @decoded_limit)

    assert {:error, %Error{code: :invalid_request}} =
             decode(encoded, expect: :map)
  end

  defp decode(encoded, opts \\ []) do
    decoded_limit = Keyword.get(opts, :decoded_limit, @decoded_limit)
    expect = Keyword.get(opts, :expect, :map_or_list)

    WireCompression.decode_json(encoded.body,
      decoded_limit: decoded_limit,
      headers: headers(encoded.uncompressed_length),
      expect: expect
    )
  end

  defp headers(length) when is_integer(length) do
    [
      {"content-encoding", "zstd"},
      {"content-type", "application/json"},
      {"x-elixirdb-uncompressed-length", Integer.to_string(length)}
    ]
  end

  defp headers(value) when is_binary(value) do
    [
      {"content-encoding", "zstd"},
      {"content-type", "application/json"},
      {"x-elixirdb-uncompressed-length", value}
    ]
  end

  defp crafted_frame(opts) do
    content_size = Keyword.fetch!(opts, :content_size)
    fcs_flag = Keyword.get(opts, :fcs_flag, 3)
    payload = Keyword.get(opts, :payload, <<0, 1, 2, 3>>)
    block_type = Keyword.get(opts, :block_type, 2)
    descriptor = fcs_flag * 64 + 32

    <<0x28, 0xB5, 0x2F, 0xFD, descriptor>> <>
      fcs_bytes(fcs_flag, content_size) <>
      <<1 + block_type * 2 + byte_size(payload) * 8::unsigned-little-24>> <>
      payload
  end

  defp fcs_bytes(2, size), do: <<size::unsigned-little-32>>
  defp fcs_bytes(3, size), do: <<size::unsigned-little-64>>

  defp nested_array(0), do: [0]
  defp nested_array(depth), do: [nested_array(depth - 1)]

  defp process_memory do
    :erlang.garbage_collect()
    {:memory, memory} = :erlang.process_info(self(), :memory)
    memory
  end
end
