defmodule ElixirDB.Attachments.RepresentationTest do
  @moduledoc "Canonical attachment representation trailer codec."

  use ExUnit.Case, async: true

  alias ElixirDB.Attachments.Representation

  @logical_digest :crypto.hash(:sha256, "logical") |> Base.encode16(case: :lower)
  @payload_digest :crypto.hash(:sha256, "payload") |> Base.encode16(case: :lower)

  test "trailer is exactly 92 bytes and big-endian fields round-trip" do
    assert {:ok, raw} =
             Representation.descriptor(%{
               format_version: 1,
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 7,
               payload_length: 7,
               payload_sha256: @logical_digest
             })

    assert {:ok, trailer} = Representation.encode_trailer(raw)
    assert byte_size(trailer) == 92
    assert {:ok, parsed} = Representation.parse_trailer(trailer)
    assert parsed == raw
  end

  test "zstd descriptor round-trips" do
    assert {:ok, descriptor} =
             Representation.descriptor(%{
               encoding: :zstd,
               logical_digest: @logical_digest,
               logical_length: 100,
               payload_length: 40,
               payload_sha256: @payload_digest
             })

    assert {:ok, trailer} = Representation.encode_trailer(descriptor)
    assert {:ok, parsed} = Representation.parse_trailer(trailer)
    assert parsed.encoding == :zstd
    assert parsed.payload_length == 40
    assert parsed.payload_sha256 == @payload_digest
  end

  test "invalid magic, version, encoding, flags, digest, and length are rejected" do
    assert {:ok, descriptor} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 1,
               payload_length: 1,
               payload_sha256: @logical_digest
             })

    assert {:ok, trailer} = Representation.encode_trailer(descriptor)
    <<_magic::binary-size(8), rest::binary>> = trailer

    assert {:error, %{code: :integrity_violation}} =
             Representation.parse_trailer(<<"NOPEBLB0", rest::binary>>)

    corrupted_version = put_trailer_bytes(trailer, 8, <<2::unsigned-big-16>>)
    assert {:error, %{code: :unsupported_format}} = Representation.parse_trailer(corrupted_version)

    corrupted_encoding = put_trailer_bytes(trailer, 10, <<9>>)
    assert {:error, %{code: :unsupported_format}} = Representation.parse_trailer(corrupted_encoding)

    corrupted_flags = put_trailer_bytes(trailer, 11, <<1>>)
    assert {:error, %{code: :unsupported_format}} = Representation.parse_trailer(corrupted_flags)

    assert {:error, %{code: :invalid_request}} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: "not-a-digest",
               logical_length: 1,
               payload_length: 1,
               payload_sha256: @logical_digest
             })

    assert {:error, %{code: :invalid_request}} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: -1,
               payload_length: 1,
               payload_sha256: @logical_digest
             })
  end

  test "raw invariant mismatch is rejected" do
    assert {:error, %{code: :integrity_violation}} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 8,
               payload_length: 7,
               payload_sha256: @logical_digest
             })

    assert {:error, %{code: :integrity_violation}} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 7,
               payload_length: 7,
               payload_sha256: @payload_digest
             })
  end

  test "zstd payload length must be positive" do
    assert {:error, %{code: :integrity_violation}} =
             Representation.descriptor(%{
               encoding: :zstd,
               logical_digest: @logical_digest,
               logical_length: 10,
               payload_length: 0,
               payload_sha256: @payload_digest
             })
  end

  test "file length mismatch is rejected" do
    assert {:ok, descriptor} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 4,
               payload_length: 4,
               payload_sha256: @logical_digest
             })

    assert :ok = Representation.validate_file_size(96, descriptor)

    assert {:error, %{code: :integrity_violation}} =
             Representation.validate_file_size(95, descriptor)
  end

  test "route digest must match logical digest" do
    assert {:ok, descriptor} =
             Representation.descriptor(%{
               encoding: :raw,
               logical_digest: @logical_digest,
               logical_length: 1,
               payload_length: 1,
               payload_sha256: @logical_digest
             })

    assert :ok = Representation.validate_route_digest(@logical_digest, descriptor)
    other = :crypto.hash(:sha256, "other") |> Base.encode16(case: :lower)

    assert {:error, %{code: :invalid_request}} =
             Representation.validate_route_digest(other, descriptor)
  end

  defp put_trailer_bytes(trailer, offset, replacement) do
    size = byte_size(replacement)
    prefix = binary_part(trailer, 0, offset)
    suffix = binary_part(trailer, offset + size, byte_size(trailer) - offset - size)
    prefix <> replacement <> suffix
  end
end
