defmodule VialKeeper.Attachments.RepresentationPropertiesTest do
  @moduledoc "Attachment trailer encode/parse preserves digest, length, and encoding."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Attachments.Representation

  property "raw trailers round-trip and keep matching logical/payload digests" do
    check all(payload <- StreamData.binary(max_length: 64), max_runs: 40) do
      digest = digest(payload)
      length = byte_size(payload)

      assert {:ok, descriptor} =
               Representation.descriptor(%{
                 format_version: 1,
                 encoding: :raw,
                 logical_digest: digest,
                 payload_sha256: digest,
                 logical_length: length,
                 payload_length: length
               })

      assert {:ok, trailer} = Representation.encode_trailer(descriptor)
      assert {:ok, parsed} = Representation.parse_trailer(trailer)
      assert parsed == descriptor
      assert parsed.logical_digest == parsed.payload_sha256
      assert parsed.logical_length == parsed.payload_length
    end
  end

  property "zstd trailers round-trip distinct payload metadata" do
    check all(
            logical <- StreamData.binary(min_length: 1, max_length: 64),
            payload <- StreamData.binary(min_length: 1, max_length: 64),
            max_runs: 25
          ) do
      assert {:ok, descriptor} =
               Representation.descriptor(%{
                 format_version: 1,
                 encoding: :zstd,
                 logical_digest: digest(logical),
                 payload_sha256: digest(payload),
                 logical_length: byte_size(logical),
                 payload_length: byte_size(payload)
               })

      assert {:ok, trailer} = Representation.encode_trailer(descriptor)
      assert {:ok, parsed} = Representation.parse_trailer(trailer)
      assert parsed == descriptor
    end
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
