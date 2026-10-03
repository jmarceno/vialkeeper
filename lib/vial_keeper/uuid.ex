defmodule VialKeeper.UUID do
  @moduledoc "UUID generation and deterministic document-history identifiers."

  @spec v4() :: binary()
  def v4, do: format_v4(:crypto.strong_rand_bytes(16))

  @doc false
  @spec document_history_id(binary()) :: binary()
  def document_history_id(document_id) when is_binary(document_id) do
    :crypto.hash(:sha256, "vialkeeper:document-history:" <> document_id)
    |> binary_part(0, 16)
    |> format_v4()
  end

  # Sets the version 4 and RFC 4122 variant bits and renders the lowercase
  # 8-4-4-4-12 form. UUIDs are generated per request and per new document,
  # so this avoids `:io_lib.format/2`.
  defp format_v4(<<a::binary-size(4), b::binary-size(2), c::16, d::16, e::binary-size(6)>>) do
    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    <<p1::binary-size(8), p2::binary-size(4), p3::binary-size(4), p4::binary-size(4),
      p5::binary-size(12)>> =
      Base.encode16(<<a::binary, b::binary, c::16, d::16, e::binary>>, case: :lower)

    p1 <> "-" <> p2 <> "-" <> p3 <> "-" <> p4 <> "-" <> p5
  end
end
