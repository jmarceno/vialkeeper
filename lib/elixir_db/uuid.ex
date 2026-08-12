defmodule ElixirDB.UUID do
  @moduledoc "UUID generation and deterministic document-history identifiers."

  @spec v4() :: binary()
  def v4 do
    <<a::32, b::16, c::16, d::16, e::48>> = :crypto.strong_rand_bytes(16)
    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
    |> String.downcase()
  end

  @doc false
  @spec document_history_id(binary()) :: binary()
  def document_history_id(document_id) when is_binary(document_id) do
    <<a::32, b::16, c::16, d::16, e::48>> =
      :crypto.hash(:sha256, "elixirdb:document-history:" <> document_id)
      |> binary_part(0, 16)

    c = Bitwise.bor(Bitwise.band(c, 0x0FFF), 0x4000)
    d = Bitwise.bor(Bitwise.band(d, 0x3FFF), 0x8000)

    :io_lib.format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b", [a, b, c, d, e])
    |> IO.iodata_to_binary()
    |> String.downcase()
  end
end
