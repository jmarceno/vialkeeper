defmodule VialKeeper.JSON.Canonical do
  @moduledoc "RFC 8785-style canonical JSON for validated JSON values."
  import Bitwise, only: [band: 2, bnot: 1, bxor: 2]

  require VialKeeper.Probe

  alias VialKeeper.Error
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Probe

  @safe_integer_max 9_007_199_254_740_991
  @default_max_depth 100
  @utf16_order_limit 0xE000
  @low_bits 0x01010101010101
  @high_bits 0x80808080808080
  @control_limit_bits @low_bits * 0x20
  @quote_bits @low_bits * ?"
  @backslash_bits @low_bits * ?\\

  # Checks seven bytes at once (SWAR): the word passes when no byte has the
  # high bit set, is below 0x20, or equals a quote or backslash. With the high
  # bits clear, `(x - 0x20..) & ~x & 0x80..` is non-zero exactly when some byte
  # is below 0x20, and the same test on `word ^ byte..` finds a byte equal to
  # that byte.
  defguardp plain_ascii_word?(word)
            when band(word, @high_bits) == 0 and
                   band(band(word - @control_limit_bits, bnot(word)), @high_bits) == 0 and
                   band(
                     band(bxor(word, @quote_bits) - @low_bits, bnot(bxor(word, @quote_bits))),
                     @high_bits
                   ) == 0 and
                   band(
                     band(
                       bxor(word, @backslash_bits) - @low_bits,
                       bnot(bxor(word, @backslash_bits))
                     ),
                     @high_bits
                   ) == 0

  defmodule Fragment do
    @moduledoc """
    JSON text that is already the canonical encoding of a value.

    `VialKeeper.JSON.Canonical.encode/1` embeds it verbatim, so a caller that
    holds a value's canonical JSON can encode a larger structure around it
    without encoding the value again. Decoded JSON can never produce this
    struct, so it cannot be smuggled in through a document body.
    """
    @enforce_keys [:json]
    defstruct [:json]

    @type t :: %__MODULE__{json: binary()}
  end

  @spec encode(term()) :: {:ok, binary()} | {:error, Error.t()}
  def encode(value) do
    Probe.measure :json_canonical_encode do
      {:ok, encode_value(value, <<>>)}
    end
  rescue
    ArgumentError -> {:error, Error.invalid_request("value is not canonical JSON")}
    ArithmeticError -> {:error, Error.invalid_request("value is not canonical JSON")}
    FunctionClauseError -> {:error, Error.invalid_request("value is not canonical JSON")}
  end

  @doc """
  Wraps `json`, which must be exactly `encode/1` of some value, for embedding.

  Encoding a structure that contains the fragment yields the same bytes as
  encoding it with that value in place of the fragment.
  """
  @spec fragment(binary()) :: Fragment.t()
  def fragment(json) when is_binary(json), do: %Fragment{json: json}

  @spec encode!(term()) :: binary()
  def encode!(value) do
    case encode(value) do
      {:ok, result} -> result
      {:error, error} -> raise ArgumentError, error.message
    end
  end

  @doc """
  Recovers the StrictDecoder term for `json` produced by `encode/1`.

  Elixir maps, lists, binaries, booleans, nil, and safe integers already match
  that term, so those values skip a second JSON parse. Floats and over-deep
  trees still round-trip through `StrictDecoder` so `1.0` becomes `1` and depth
  limits stay identical.
  """
  @spec decode_encoded(term(), binary(), keyword()) :: {:ok, term()} | {:error, Error.t()}
  def decode_encoded(value, json, opts \\ [])

  def decode_encoded(value, json, opts) when is_binary(json) and is_list(opts) do
    max_depth = Keyword.get(opts, :max_depth, @default_max_depth)

    case term_kind(value, 0, max_depth) do
      :canonical -> {:ok, value}
      :roundtrip -> StrictDecoder.decode(json, opts)
    end
  end

  def decode_encoded(_value, _json, _opts),
    do: {:error, Error.invalid_request("canonical JSON body must be UTF-8 text")}

  # The encoder appends to one binary accumulator, which the runtime extends in
  # place; this avoids building and then flattening an iolist.
  defp encode_value(nil, acc), do: <<acc::binary, "null">>
  defp encode_value(true, acc), do: <<acc::binary, "true">>
  defp encode_value(false, acc), do: <<acc::binary, "false">>

  defp encode_value(value, acc) when is_integer(value) and abs(value) <= @safe_integer_max,
    do: <<acc::binary, Integer.to_string(value)::binary>>

  defp encode_value(value, acc) when is_float(value),
    do: <<acc::binary, encode_float(value)::binary>>

  defp encode_value(value, acc) when is_binary(value), do: encode_string(value, acc)

  defp encode_value(%Fragment{json: json}, acc) when is_binary(json),
    do: <<acc::binary, json::binary>>

  defp encode_value([], acc), do: <<acc::binary, "[]">>

  defp encode_value([head | tail], acc),
    do: encode_list_tail(tail, encode_value(head, <<acc::binary, ?[>>))

  defp encode_value(value, acc) when is_map(value), do: encode_object(Map.to_list(value), acc)
  defp encode_value(_value, _acc), do: raise(ArgumentError)

  defp encode_list_tail([], acc), do: <<acc::binary, ?]>>

  defp encode_list_tail([head | tail], acc),
    do: encode_list_tail(tail, encode_value(head, <<acc::binary, ?,>>))

  defp encode_list_tail(_improper, _acc), do: raise(ArgumentError)

  # Strings that need no escaping are embedded as they are; anything else goes
  # through the JSON escaper, which also rejects invalid UTF-8.
  defp encode_string(value, acc) do
    if plain_string?(value, 0x110000),
      do: <<acc::binary, ?", value::binary, ?">>,
      else: <<acc::binary, escape_string(value)::binary>>
  end

  defp escape_string(value), do: IO.iodata_to_binary(JSON.encode_to_iodata!(value))

  # True when `value` is valid UTF-8 that JSON emits verbatim (no quote,
  # backslash, or control character) and every code point is below `limit`.
  defp plain_string?(<<word::56, rest::binary>>, limit) when plain_ascii_word?(word),
    do: plain_string?(rest, limit)

  defp plain_string?(<<byte, rest::binary>>, limit)
       when byte >= 0x20 and byte < 0x80 and byte != ?" and byte != ?\\,
       do: plain_string?(rest, limit)

  defp plain_string?(<<codepoint::utf8, rest::binary>>, limit)
       when codepoint >= 0x80 and codepoint < limit,
       do: plain_string?(rest, limit)

  defp plain_string?(<<>>, _limit), do: true
  defp plain_string?(_binary, _limit), do: false

  defp encode_object([], acc), do: <<acc::binary, "{}">>

  defp encode_object(pairs, acc) do
    members =
      case byte_order_members(:lists.keysort(1, pairs)) do
        :utf16 -> utf16_members(pairs)
        members -> members
      end

    encode_members(members, <<acc::binary, ?{>>)
  end

  defp encode_members([{name, member}], acc),
    do: <<encode_value(member, append_name(name, acc))::binary, ?}>>

  defp encode_members([{name, member} | rest], acc),
    do: encode_members(rest, <<encode_value(member, append_name(name, acc))::binary, ?,>>)

  # A plain name is kept as its raw string, `{:json, encoded}` is an already
  # escaped one, and `{:string, name}` is encoded only when it is reached, as
  # its value would be.
  defp append_name(name, acc) when is_binary(name), do: <<acc::binary, ?", name::binary, ?", ?:>>
  defp append_name({:json, encoded}, acc), do: <<acc::binary, encoded::binary, ?:>>
  defp append_name({:string, name}, acc), do: <<encode_string(name, acc)::binary, ?:>>

  # RFC 8785 compares names as UTF-16 code units. UTF-8 byte order matches
  # that order unless a name has a code point at or above U+E000, so members
  # sorted by bytes are used as they are when every name is below it. Names
  # are encoded before any member value, so an object that needs the UTF-16
  # order is re-sorted before a value is encoded.
  defp byte_order_members([]), do: []

  defp byte_order_members([{key, member} | rest]) when is_binary(key) do
    with encoded when encoded != :utf16 <- byte_order_name(key),
         members when members != :utf16 <- byte_order_members(rest) do
      [{encoded, member} | members]
    end
  end

  defp byte_order_members(_pairs), do: raise(ArgumentError)

  defp byte_order_name(key) do
    cond do
      plain_string?(key, @utf16_order_limit) -> key
      byte_order_key?(key) -> {:json, escape_string(key)}
      true -> :utf16
    end
  end

  defp byte_order_key?(<<byte, rest::binary>>) when byte < 0x80, do: byte_order_key?(rest)

  defp byte_order_key?(<<codepoint::utf8, rest::binary>>) when codepoint < @utf16_order_limit,
    do: byte_order_key?(rest)

  defp byte_order_key?(<<>>), do: true
  defp byte_order_key?(_binary), do: false

  defp utf16_members(pairs) do
    pairs
    |> Enum.map(&utf16_sort_pair/1)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {_sort_key, {key, member}} -> {{:string, key}, member} end)
  end

  defp utf16_sort_pair({key, member}) when is_binary(key),
    do: {utf16_key(key), {key, member}}

  defp utf16_sort_pair(_pair), do: raise(ArgumentError)

  defp utf16_key(key), do: :unicode.characters_to_binary(key, :utf8, {:utf16, :big})

  defp encode_float(value) when is_float(value) do
    truncated = trunc(value)

    cond do
      value == 0.0 ->
        "0"

      value == truncated and abs(value) < 1.0e21 ->
        Integer.to_string(truncated)

      true ->
        value
        |> then(&:erlang.float_to_binary(&1, [:short]))
        |> normalize_float()
    end
  end

  defp normalize_float(value) do
    {mantissa, exponent} = split_exponent(value)
    sign = sign_for(mantissa)
    unsigned = String.trim_leading(mantissa, "-")
    {integer, fraction} = split_decimal(unsigned)
    digits = String.trim_trailing(integer <> fraction, "0")
    digits = if digits == "", do: "0", else: digits
    decimal_position = byte_size(integer) + exponent

    format_float_parts(sign, digits, decimal_position)
  end

  defp split_exponent(value) do
    case String.split(value, "e", parts: 2) do
      [mantissa] -> {mantissa, 0}
      [mantissa, exponent] -> {mantissa, String.to_integer(exponent)}
    end
  end

  defp sign_for(mantissa), do: if(String.starts_with?(mantissa, "-"), do: "-", else: "")

  defp split_decimal(unsigned) do
    case String.split(unsigned, ".", parts: 2) do
      [integer] -> {integer, ""}
      [integer, fraction] -> {integer, fraction}
    end
  end

  defp format_float_parts(sign, digits, decimal_position) do
    if decimal_position >= -5 and decimal_position < 22 do
      sign <> decimal_notation(digits, decimal_position)
    else
      sign <> scientific_notation(digits, decimal_position)
    end
  end

  defp decimal_notation(digits, decimal_position) do
    cond do
      decimal_position <= 0 ->
        "0." <> String.duplicate("0", -decimal_position) <> digits

      decimal_position >= byte_size(digits) ->
        digits <> String.duplicate("0", decimal_position - byte_size(digits))

      true ->
        binary_part(digits, 0, decimal_position) <>
          "." <> binary_part(digits, decimal_position, byte_size(digits) - decimal_position)
    end
  end

  defp scientific_notation(digits, decimal_position) do
    exponent_value = decimal_position - 1

    coefficient =
      binary_part(digits, 0, 1) <>
        if(byte_size(digits) > 1,
          do: "." <> binary_part(digits, 1, byte_size(digits) - 1),
          else: ""
        )

    exponent_sign = if exponent_value >= 0, do: "+", else: "-"
    coefficient <> "e" <> exponent_sign <> Integer.to_string(abs(exponent_value))
  end

  defp term_kind(_value, depth, max_depth) when depth > max_depth, do: :roundtrip
  defp term_kind(value, _depth, _max_depth) when is_nil(value) or is_boolean(value), do: :canonical

  defp term_kind(value, _depth, _max_depth)
       when is_integer(value) and abs(value) <= @safe_integer_max,
       do: :canonical

  defp term_kind(value, _depth, _max_depth) when is_float(value), do: :roundtrip

  defp term_kind(value, _depth, _max_depth) when is_binary(value) do
    if valid_utf8?(value), do: :canonical, else: :roundtrip
  end

  defp term_kind(value, depth, max_depth) when is_list(value),
    do: list_kind(value, depth, max_depth)

  defp term_kind(value, depth, max_depth) when is_map(value),
    do: members_kind(:maps.next(:maps.iterator(value)), depth, max_depth)

  defp term_kind(_value, _depth, _max_depth), do: :roundtrip

  defp list_kind([], _depth, _max_depth), do: :canonical

  defp list_kind([head | tail], depth, max_depth) do
    case term_kind(head, depth + 1, max_depth) do
      :canonical -> list_kind(tail, depth, max_depth)
      kind -> kind
    end
  end

  defp members_kind(:none, _depth, _max_depth), do: :canonical

  defp members_kind({key, value, iterator}, depth, max_depth) when is_binary(key) do
    case term_kind(value, depth + 1, max_depth) do
      :canonical -> members_kind(:maps.next(iterator), depth, max_depth)
      kind -> kind
    end
  end

  defp members_kind(_member, _depth, _max_depth), do: :roundtrip

  # Seven ASCII bytes per step, then one code point at a time.
  defp valid_utf8?(<<word::56, rest::binary>>) when band(word, @high_bits) == 0,
    do: valid_utf8?(rest)

  defp valid_utf8?(<<_codepoint::utf8, rest::binary>>), do: valid_utf8?(rest)
  defp valid_utf8?(<<>>), do: true
  defp valid_utf8?(_binary), do: false
end
