defmodule VialKeeper.JSON.CanonicalReferenceTest do
  @moduledoc """
  The optimized canonical encoder against a straightforward reference.

  `Reference` is the iolist encoder that sorts object names by their UTF-16
  code units and escapes every string through the JSON escaper. The optimized
  encoder must produce the same bytes, and the same error or exception, for
  every input.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.JSON.Canonical

  defmodule Reference do
    @moduledoc false
    alias VialKeeper.JSON.Canonical.Fragment

    @safe_integer_max 9_007_199_254_740_991

    def encode(value) do
      {:ok, IO.iodata_to_binary(encode_value(value))}
    rescue
      ArgumentError -> :error
      ArithmeticError -> :error
      FunctionClauseError -> :error
    end

    defp encode_value(nil), do: "null"
    defp encode_value(true), do: "true"
    defp encode_value(false), do: "false"

    defp encode_value(value) when is_integer(value) and abs(value) <= @safe_integer_max,
      do: Integer.to_string(value)

    defp encode_value(value) when is_float(value),
      do: value |> List.wrap() |> Canonical.encode!() |> String.slice(1..-2//1)

    defp encode_value(value) when is_binary(value), do: JSON.encode_to_iodata!(value)
    defp encode_value(%Fragment{json: json}) when is_binary(json), do: json

    defp encode_value(value) when is_list(value),
      do: [?[, Enum.map_intersperse(value, ?,, &encode_value/1), ?]]

    defp encode_value(value) when is_map(value) do
      pairs = Map.to_list(value)
      Enum.each(pairs, fn {key, _member} -> if !is_binary(key), do: raise(ArgumentError) end)

      members =
        pairs
        |> Enum.sort_by(fn {key, _member} ->
          :unicode.characters_to_binary(key, :utf8, {:utf16, :big})
        end)
        |> Enum.map_intersperse(?,, fn {key, member} ->
          [JSON.encode_to_iodata!(key), ?:, encode_value(member)]
        end)

      [?{, members, ?}]
    end

    defp encode_value(_value), do: raise(ArgumentError)
  end

  property "valid JSON values encode to the reference bytes" do
    check all(value <- json_value(), max_runs: 400) do
      assert {:ok, expected} = Reference.encode(value)
      assert Canonical.encode(value) == {:ok, expected}
    end
  end

  property "values with embedded fragments encode to the reference bytes" do
    check all(value <- json_value(), key <- name(), max_runs: 100) do
      wrapped = %{key => Canonical.fragment(Canonical.encode!(value)), "z" => [value]}
      assert Canonical.encode(wrapped) == Reference.encode(wrapped)
    end
  end

  test "names that sort differently as UTF-16 than as UTF-8 bytes" do
    # U+1F600 is the surrogate pair D83D DE00 in UTF-16, so it sorts before
    # U+E000 and U+FF61 there but after them as UTF-8 bytes.
    value = %{"\u{1F600}" => 1, "\uFF61" => 2, "a" => 3, "\uE000" => 4, "\uD7FF" => 5, "a\"" => 6}
    assert {:ok, json} = Canonical.encode(value)
    assert {:ok, json} == Reference.encode(value)
    assert json == ~s({"a":3,"a\\"":6,"\uD7FF":5,"\u{1F600}":1,"\uE000":4,"\uFF61":2})
  end

  test "strings around the seven-byte fast path keep their escapes" do
    for prefix <- ["", "a", "abcdef", "abcdefg", "abcdefghijklmn"],
        special <- ["\"", "\\", "\u0000", "\u001F", "\u007F", "\u00E9", "\u{1F600}", "\u2028"],
        suffix <- ["", "x", "abcdefgh"] do
      string = prefix <> special <> suffix
      value = %{string => string}
      assert Canonical.encode(value) == Reference.encode(value), inspect(string)
    end
  end

  test "invalid values fail the way the reference fails" do
    for value <- [
          %{1 => 1},
          %{"a" => 1, :b => 2},
          %{"\u{1F600}" => 1, :b => 2},
          [1 | 2],
          %{"a" => self()},
          9_007_199_254_740_992,
          %{"a" => %Canonical.Fragment{json: :not_binary}}
        ] do
      assert {:error, _error} = Canonical.encode(value)
      assert Reference.encode(value) == :error
    end
  end

  test "invalid UTF-8 raises the same exception as the reference" do
    for value <- [
          <<0xFF>>,
          %{<<0xC0, 0x80>> => 1},
          %{"a" => <<0xED, 0xA0, 0x80>>},
          ["ok", <<0xFF>>]
        ] do
      expected = catch_error(Reference.encode(value))
      assert catch_error(Canonical.encode(value)) == expected
    end
  end

  defp json_value do
    StreamData.tree(scalar(), fn child ->
      StreamData.one_of([
        StreamData.list_of(child, max_length: 5),
        StreamData.map_of(name(), child, max_length: 6)
      ])
    end)
  end

  defp scalar do
    StreamData.one_of([
      StreamData.constant(nil),
      StreamData.boolean(),
      StreamData.integer(),
      StreamData.member_of([9_007_199_254_740_991, -9_007_199_254_740_991]),
      StreamData.float(),
      text()
    ])
  end

  defp name, do: StreamData.one_of([StreamData.string(:alphanumeric, max_length: 10), text()])

  defp text do
    StreamData.one_of([
      StreamData.string(:printable, max_length: 24),
      StreamData.string(:utf8, max_length: 24),
      StreamData.string(
        Enum.concat([
          0..0x7F,
          [0xE9, 0x2028, 0xD7FF, 0xE000, 0xFF61, 0xFFFF, 0x10000, 0x1F600, 0x10FFFF]
        ]),
        max_length: 24
      )
    ])
  end
end
