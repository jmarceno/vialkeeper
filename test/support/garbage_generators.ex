defmodule ElixirDB.TestSupport.GarbageGenerators do
  @moduledoc """
  Depth-bounded StreamData generators for adversarial public-input testing.
  """

  @max_depth 3
  @max_map_size 8

  @doc "Generates scalar and depth-bounded compound junk terms."
  @spec junk_term() :: StreamData.t(term())
  def junk_term do
    junk_term(@max_depth)
  end

  @doc "Generates depth-bounded maps with mixed atom, string, and integer keys."
  @spec junk_map() :: StreamData.t(map())
  def junk_map do
    junk_map(@max_depth)
  end

  @doc "Generates a valid map with one listed key deleted or replaced."
  @spec near_valid(map(), [term()]) :: StreamData.t(map())
  def near_valid(valid_map, keys) when is_map(valid_map) and is_list(keys) and keys != [] do
    StreamData.bind(StreamData.member_of(keys), fn key ->
      StreamData.one_of([
        StreamData.constant(Map.delete(valid_map, key)),
        StreamData.map(junk_term(), &Map.put(valid_map, key, &1))
      ])
    end)
  end

  @doc "Generates truncations, one-byte flips, and appended-byte mutations."
  @spec mutated_binary(binary()) :: StreamData.t(binary())
  def mutated_binary(binary) when is_binary(binary) do
    mutations = [
      StreamData.map(StreamData.binary(min_length: 1, max_length: 16), &(binary <> &1))
    ]

    mutations =
      if byte_size(binary) == 0 do
        mutations
      else
        [
          truncated_binary(binary),
          flipped_binary(binary)
          | mutations
        ]
      end

    StreamData.one_of(mutations)
  end

  defp junk_term(0), do: scalar_junk()

  defp junk_term(depth) when depth > 0 do
    child = junk_term(depth - 1)

    StreamData.one_of([
      scalar_junk(),
      StreamData.list_of(child, max_length: 6),
      StreamData.map(StreamData.list_of(child, max_length: 4), &List.to_tuple/1),
      junk_map(depth)
    ])
  end

  defp junk_map(depth) when depth > 0 do
    StreamData.map_of(junk_key(), junk_term(depth - 1), max_length: @max_map_size)
  end

  defp scalar_junk do
    StreamData.one_of([
      StreamData.constant(nil),
      StreamData.boolean(),
      StreamData.member_of([:ok, :error, :undefined, :infinity]),
      StreamData.integer(),
      StreamData.member_of([
        -18_446_744_073_709_551_616,
        -9_223_372_036_854_775_809,
        9_223_372_036_854_775_808,
        18_446_744_073_709_551_616
      ]),
      StreamData.member_of([-1.0e308, -1.5, 0.0, 1.5, 1.0e308]),
      StreamData.binary(max_length: 64),
      StreamData.member_of([
        <<0xFF>>,
        <<0xFF, 0xFE>>,
        <<0xC3, 0x28>>,
        <<0xF0, 0x28, 0x8C, 0x28>>
      ])
    ])
  end

  defp junk_key do
    StreamData.one_of([
      StreamData.member_of([:digest, :blob, :length, :now, :unexpected]),
      StreamData.string(:alphanumeric, max_length: 16),
      StreamData.integer(-16..16),
      StreamData.member_of([{:unsupported, 1}, [:unsupported]])
    ])
  end

  defp truncated_binary(binary) do
    StreamData.map(
      StreamData.integer(0..(byte_size(binary) - 1)),
      &binary_part(binary, 0, &1)
    )
  end

  defp flipped_binary(binary) do
    StreamData.map(
      StreamData.tuple({
        StreamData.integer(0..(byte_size(binary) - 1)),
        StreamData.integer(1..255)
      }),
      fn {offset, mask} ->
        prefix = binary_part(binary, 0, offset)
        suffix_offset = offset + 1
        suffix = binary_part(binary, suffix_offset, byte_size(binary) - suffix_offset)
        prefix <> <<Bitwise.bxor(:binary.at(binary, offset), mask)>> <> suffix
      end
    )
  end
end
