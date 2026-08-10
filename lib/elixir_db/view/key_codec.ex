defmodule ElixirDB.View.KeyCodec do
  @moduledoc """
  Storage-neutral sortable composite-key codec for declarative views.

  Component type order is `null < false < true < number < string`. Numbers use
  IEEE-754 binary64 sortable big-endian bytes. Strings use UTF-8 with an escaped
  terminator so components cannot prefix-collide.
  """

  alias ElixirDB.View.Number

  @type component :: nil | boolean() | number() | binary()
  @type key :: [component()]

  @null_tag 0x00
  @false_tag 0x01
  @true_tag 0x02
  @number_tag 0x03
  @string_tag 0x04

  @doc "Encodes a key component array into a lexicographically sortable blob."
  @spec encode(key()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def encode(components) when is_list(components) do
    Enum.reduce_while(components, {:ok, []}, fn component, {:ok, acc} ->
      case encode_component(component) do
        {:ok, encoded} -> {:cont, {:ok, [acc, encoded]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, iodata} -> {:ok, IO.iodata_to_binary(iodata)}
      error -> error
    end
  end

  def encode(_),
    do: {:error, ElixirDB.Error.invalid_request("view key must be an array")}

  @doc "Compares two key component arrays using the public ordering contract."
  @spec compare(key(), key()) :: :lt | :eq | :gt
  def compare(left, right) when is_list(left) and is_list(right) do
    case compare_lists(left, right) do
      :lt ->
        :lt

      :gt ->
        :gt

      :eq ->
        if length(left) == length(right), do: :eq, else: compare_int(length(left), length(right))
    end
  end

  @doc "Compares two encoded key blobs."
  @spec compare_encoded(binary(), binary()) :: :lt | :eq | :gt
  def compare_encoded(left, right) when is_binary(left) and is_binary(right) do
    if left == right, do: :eq, else: if(left < right, do: :lt, else: :gt)
  end

  @doc "Returns the encoded prefix for `group_level` components."
  @spec group_prefix(key(), non_neg_integer()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def group_prefix(key, group_level)
      when is_list(key) and is_integer(group_level) and group_level >= 0 do
    if group_level > length(key) do
      {:error, ElixirDB.Error.invalid_request("group_level exceeds key length")}
    else
      encode(Enum.take(key, group_level))
    end
  end

  defp compare_lists([], []), do: :eq

  defp compare_lists([left | rest_left], [right | rest_right]) do
    case compare_component(left, right) do
      :eq -> compare_lists(rest_left, rest_right)
      other -> other
    end
  end

  defp compare_lists([], _), do: :lt
  defp compare_lists(_, []), do: :gt

  defp compare_component(left, right) do
    with {:ok, left_encoded} <- encode_component(left),
         {:ok, right_encoded} <- encode_component(right) do
      compare_encoded(left_encoded, right_encoded)
    end
  end

  defp compare_int(left, right) when left < right, do: :lt
  defp compare_int(left, right) when left > right, do: :gt
  defp compare_int(_, _), do: :eq

  defp encode_component(nil), do: {:ok, <<@null_tag>>}
  defp encode_component(false), do: {:ok, <<@false_tag>>}
  defp encode_component(true), do: {:ok, <<@true_tag>>}

  defp encode_component(value) when is_number(value) do
    case Number.to_binary64(value) do
      {:ok, float} ->
        {:ok, <<@number_tag>> <> sortable_double_bytes(float)}

      :overflow ->
        {:error, ElixirDB.Error.invalid_request("view key number must be finite")}
    end
  end

  defp encode_component(value) when is_binary(value) do
    if String.valid?(value) do
      {:ok, <<@string_tag>> <> escape_string(value) <> <<0, 0>>}
    else
      {:error, ElixirDB.Error.invalid_request("view key string must be valid UTF-8")}
    end
  end

  defp encode_component(_),
    do: {:error, ElixirDB.Error.invalid_request("view key component type is not supported")}

  defp escape_string(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.map(fn
      0 -> <<0, 1>>
      byte -> <<byte>>
    end)
    |> IO.iodata_to_binary()
  end

  defp sortable_double_bytes(value) do
    <<bits::64>> = <<Number.normalize_zero(value)::float>>

    encoded =
      if Bitwise.band(bits, 0x8000_0000_0000_0000) == 0,
        do: Bitwise.bxor(bits, 0x8000_0000_0000_0000),
        else: Bitwise.bnot(bits)

    <<encoded::64-big>>
  end
end
