defmodule VialKeeper.Headers do
  @moduledoc """
  Case-insensitive lookup of a single HTTP header from a map or keyword list.

  Plug, Req, and test fixtures pass headers as lists of tuples or maps, and
  values may be a binary or a non-empty list of binaries. This helper returns
  the first string value for a lower-case header name.
  """

  @spec get(term(), binary()) :: binary() | nil
  def get(headers, name) when is_map(headers) and is_binary(name) do
    Enum.find_value(headers, fn {key, value} ->
      if String.downcase(to_string(key)) == name, do: header_value(value)
    end)
  end

  def get(headers, name) when is_list(headers) and is_binary(name) do
    Enum.find_value(headers, fn
      {key, value} ->
        if String.downcase(to_string(key)) == name, do: header_value(value)

      _ ->
        nil
    end)
  end

  def get(_, _), do: nil

  defp header_value(value) when is_binary(value), do: value
  defp header_value([value | _]) when is_binary(value), do: value
  defp header_value(_), do: nil
end
