defmodule VialKeeper.Query.PrefixBounds do
  @moduledoc "Safe Unicode-scalar bounds for exact string-prefix candidates."

  @type t :: %{lower: binary(), upper: binary() | nil}

  @spec bounds(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def bounds(prefix) when is_binary(prefix) do
    cond do
      prefix == "" ->
        {:error, VialKeeper.Error.invalid_request("prefix must be non-empty")}

      not String.valid?(prefix) ->
        {:error, VialKeeper.Error.invalid_request("prefix must be valid UTF-8")}

      true ->
        scalars = String.to_charlist(prefix)
        {:ok, %{lower: prefix, upper: successor_prefix(scalars)}}
    end
  end

  def bounds(_), do: {:error, VialKeeper.Error.invalid_request("prefix must be a string")}

  @doc "Alias for `bounds/1`."
  @spec calculate(binary()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def calculate(prefix), do: bounds(prefix)

  defp successor_prefix(scalars) do
    scalars
    |> Enum.with_index()
    |> Enum.reverse()
    |> Enum.find_value(fn {scalar, index} ->
      case next_scalar(scalar) do
        nil ->
          nil

        successor ->
          scalars
          |> Enum.take(index)
          |> Kernel.++([successor])
          |> to_string()
      end
    end)
  end

  defp next_scalar(0x10FFFF), do: nil
  defp next_scalar(0xD7FF), do: 0xE000
  defp next_scalar(scalar) when (scalar + 1) in 0xD800..0xDFFF, do: 0xE000
  defp next_scalar(scalar), do: scalar + 1
end
