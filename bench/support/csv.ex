defmodule VialKeeper.Bench.CSV do
  @moduledoc "Quoted CSV line parser for official Open Images and PMC inventory files."

  @spec parse_line(binary()) :: {:ok, [binary()]} | :skip | {:error, binary()}
  def parse_line(line) when is_binary(line) do
    line = line |> String.trim_trailing("\n") |> String.trim_trailing("\r")

    if line == "" do
      :skip
    else
      parse_fields(String.to_charlist(line), [], [], false)
    end
  end

  defp parse_fields([], current, acc, false) do
    {:ok, Enum.reverse([current_to_string(current) | acc])}
  end

  defp parse_fields([], _current, _acc, true), do: {:error, "unterminated CSV quote"}

  defp parse_fields([?" | rest], current, acc, false) do
    parse_fields(rest, current, acc, true)
  end

  defp parse_fields([?" | rest], current, acc, true) do
    case rest do
      [?" | more] -> parse_fields(more, [?" | current], acc, true)
      _ -> parse_fields(rest, current, acc, false)
    end
  end

  defp parse_fields([?, | rest], current, acc, false) do
    parse_fields(rest, [], [current_to_string(current) | acc], false)
  end

  defp parse_fields([char | rest], current, acc, quoted) do
    parse_fields(rest, [char | current], acc, quoted)
  end

  defp current_to_string(current), do: current |> Enum.reverse() |> List.to_string()
end
