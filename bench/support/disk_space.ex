defmodule VialKeeper.Bench.DiskSpace do
  @moduledoc """
  Fail-closed free-space queries for the filesystem that holds the benchmark root.
  """

  @df "/usr/bin/df"
  @gib 1024 * 1024 * 1024

  @spec available_bytes(Path.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def available_bytes(path) when is_binary(path) do
    target = existing_path(path)

    case System.cmd(@df, ["-B1", "--output=avail", target], stderr_to_stdout: true) do
      {output, 0} -> parse_avail(output)
      {output, status} -> {:error, {:df_failed, status, String.trim(output)}}
    end
  rescue
    error in [ErlangError, ArgumentError] ->
      {:error, {:df_unavailable, Exception.message(error)}}
  end

  @spec required_bytes(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def required_bytes(source_bytes, working_bytes)
      when is_integer(source_bytes) and source_bytes >= 0 and is_integer(working_bytes) and
             working_bytes >= 0 do
    base = source_bytes + working_bytes
    reserve = max(10 * @gib, div(base * 15, 100))
    base + reserve
  end

  @spec preflight(Path.t(), non_neg_integer()) :: :ok | {:error, binary()}
  def preflight(path, required) when is_binary(path) and is_integer(required) and required >= 0 do
    case available_bytes(path) do
      {:ok, available} when available >= required ->
        :ok

      {:ok, available} ->
        {:error,
         "insufficient free space: need #{required} bytes, #{available} bytes available on #{path}"}

      {:error, reason} ->
        {:error, "could not query free space for #{path}: #{inspect(reason)}"}
    end
  end

  defp existing_path(path) do
    if File.exists?(path), do: path, else: existing_path(Path.dirname(path))
  end

  defp parse_avail(output) do
    case output |> String.split("\n", trim: true) |> Enum.map(&String.trim/1) do
      ["Avail", digits] -> parse_digits(digits)
      [digits] -> parse_digits(digits)
      _ -> {:error, {:df_unparseable, output}}
    end
  end

  defp parse_digits(digits) do
    case Integer.parse(digits) do
      {value, ""} when value >= 0 -> {:ok, value}
      _ -> {:error, {:df_unparseable, digits}}
    end
  end
end
