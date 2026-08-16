defmodule VialKeeper.Bench.Checksums do
  @moduledoc "Streaming checksum helpers for dataset objects."

  @spec md5_file(Path.t()) :: {:ok, binary()} | {:error, term()}
  def md5_file(path) when is_binary(path), do: hash_file(path, :md5)

  @spec sha256_file(Path.t()) :: {:ok, binary()} | {:error, term()}
  def sha256_file(path) when is_binary(path), do: hash_file(path, :sha256)

  @spec md5_iodata(iodata()) :: binary()
  def md5_iodata(data), do: hash_hex(:md5, data)

  @spec sha256_iodata(iodata()) :: binary()
  def sha256_iodata(data), do: hash_hex(:sha256, data)

  @spec canonicalize_md5(binary()) :: {:ok, binary()} | {:error, binary()}
  def canonicalize_md5(value) when is_binary(value) do
    trimmed = String.trim(value)

    if Regex.match?(~r/^[0-9a-f]{32}$/i, trimmed) do
      {:ok, String.downcase(trimmed)}
    else
      case Base.decode64(trimmed) do
        {:ok, raw} when byte_size(raw) == 16 ->
          {:ok, Base.encode16(raw, case: :lower)}

        _ ->
          {:error, "cannot canonicalize MD5 #{inspect(value)}"}
      end
    end
  end

  @spec verify_file(Path.t(), keyword()) :: :ok | {:error, binary()}
  def verify_file(path, opts) when is_binary(path) and is_list(opts) do
    with {:ok, stat} <- stat(path),
         :ok <- verify_size(stat.size, opts[:expected_size]),
         :ok <- verify_hash(path, opts) do
      verify_not_html(path, stat.size, opts)
    end
  end

  defp hash_file(path, algorithm) do
    case File.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        try do
          hash_loop(io, :crypto.hash_init(algorithm))
        after
          File.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp hash_loop(io, acc) do
    case IO.binread(io, 65_536) do
      :eof ->
        {:ok, acc |> :crypto.hash_final() |> Base.encode16(case: :lower)}

      {:error, reason} ->
        {:error, reason}

      data when is_binary(data) ->
        hash_loop(io, :crypto.hash_update(acc, data))
    end
  end

  defp hash_hex(algorithm, data) do
    :crypto.hash(algorithm, data) |> Base.encode16(case: :lower)
  end

  defp stat(path) do
    case File.stat(path) do
      {:ok, stat} -> {:ok, stat}
      {:error, reason} -> {:error, "cannot stat #{path}: #{inspect(reason)}"}
    end
  end

  defp verify_size(_size, nil), do: :ok

  defp verify_size(size, expected) when is_integer(expected) and expected >= 0 do
    if size == expected do
      :ok
    else
      {:error, "byte length mismatch: expected #{expected}, got #{size}"}
    end
  end

  defp verify_hash(path, opts) do
    cond do
      is_binary(opts[:md5]) ->
        compare_md5(path, opts[:md5], opts[:etag])

      is_binary(opts[:sha256]) ->
        compare_sha256(path, opts[:sha256])

      true ->
        :ok
    end
  end

  defp compare_sha256(path, expected) do
    with {:ok, actual} <- hash_file(path, :sha256) do
      if actual == String.downcase(expected) do
        :ok
      else
        {:error, "SHA-256 mismatch for #{path}"}
      end
    end
  end

  defp compare_md5(path, expected, etag) do
    with {:ok, expected} <- canonicalize_md5(expected),
         {:ok, actual} <- hash_file(path, :md5) do
      cond do
        actual == expected ->
          :ok

        etag_md5(etag) == actual ->
          :ok

        true ->
          {:error, "MD5 mismatch for #{path}"}
      end
    end
  end

  defp etag_md5(value) when is_binary(value) do
    case Regex.run(~r/^\s*(?:W\/)?"([0-9a-f]{32})"\s*$/i, value) do
      [_, md5] -> String.downcase(md5)
      _ -> nil
    end
  end

  defp etag_md5(_value), do: nil

  defp verify_not_html(path, size, opts) do
    cond do
      is_binary(opts[:md5]) or is_binary(opts[:sha256]) ->
        :ok

      size > 1_048_576 ->
        :ok

      true ->
        case File.open(path, [:read, :binary, :raw]) do
          {:ok, io} ->
            prefix = IO.binread(io, 256)
            _ = File.close(io)
            reject_html_prefix(prefix)

          {:error, reason} ->
            {:error, "cannot read #{path}: #{inspect(reason)}"}
        end
    end
  end

  defp reject_html_prefix(prefix) when is_binary(prefix) do
    trimmed = prefix |> String.trim_leading() |> String.downcase()

    if String.starts_with?(trimmed, "<!doctype html") or String.starts_with?(trimmed, "<html") do
      {:error, "downloaded object looks like an HTML error page"}
    else
      :ok
    end
  end

  defp reject_html_prefix(_), do: :ok
end
