defmodule ElixirDB.JSON.StrictCache do
  @moduledoc "Bounded process-local memoization for strict JSON decoding."

  alias ElixirDB.JSON.StrictDecoder

  @cache_key :elixir_db_json_strict_cache

  @doc "Memoizes strict JSON decoding by exact input bytes, nesting limit, and cache name."
  @spec decode_with_cache(binary(), non_neg_integer(), atom(), pos_integer()) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def decode_with_cache(input, max_depth, cache_name, cache_limit)
      when is_binary(input) and is_integer(max_depth) and max_depth >= 0 and
             is_atom(cache_name) and is_integer(cache_limit) and cache_limit > 0 do
    key = {input, max_depth}
    process_cache_key = {@cache_key, cache_name}
    cache = Process.get(process_cache_key, %{})

    case cached_value(cache, key) do
      {:ok, value} ->
        {:ok, value}

      :miss ->
        decode_and_cache(input, max_depth, process_cache_key, cache, key, cache_limit)
    end
  end

  defp cached_value(cache, key) do
    case Map.fetch(cache, key) do
      {:ok, value} -> {:ok, value}
      :error -> :miss
    end
  end

  defp decode_and_cache(input, max_depth, process_cache_key, cache, key, cache_limit) do
    case StrictDecoder.decode(input, max_depth: max_depth) do
      {:ok, value} = result ->
        Process.put(process_cache_key, put_cache_value(cache, key, value, cache_limit))
        result

      {:error, _error} = error ->
        error
    end
  end

  defp put_cache_value(cache, key, value, cache_limit) when map_size(cache) < cache_limit,
    do: Map.put(cache, key, value)

  defp put_cache_value(cache, key, value, _cache_limit) do
    [evicted_key | _] = Map.keys(cache)
    cache |> Map.delete(evicted_key) |> Map.put(key, value)
  end
end
