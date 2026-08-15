defmodule VialKeeper.JSON.StrictCache do
  @moduledoc "Bounded process-local memoization for strict JSON decoding."

  alias VialKeeper.JSON.StrictDecoder

  @cache_key :vial_keeper_json_strict_cache

  @doc "Memoizes strict JSON decoding by exact input bytes, nesting limit, and cache name."
  @spec decode_with_cache(binary(), non_neg_integer(), atom(), pos_integer()) ::
          {:ok, term()} | {:error, VialKeeper.Error.t()}
  def decode_with_cache(input, max_depth, cache_name, cache_limit)
      when is_binary(input) and is_integer(max_depth) and max_depth >= 0 and
             is_atom(cache_name) and is_integer(cache_limit) and cache_limit > 0 do
    memoize(cache_name, {input, max_depth}, cache_limit, fn ->
      StrictDecoder.decode(input, max_depth: max_depth)
    end)
  end

  @doc false
  @spec memoize(term(), term(), pos_integer(), (-> {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def memoize(cache_name, key, cache_limit, fun)
      when is_integer(cache_limit) and cache_limit > 0 and is_function(fun, 0) do
    process_cache_key = {@cache_key, cache_name}
    cache = Process.get(process_cache_key, %{})

    case Map.fetch(cache, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        case fun.() do
          {:ok, value} = result ->
            Process.put(process_cache_key, put_cache_value(cache, key, value, cache_limit))
            result

          {:error, _error} = error ->
            error
        end
    end
  end

  @doc "Returns a previously decoded value without parsing on a cache miss."
  @spec fetch_cached(binary(), non_neg_integer(), atom()) :: {:ok, term()} | :miss
  def fetch_cached(input, max_depth, cache_name)
      when is_binary(input) and is_integer(max_depth) and max_depth >= 0 and is_atom(cache_name) do
    cache = Process.get({@cache_key, cache_name}, %{})

    case Map.fetch(cache, {input, max_depth}) do
      {:ok, value} -> {:ok, value}
      :error -> :miss
    end
  end

  defp put_cache_value(cache, key, value, cache_limit) when map_size(cache) < cache_limit,
    do: Map.put(cache, key, value)

  defp put_cache_value(cache, key, value, _cache_limit) do
    [evicted_key | _] = Map.keys(cache)
    cache |> Map.delete(evicted_key) |> Map.put(key, value)
  end
end
