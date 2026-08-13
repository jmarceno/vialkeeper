defmodule ElixirDB.Storage.SQLite.ProcessResultCache do
  @moduledoc """
  Process-dictionary cache for successful `{:ok, term}` query results.

  Writer connections may cache catalog lists across statements. Snapshot
  readers pass `cache?: false` so they re-read inside the current snapshot
  and never populate this cache.
  """

  @type result :: {:ok, term()} | {:error, term()}

  @doc """
  Returns a cached `{:ok, term}` for `key`, or runs `fun` and stores success.

  When `cache?` is false, `fun` always runs and the process dictionary is
  left unchanged.
  """
  @spec fetch(term(), boolean(), (-> result())) :: result()
  def fetch(key, true, fun) when is_function(fun, 0) do
    case Process.get(key) do
      {:ok, _} = cached ->
        cached

      nil ->
        store(key, fun.())
    end
  end

  def fetch(_key, false, fun) when is_function(fun, 0), do: fun.()

  defp store(key, result) do
    if match?({:ok, _}, result), do: Process.put(key, result)
    result
  end
end
