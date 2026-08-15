defmodule VialKeeper.Bench.Select do
  @moduledoc """
  Deterministic top-K selection by a rank key.

  Used to freeze PMC article and Open Images ID sets independently of upstream
  listing order. Equal ranks are broken by a caller-supplied unique id.
  """

  @spec smallest(Enumerable.t(), pos_integer(), (term() -> {binary(), binary()})) :: [term()]
  def smallest(enumerable, count, rank_fun)
      when is_integer(count) and count > 0 and is_function(rank_fun, 1) do
    {set, _size} =
      Enum.reduce(enumerable, {:gb_sets.empty(), 0}, fn item, {set, size} ->
        {rank, tie} = rank_fun.(item)
        key = {rank, tie, item}
        consider(set, size, count, key)
      end)

    set
    |> :gb_sets.to_list()
    |> Enum.sort()
    |> Enum.map(fn {_rank, _tie, item} -> item end)
  end

  defp consider(set, size, count, key) when size < count do
    {:gb_sets.insert(key, set), size + 1}
  end

  defp consider(set, size, _count, {rank, tie, _item} = key) do
    {largest, rest} = :gb_sets.take_largest(set)
    {largest_rank, largest_tie, _} = largest

    if {rank, tie} < {largest_rank, largest_tie} do
      {:gb_sets.insert(key, rest), size}
    else
      {set, size}
    end
  end
end
