defmodule VialKeeper.Bench.Metrics do
  @moduledoc """
  Ranked-retrieval metrics for TREC-style qrels.

  nDCG uses exponential gain `2^rel - 1`. Recall and MAP treat any positive
  grade as relevant. Formulas match `pytrec_eval` for the graded nDCG and
  binary MAP/recall measures used by BEIR.
  """

  @spec ndcg_at([binary()], map(), pos_integer()) :: float()
  def ndcg_at(ranked_ids, qrels, k) when is_list(ranked_ids) and is_map(qrels) and k > 0 do
    gains = ranked_ids |> Enum.take(k) |> Enum.map(&grade(qrels, &1))
    dcg = dcg(gains)
    ideal = qrels |> Map.values() |> Enum.sort(:desc) |> Enum.take(k) |> dcg()

    if ideal == 0.0, do: 0.0, else: dcg / ideal
  end

  @spec recall_at([binary()], map(), pos_integer()) :: float()
  def recall_at(ranked_ids, qrels, k) when is_list(ranked_ids) and is_map(qrels) and k > 0 do
    relevant = relevant_ids(qrels)
    total = MapSet.size(relevant)

    if total == 0 do
      0.0
    else
      retrieved =
        ranked_ids
        |> Enum.take(k)
        |> Enum.count(&MapSet.member?(relevant, &1))

      retrieved / total
    end
  end

  @spec map_at([binary()], map(), pos_integer()) :: float()
  def map_at(ranked_ids, qrels, k) when is_list(ranked_ids) and is_map(qrels) and k > 0 do
    relevant = relevant_ids(qrels)
    total = MapSet.size(relevant)

    if total == 0 do
      0.0
    else
      {sum, _hits} =
        ranked_ids
        |> Enum.take(k)
        |> Enum.with_index(1)
        |> Enum.reduce({0.0, 0}, &accumulate_map(&1, &2, relevant))

      sum / total
    end
  end

  defp accumulate_map({id, rank}, {sum, hits}, relevant) do
    if MapSet.member?(relevant, id) do
      hits = hits + 1
      {sum + hits / rank, hits}
    else
      {sum, hits}
    end
  end

  @spec mean(Enumerable.t()) :: float()
  def mean(values) do
    list = Enum.to_list(values)
    count = length(list)
    if count == 0, do: 0.0, else: Enum.sum(list) / count
  end

  defp grade(qrels, id), do: Map.get(qrels, id, 0)

  defp relevant_ids(qrels) do
    qrels
    |> Enum.filter(fn {_id, grade} -> is_number(grade) and grade > 0 end)
    |> Enum.map(&elem(&1, 0))
    |> MapSet.new()
  end

  defp dcg(gains) do
    gains
    |> Enum.with_index(1)
    |> Enum.reduce(0.0, fn {rel, rank}, acc ->
      acc + (Integer.pow(2, max(rel, 0)) - 1) / :math.log2(rank + 1)
    end)
  end
end
