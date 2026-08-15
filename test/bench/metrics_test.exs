defmodule VialKeeper.Bench.MetricsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.Metrics

  # Hand-checked against pytrec_eval's nDCG (exponential gain), Recall, and MAP.
  # ranked = [d1, d2, d3], qrels d1=2, d3=1, d2=0
  @ranked ["d1", "d2", "d3"]
  @qrels %{"d1" => 2, "d3" => 1}

  test "nDCG@10 matches the exponential-gain formula" do
    dcg = 3 / :math.log2(2) + 0 / :math.log2(3) + 1 / :math.log2(4)
    idcg = 3 / :math.log2(2) + 1 / :math.log2(3)
    assert_in_delta Metrics.ndcg_at(@ranked, @qrels, 10), dcg / idcg, 1.0e-12
  end

  test "recall@10 and recall@100 count binary relevance" do
    assert Metrics.recall_at(@ranked, @qrels, 10) == 1.0
    assert Metrics.recall_at(["d1"], @qrels, 10) == 0.5
    assert Metrics.recall_at(["d2"], @qrels, 100) == 0.0
  end

  test "MAP@100 divides by the full relevant set" do
    # hit d1 at rank 1, d3 at rank 3 -> (1/1 + 2/3) / 2
    assert_in_delta Metrics.map_at(@ranked, @qrels, 100), (1.0 + 2 / 3) / 2, 1.0e-12
  end

  test "empty qrels yield zeros" do
    assert Metrics.ndcg_at(@ranked, %{}, 10) == 0.0
    assert Metrics.recall_at(@ranked, %{}, 10) == 0.0
    assert Metrics.map_at(@ranked, %{}, 100) == 0.0
  end
end
