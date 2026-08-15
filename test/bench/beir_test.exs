defmodule VialKeeper.Bench.BeirTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.{Beir, Metrics}

  setup do
    dir = Path.join(System.tmp_dir!(), "vk-beir-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(dir, "qrels"))

    File.write!(Path.join(dir, "corpus.jsonl"), """
    {"_id":"d1","title":"covid vaccine","text":"mrna","metadata":{"source":"x"}}
    {"_id":"d2","title":"cats","text":"unrelated","metadata":{}}
    {"_id":"d3","title":"covid treatment","text":"antiviral","metadata":{}}
    """)

    File.write!(Path.join(dir, "queries.jsonl"), """
    {"_id":"q1","text":"covid"}
    """)

    File.write!(Path.join(dir, "qrels/test.tsv"), """
    query-id\tcorpus-id\tscore
    q1\td1\t2
    q1\td3\t1
    """)

    {:ok, dir: dir}
  end

  test "parses corpus, queries, and qrels", %{dir: dir} do
    {:ok, corpus} = Beir.find_file(dir, "corpus.jsonl")
    {:ok, queries_path} = Beir.find_file(dir, "queries.jsonl")
    {:ok, qrels_path} = Beir.find_file(dir, "test.tsv")
    {:ok, queries} = Beir.load_queries(queries_path)
    {:ok, qrels} = Beir.load_qrels(qrels_path)

    [row | _] = corpus |> Beir.stream_jsonl() |> Enum.to_list()
    {:ok, id, body} = Beir.document_body(row)
    assert id == "d1"
    assert body["title"] == "covid vaccine"
    assert body["metadata"]["source"] == "x"

    {:ok, "q1", "covid"} = Beir.query_text(hd(queries))
    assert qrels["q1"]["d1"] == 2
  end

  test "evaluator scores a known ranking", %{dir: dir} do
    {:ok, qrels_path} = Beir.find_file(dir, "test.tsv")
    {:ok, qrels} = Beir.load_qrels(qrels_path)
    rel = qrels["q1"]
    ranked = ["d1", "d3", "d2"]

    assert Metrics.ndcg_at(ranked, rel, 10) == 1.0
    assert Metrics.recall_at(ranked, rel, 10) == 1.0
    assert Metrics.map_at(ranked, rel, 100) == 1.0
  end
end
