defmodule VialKeeper.Bench.FTSTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Bench.{Beir, FTS}
  alias VialKeeper.Storage.SQLite.Adapter

  setup do
    dir = Path.join(System.tmp_dir!(), "vk-fts-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(dir, "qrels"))

    File.write!(Path.join(dir, "corpus.jsonl"), """
    {"_id":"d1","title":"covid vaccine","text":"mrna vaccine trial","metadata":{}}
    {"_id":"d2","title":"cats","text":"unrelated feline","metadata":{}}
    {"_id":"d3","title":"covid treatment","text":"antiviral covid therapy","metadata":{}}
    """)

    File.write!(Path.join(dir, "queries.jsonl"), """
    {"_id":"q1","text":"covid vaccine"}
    """)

    File.write!(Path.join(dir, "qrels/test.tsv"), """
    q1\td1\t2
    q1\td3\t1
    """)

    db = Path.join(dir, "database.sqlite3")
    {:ok, adapter} = Adapter.create(db, %{storage_mode: :disk})

    on_exit(fn ->
      _ = Adapter.close(adapter)
    end)

    {:ok, adapter: adapter, dataset: dir}
  end

  test "adapter ingest, index, and quality stay on the SQLite FTS seam", %{
    adapter: adapter,
    dataset: dataset
  } do
    results = FTS.measure_adapter(adapter, dataset, warmup: 0, iterations: 1)

    assert results["ingest"]["documents"] == 3
    assert results["fts_build"]["index_id"]
    assert results["quality"]["mode"] == "all"
    assert results["quality"]["ndcg@10"] > 0.0
    assert results["quality"]["recall@10"] > 0.0
    assert is_map(results["latency"]["first_pass"])

    {:ok, corpus} = Beir.find_file(dataset, "corpus.jsonl")
    assert File.regular?(corpus)
  end
end
