defmodule VialKeeper.Search.EngineTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Search.Engine

  @index_id "idx_search_engine_test"
  @definition %{
    "index_id" => @index_id,
    "fields" => ["/title"],
    "tokenization" => %{"strategy" => "unicode_words_v1", "diacritics" => "preserve"}
  }

  setup do
    tables = Engine.new_tables()
    :ok = Engine.put_index(tables, @definition)
    %{tables: tables}
  end

  test "all requires every query token", %{tables: tables} do
    :ok = Engine.refresh(tables, "both", %{"title" => "alpha beta"}, false)
    :ok = Engine.refresh(tables, "one", %{"title" => "alpha only"}, false)

    assert {:ok, [%{id: "both"}]} = Engine.search(tables, @index_id, "alpha beta", "all")
  end

  test "any matches documents with at least one query token", %{tables: tables} do
    :ok = Engine.refresh(tables, "both", %{"title" => "alpha beta"}, false)
    :ok = Engine.refresh(tables, "one", %{"title" => "alpha only"}, false)

    assert {:ok, hits} = Engine.search(tables, @index_id, "alpha beta", "any")
    assert Enum.map(hits, & &1.id) == ["both", "one"]
  end

  test "phrase requires consecutive token positions", %{tables: tables} do
    :ok = Engine.refresh(tables, "ordered", %{"title" => "alpha beta gamma"}, false)
    :ok = Engine.refresh(tables, "reversed", %{"title" => "beta alpha"}, false)

    assert {:ok, [%{id: "ordered"}]} = Engine.search(tables, @index_id, "alpha beta", "phrase")
  end

  test "prefix matches token prefixes and not infixes", %{tables: tables} do
    :ok = Engine.refresh(tables, "replication", %{"title" => "replication checkpoint"}, false)
    :ok = Engine.refresh(tables, "unrelated", %{"title" => "application checkpoint"}, false)

    assert {:ok, [%{id: "replication"}]} =
             Engine.search(tables, @index_id, "checkp replic", "prefix")

    assert {:ok, []} = Engine.search(tables, @index_id, "plica", "prefix")
  end

  test "higher term frequency ranks first", %{tables: tables} do
    :ok = Engine.refresh(tables, "a", %{"title" => "oak"}, false)
    :ok = Engine.refresh(tables, "b", %{"title" => "oak oak"}, false)

    assert {:ok, [%{id: "b", rank: b_rank}, %{id: "a", rank: a_rank}]} =
             Engine.search(tables, @index_id, "oak", "all")

    assert b_rank < a_rank
  end

  test "deleted documents leave the posting lists", %{tables: tables} do
    :ok = Engine.refresh(tables, "doc", %{"title" => "pine"}, false)
    :ok = Engine.refresh(tables, "doc", %{"title" => "pine"}, true)

    assert {:ok, []} = Engine.search(tables, @index_id, "pine", "all")
  end
end
