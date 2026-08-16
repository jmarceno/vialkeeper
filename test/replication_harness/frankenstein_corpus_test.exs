Code.require_file(Path.expand("../../demo/replication_harness/frankenstein_corpus.exs", __DIR__))

defmodule VialKeeper.ReplicationHarness.FrankensteinCorpusTest do
  @moduledoc "Covers splitting the bundled Frankenstein markdown into search documents."

  use ExUnit.Case, async: true

  alias VialKeeper.ReplicationHarness.FrankensteinCorpus

  @corpus Path.expand("../../demo/replication_harness/fixtures/frankenstein.md", __DIR__)

  test "splits the bundled book into four letters and twenty-four chapters" do
    assert {:ok, documents} = FrankensteinCorpus.documents(@corpus)
    assert Enum.map(documents, & &1.id) == expected_ids()

    first = hd(documents)
    last = hd(Enum.reverse(documents))

    assert first.body["title"] == "Letter 1"
    assert first.body["part"] == "letter"
    assert first.body["ordinal"] == 1
    assert first.body["text"] =~ "St. Petersburgh"
    assert first.body["excerpt"] =~ "St. Petersburgh"
    refute first.body["text"] =~ "START OF THE PROJECT GUTENBERG"

    assert last.body["title"] == "Chapter 24"
    assert last.body["part"] == "chapter"
    assert last.body["ordinal"] == 24
    assert last.body["text"] =~ "Farewell"
    refute last.body["text"] =~ "END OF THE PROJECT GUTENBERG"
  end

  test "documents! raises when the corpus file is missing" do
    assert_raise ArgumentError, ~r/could not read Frankenstein corpus/, fn ->
      FrankensteinCorpus.documents!("/no/such/frankenstein.md")
    end
  end

  defp expected_ids do
    letters = Enum.map(1..4, &"letter-#{&1}")
    chapters = Enum.map(1..24, &"chapter-#{&1}")
    letters ++ chapters
  end
end
