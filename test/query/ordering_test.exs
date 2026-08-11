defmodule ElixirDB.Query.OrderingTest do
  @moduledoc "Covers query sort values and continuation cursor ordering."

  use ExUnit.Case, async: true

  alias ElixirDB.Query.Ordering

  test "preserves missing, null, scalar, and document-id ordering" do
    documents = [
      %{id: "missing", revision: "r", body: %{}},
      %{id: "null", revision: "r", body: %{"value" => nil}},
      %{id: "two", revision: "r", body: %{"value" => 2}},
      %{id: "one", revision: "r", body: %{"value" => 1}}
    ]

    sorted =
      Enum.sort(
        documents,
        &(Ordering.compare_documents(&1, &2, [%{path: "/value", direction: "asc"}]) == :lt)
      )

    assert Enum.map(sorted, & &1.id) == ["one", "two", "null", "missing"]
    assert Ordering.compare_documents(%{id: "a", body: %{}}, %{id: "b", body: %{}}, []) == :lt
  end

  test "cursor comparison accepts decoded string-key and atom-key cursors" do
    sort = [%{path: "/priority", direction: "desc"}]
    document = %{id: "next", body: %{"priority" => 3}}

    string_cursor = %{"sort" => [%{"present" => true, "value" => 4}], "id" => "first"}
    atom_cursor = %{sort: [%{"present" => true, "value" => 4}], id: "first"}

    assert Ordering.compare_cursor(document, string_cursor, sort) == :gt
    assert Ordering.compare_cursor(document, atom_cursor, sort) == :gt
  end

  test "ordering keys preserve full-text rank only for the empty sort" do
    ranked = %{id: "ranked", body: %{}, rank: -1.5}
    other = %{id: "other", body: %{}, rank: -2.0}

    assert Ordering.ordering_key(ranked, []) == %{"sort" => [], "rank" => -1.5, "id" => "ranked"}
    assert Ordering.compare_cursor(ranked, Ordering.ordering_key(other, []), []) == :gt
  end
end
