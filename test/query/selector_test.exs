defmodule ElixirDB.Query.SelectorTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Query.Regex, as: QueryRegex
  alias ElixirDB.Query.Selector

  test "keeps missing distinct from null and preserves negative truth tables" do
    body = %{"null" => nil, "number" => 10, "text" => "10"}

    assert Selector.matches?(body, {:field, "/null", [{:eq, nil}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/missing", [{:eq, nil}]}) == {:ok, false}
    assert Selector.matches?(body, {:field, "/null", [{:exists, true}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/missing", [{:exists, false}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/missing", [{:ne, 10}]}) == {:ok, false}
    assert Selector.matches?(body, {:not, {:field, "/missing", [{:eq, 10}]}}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/number", [{:ne, 10}]}) == {:ok, false}
    assert Selector.matches?(body, {:field, "/text", [{:ne, 10}]}) == {:ok, true}
  end

  test "evaluates boolean operators and field operators" do
    body = %{
      "state" => "open",
      "priority" => 4,
      "tags" => ["one", "two"],
      "items" => [%{"ok" => true}]
    }

    assert Selector.matches?(
             body,
             {:and,
              [
                {:field, "/state", [{:eq, "open"}]},
                {:field, "/priority", [{:gte, 3}, {:lt, 5}]}
              ]}
           ) == {:ok, true}

    assert Selector.matches?(
             body,
             {:or,
              [
                {:field, "/state", [{:eq, "closed"}]},
                {:field, "/priority", [{:eq, 4}]}
              ]}
           ) == {:ok, true}

    assert Selector.matches?(body, {:field, "/tags", [{:all, ["one", "two"]}]}) == {:ok, true}

    assert Selector.matches?(
             body,
             {:field, "/items", [{:elem_match, {:field, "/ok", [{:eq, true}]}}]}
           ) ==
             {:ok, true}

    assert Selector.matches?(body, {:field, "/priority", [{:mod, 2, 0}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/priority", [{:size, 1}]}) == {:ok, false}
  end

  test "fails safely for a handcrafted zero modulo divisor" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Selector.matches?(%{"priority" => 4}, {:field, "/priority", [{:mod, 0, 0}]})
  end

  test "propagates regex resource exhaustion instead of converting it to a miss" do
    assert {:ok, regex} = QueryRegex.compile("(a+)+$")

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Selector.matches?(
               %{"value" => String.duplicate("a", 1_000) <> "!"},
               {:field, "/value", [{:regex, regex}]}
             )
  end
end
