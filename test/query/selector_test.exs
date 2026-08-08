defmodule ElixirDB.Query.SelectorTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Query.Regex, as: QueryRegex
  alias ElixirDB.Query.Selector

  test "keeps missing distinct from null and preserves negative truth tables" do
    body = %{"null" => nil, "number" => 10, "text" => "10"}

    assert Selector.matches?(body, {:field, "/null", [{:eq, nil}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/missing", [{:eq, nil}]}) == {:ok, false}
    assert Selector.matches?(body, {:field, "/null", [{:exists, true}]}) == {:ok, true}
    assert Selector.matches?(body, {:field, "/null", [{:exists, false}]}) == {:ok, false}
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

  test "covers operator truth tables across missing, null, scalar, array, and object values" do
    assert {:ok, regex} = QueryRegex.compile("^hello")

    cases = [
      {:eq, {:eq, 10}, %{"value" => 10}, true},
      {:eq_null, {:eq, nil}, %{"value" => nil}, true},
      {:eq_missing, {:eq, nil}, %{}, false},
      {:ne, {:ne, 10}, %{"value" => "10"}, true},
      {:ne_missing, {:ne, 10}, %{}, false},
      {:in, {:in, [10, "10"]}, %{"value" => 10}, true},
      {:nin, {:nin, [10]}, %{"value" => "10"}, true},
      {:nin_missing, {:nin, [10]}, %{}, false},
      {:gt, {:gt, 9}, %{"value" => 10}, true},
      {:gte, {:gte, 10}, %{"value" => 10}, true},
      {:lt, {:lt, 11}, %{"value" => 10}, true},
      {:lte, {:lte, 10}, %{"value" => 10}, true},
      {:wrong_ordered_type, {:gt, 9}, %{"value" => "10"}, false},
      {:exists_null, {:exists, true}, %{"value" => nil}, true},
      {:exists_missing, {:exists, false}, %{}, true},
      {:type_null, {:type, :null}, %{"value" => nil}, true},
      {:type_array, {:type, :array}, %{"value" => [1]}, true},
      {:type_object, {:type, :object}, %{"value" => %{}}, true},
      {:type_missing, {:type, :null}, %{}, false},
      {:begins_with, {:begins_with, "hel"}, %{"value" => "hello"}, true},
      {:begins_with_non_string, {:begins_with, "hel"}, %{"value" => [1]}, false},
      {:regex, {:regex, regex}, %{"value" => "hello"}, true},
      {:regex_non_string, {:regex, regex}, %{"value" => 1}, false},
      {:all, {:all, [1, nil]}, %{"value" => [1, nil, 1]}, true},
      {:all_non_array, {:all, [1]}, %{"value" => %{}}, false},
      {:elem_match, {:elem_match, {:field, "/state", [{:eq, "open"}]}},
       %{"value" => [%{"state" => "open"}]}, true},
      {:elem_match_split,
       {:elem_match,
        {:and,
         [
           {:field, "/state", [{:eq, "open"}]},
           {:field, "/priority", [{:gte, 3}]}
         ]}}, %{"value" => [%{"state" => "open"}, %{"priority" => 3}]}, false},
      {:size, {:size, 2}, %{"value" => [1, 2]}, true},
      {:size_non_array, {:size, 2}, %{"value" => "12"}, false},
      {:mod, {:mod, 3, 1}, %{"value" => 10}, true},
      {:mod_non_integral, {:mod, 3, 1}, %{"value" => 10.5}, false}
    ]

    for {label, predicate, body, expected} <- cases do
      assert Selector.matches?(body, {:field, "/value", [predicate]}) == {:ok, expected},
             "unexpected result for #{label}"
    end
  end

  test "covers dense scalar and collection edge tables for every comparable operator" do
    values = [
      {:missing, %{}, false},
      {:null, %{"value" => nil}, false},
      {:boolean, %{"value" => false}, false},
      {:text, %{"value" => "10"}, false},
      {:number, %{"value" => 10}, true},
      {:array, %{"value" => [10]}, false},
      {:object, %{"value" => %{"n" => 10}}, false}
    ]

    for {label, predicate, expected} <- [
          {:eq, {:eq, 10}, [false, false, false, false, true, false, false]},
          {:ne, {:ne, 10}, [false, true, true, true, false, true, true]},
          {:gt, {:gt, 9}, [false, false, false, false, true, false, false]},
          {:in, {:in, [10, "10"]}, [false, false, false, true, true, false, false]},
          {:nin, {:nin, [10]}, [false, true, true, true, false, false, false]},
          {:exists, {:exists, true}, [false, true, true, true, true, true, true]},
          {:begins_with, {:begins_with, "1"}, [false, false, false, true, false, false, false]},
          {:all, {:all, [10]}, [false, false, false, false, false, true, false]},
          {:size, {:size, 1}, [false, false, false, false, false, true, false]},
          {:mod, {:mod, 2, 0}, [false, false, false, false, true, false, false]}
        ] do
      for {{value_label, body, _value_expected}, result} <- Enum.zip(values, expected) do
        assert Selector.matches?(body, {:field, "/value", [predicate]}) == {:ok, result},
               "#{label} failed for #{value_label}"
      end
    end

    for {predicate, expected} <- [
          {{:type, :null}, [false, true, false, false, false, false, false]},
          {{:type, :array}, [false, false, false, false, false, true, false]},
          {{:type, :object}, [false, false, false, false, false, false, true]}
        ] do
      for {{value_label, body, _value_expected}, result} <-
            Enum.zip(values, expected) do
        assert Selector.matches?(body, {:field, "/value", [predicate]}) == {:ok, result},
               "type failed for #{value_label}"
      end
    end
  end

  test "boolean negation keeps missing fields distinct from negative field operators" do
    missing = %{}
    equal = %{"value" => 10}

    assert Selector.matches?(missing, {:field, "/value", [{:ne, 10}]}) == {:ok, false}
    assert Selector.matches?(missing, {:not, {:field, "/value", [{:eq, 10}]}}) == {:ok, true}

    assert Selector.matches?(
             equal,
             {:not,
              {:or,
               [
                 {:field, "/value", [{:eq, 10}]},
                 {:field, "/other", [{:eq, 10}]}
               ]}}
           ) == {:ok, false}
  end
end
