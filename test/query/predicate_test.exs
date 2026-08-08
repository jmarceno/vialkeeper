defmodule ElixirDB.Query.PredicateTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Query.Predicate

  test "classifies JSON values and compares exact JSON values" do
    assert Predicate.json_type(nil) == :null
    assert Predicate.json_type(false) == :boolean
    assert Predicate.json_type(1) == :number
    assert Predicate.json_type("one") == :string
    assert Predicate.json_type([]) == :array
    assert Predicate.json_type(%{}) == :object

    assert Predicate.exact_equal?(1, 1.0)
    assert Predicate.exact_equal?(%{"a" => 1, "b" => 2}, %{"b" => 2, "a" => 1})
    refute Predicate.exact_equal?([1, 2], [2, 1])
    refute Predicate.exact_equal?(nil, false)
  end

  test "orders only same-type numbers and strings" do
    assert Predicate.ordered_compare(1, 2) == :lt
    assert Predicate.ordered_compare("b", "a") == :gt
    assert Predicate.ordered_compare(1, "1") == :incomparable
    assert Predicate.same_type?(1, 1.0)
    refute Predicate.same_type?(1, true)
  end

  test "renders and introspects nested predicates without runtime state" do
    predicate = {:and, [{:field, "/status", [{:eq, "open"}]}, {:not, :match_all}]}

    assert Predicate.render(predicate) == %{
             "$and" => [
               %{"$field" => "/status", "predicates" => [%{"op" => "$eq", "value" => "open"}]},
               %{"$not" => %{"$matchAll" => true}}
             ]
           }

    assert Predicate.introspect(predicate).node_count == 5
    assert Predicate.depth(predicate) == 3
  end

  test "post-filter explain keeps $not context and strips only pushed positives" do
    predicate =
      {:and,
       [
         {:field, "/status", [{:eq, "open"}]},
         {:not, {:field, "/priority", [{:eq, 1}]}},
         {:field, "/title", [{:begins_with, "replication"}]}
       ]}

    pushdowns = [%{"path" => "/status", "operator" => "$eq", "value" => "open"}]

    assert Predicate.post_filter_predicates(predicate, pushdowns) == [
             %{
               "op" => "$not",
               "predicate" => %{
                 "$field" => "/priority",
                 "predicates" => [%{"op" => "$eq", "value" => 1}]
               }
             },
             %{
               "path" => "/title",
               "predicate" => %{"op" => "$beginsWith", "value" => "replication"}
             }
           ]
  end
end
