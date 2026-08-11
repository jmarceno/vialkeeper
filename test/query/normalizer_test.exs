defmodule ElixirDB.Query.NormalizerTest do
  @moduledoc "Covers query selector normalization and generated predicates."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.Query.{Normalizer, Predicate}

  test "normalizes Boolean and field operators into one predicate tree" do
    assert {:ok, normalized} =
             Normalizer.normalize(%{
               selector: %{
                 "$and" => [
                   %{"/status" => %{"$in" => ["open", "queued"]}},
                   %{"$or" => [%{"/priority" => %{"$gte" => 3}}, %{"/done" => true}]}
                 ]
               }
             })

    assert %{selector: selector, predicate: {:and, [_first, {:or, _second}]}} = normalized
    assert selector["$and"] |> is_list()
  end

  test "disambiguates literal, operator, and mixed condition objects" do
    assert {:ok, %{predicate: {:field, "/profile", [{:eq, %{}}]}}} =
             Normalizer.normalize(%{selector: %{"/profile" => %{}}})

    assert {:ok, %{predicate: {:field, "/profile", [{:eq, %{"$kind" => "A"}}]}}} =
             Normalizer.normalize(%{
               selector: %{"/profile" => %{"$eq" => %{"$kind" => "A"}}}
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{"/profile" => %{"$kind" => "A", "$eq" => "A"}}})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{"/profile" => %{"name" => "A", "$eq" => "A"}}})
  end

  test "supports the complete operator validation surface" do
    assert {:ok, normalized} =
             Normalizer.normalize(%{
               selector: %{
                 "/n" => %{
                   "$ne" => 1,
                   "$gt" => 0,
                   "$lte" => 10,
                   "$nin" => [2, 3],
                   "$exists" => true,
                   "$type" => "number",
                   "$beginsWith" => "1",
                   "$regex" => "^1",
                   "$mod" => [2, 0]
                 },
                 "/tags" => %{"$all" => ["a", "b"]},
                 "/items" => %{"$elemMatch" => %{"/state" => "open"}},
                 "/values" => %{"$size" => 2}
               },
               search: %{index: "notes", text: "replic checkp", mode: "prefix"}
             })

    assert normalized.search.mode == "prefix"
    assert {:and, predicates} = normalized.predicate

    assert Enum.any?(predicates, fn
             {:field, "/n", field_predicates} ->
               Enum.any?(field_predicates, &match?({:regex, _}, &1))

             _ ->
               false
           end)
  end

  test "rejects complexity before owner-facing query execution" do
    assert {:ok, _normalized} =
             Normalizer.normalize(%{
               selector: %{"$or" => Enum.map(1..64, &%{"/value" => &1})}
             })

    children = Enum.map(1..65, fn n -> %{"/value" => n} end)

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Normalizer.normalize(%{selector: %{"$or" => children}})

    depth_32 = Enum.reduce(1..30, %{"/value" => 1}, fn _n, selector -> %{"$not" => selector} end)
    assert {:ok, _normalized} = Normalizer.normalize(%{selector: depth_32})

    depth_33 = Enum.reduce(1..31, %{"/value" => 1}, fn _n, selector -> %{"$not" => selector} end)

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Normalizer.normalize(%{selector: depth_33})

    assert {:ok, _normalized} =
             Normalizer.normalize(%{
               selector:
                 Enum.into(1..126, %{}, fn n -> {"/v#{n}", n} end)
                 |> Map.put("$not", %{"/special" => 1})
             })

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Normalizer.normalize(%{
               selector: Enum.into(1..128, %{}, fn n -> {"/v#{n}", n} end)
             })
  end

  test "rejects atom and string keys that collide after stringification" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{:selector => 1, "selector" => 2}})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{"/value" => %{:foo => 1, "foo" => 2}}})
  end

  test "does not treat false selector or sort as absent" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: false})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{sort: false})
  end

  test "counts elemMatch predicates in canonical complexity helpers" do
    predicate = {:field, "/items", [{:elem_match, {:field, "/state", [{:eq, "open"}]}}]}
    assert Predicate.depth(predicate) == 4
    assert Predicate.node_count(predicate) == 4
  end

  test "enforces the elemMatch depth boundary" do
    accepted = nested_elem_match_selector(15)
    rejected = nested_elem_match_selector(16)

    assert {:ok, _} = Normalizer.normalize(%{selector: accepted})

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Normalizer.normalize(%{selector: rejected})
  end

  test "normalizes NOR as exact negation of OR and rejects malformed Boolean arrays" do
    assert {:ok, %{predicate: {:not, {:or, [_first, _second]}}}} =
             Normalizer.normalize(%{
               selector: %{"$nor" => [%{"/state" => "closed"}, %{"/deleted" => true}]}
             })

    for operator <- ["$and", "$or", "$nor"] do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               Normalizer.normalize(%{selector: %{operator => []}})

      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               Normalizer.normalize(%{selector: %{operator => %{"/state" => "open"}}})
    end

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{"$not" => [%{"/state" => "open"}]}})
  end

  test "rejects semantic complexity before dispatching to a database owner" do
    selector = %{"$or" => Enum.map(1..65, &%{"/value" => &1})}

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             ElixirDB.Query.execute("not-a-database", %{selector: selector})
  end

  test "fingerprints normalized selector source rather than compiled regex state" do
    request = %{selector: %{"/title" => %{"$regex" => "^hello"}}}
    assert {:ok, first} = Normalizer.normalize(request)
    assert {:ok, second} = Normalizer.normalize(request)
    assert first.fingerprint == second.fingerprint
    assert first.predicate != first.selector
  end

  property "generated Boolean trees remain bounded by semantic depth" do
    check all(depth <- StreamData.integer(0..6)) do
      assert {:ok, %{predicate: predicate}} =
               Normalizer.normalize(%{selector: boolean_selector(depth)})

      assert Predicate.depth(predicate) <= 32
    end
  end

  defp nested_elem_match_selector(depth) do
    Enum.reduce(1..depth, %{"/value" => 1}, fn _level, selector ->
      %{"/items" => %{"$elemMatch" => selector}}
    end)
  end

  defp boolean_selector(0), do: %{"/value" => 1}

  defp boolean_selector(depth) do
    %{
      "$and" => [
        %{"$or" => [boolean_selector(depth - 1), %{"/other" => %{"$gte" => depth}}]},
        %{"/stable" => true}
      ]
    }
  end
end
