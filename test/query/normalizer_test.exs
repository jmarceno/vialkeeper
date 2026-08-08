defmodule ElixirDB.Query.NormalizerTest do
  use ExUnit.Case, async: true

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

  test "supports the complete Wave 1 operator validation surface" do
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

  test "fingerprints normalized selector source rather than compiled regex state" do
    request = %{selector: %{"/title" => %{"$regex" => "^hello"}}}
    assert {:ok, first} = Normalizer.normalize(request)
    assert {:ok, second} = Normalizer.normalize(request)
    assert first.fingerprint == second.fingerprint
    assert first.predicate != first.selector
  end
end
