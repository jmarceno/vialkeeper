defmodule ElixirDB.View.DefinitionTest do
  @moduledoc "Validation tests for declarative view definitions."
  use ExUnit.Case, async: true

  alias ElixirDB.Query.Normalizer
  alias ElixirDB.View.Definition

  @base %{
    "name" => "by-type",
    "key" => [%{"path" => "/type"}],
    "reducer" => "_count"
  }

  test "normalizes a valid count view and produces a stable digest" do
    assert {:ok, first} = Definition.normalize(@base)
    assert {:ok, second} = Definition.normalize(@base)

    assert first.definition_digest == second.definition_digest
    assert is_binary(first.definition_json)
    assert first.name == "by-type"
    assert first.reducer == :_count
    assert first.value == nil
  end

  test "rejects unknown fields" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Definition.normalize(Map.put(@base, "extra", true))
  end

  test "rejects invalid reducer and path expressions" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Definition.normalize(Map.put(@base, "reducer", "_avg"))

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Definition.normalize(%{
               "name" => "bad-path",
               "key" => [%{"path" => "type"}],
               "reducer" => "_count"
             })
  end

  test "requires value for numeric reducers" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Definition.normalize(%{
               "name" => "sum-view",
               "key" => [%{"literal" => "all"}],
               "reducer" => "_sum"
             })
  end

  test "allows map-only views without reducer value requirements" do
    assert {:ok, view} =
             Definition.normalize(%{
               "name" => "map-only",
               "key" => [%{"path" => "/id"}]
             })

    assert view.reducer == nil
    assert view.value == nil
  end

  test "selector is normalized through Query.Normalizer" do
    selector = %{"/type" => %{"$eq" => "post"}}

    assert {:ok, view} =
             Definition.normalize(%{
               "name" => "filtered",
               "selector" => selector,
               "key" => [%{"path" => "/type"}],
               "reducer" => "_count"
             })

    assert {:ok, query} = Normalizer.normalize(%{"selector" => selector})
    assert view.selector == query.selector
  end

  test "rejects query controls in view definitions" do
    for field <- ~w(sort search limit bookmark index) do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               Definition.normalize(Map.put(@base, field, %{}))
    end
  end

  test "accepts structured selectors that Query.Normalizer accepts" do
    assert {:ok, _} =
             Definition.normalize(%{
               "name" => "compound",
               "selector" => %{"$and" => [%{"/type" => %{"$eq" => "a"}}]},
               "key" => [%{"path" => "/type"}],
               "reducer" => "_count"
             })
  end
end
