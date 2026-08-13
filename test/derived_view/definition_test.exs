defmodule ElixirDB.DerivedView.DefinitionTest do
  @moduledoc "Adversarial validation tests for materialized view definitions."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.DerivedView.Definition
  alias ElixirDB.TestSupport.GarbageGenerators

  @source_a "123e4567-e89b-12d3-a456-426614174000"
  @source_b "223e4567-e89b-42d3-b456-426614174001"
  @top_fields ~w(version name sources map reduce group_level options enabled)
  @option_fields ~w(
    max_concurrent_sources
    batch_documents
    retry_base_delay_ms
    retry_max_delay_ms
  )
  @normalize_opts [enforce_host_limits: false]
  @valid_definition %{
    "name" => "sales-by-month",
    "sources" => [@source_a],
    "map" => %{
      "selector" => %{"/kind" => "sale"},
      "key" => [%{"path" => "/month"}],
      "value" => %{"path" => "/amount"}
    }
  }

  test "normalizes a valid definition deterministically" do
    assert {:ok, first} = Definition.normalize(@valid_definition, @normalize_opts)
    assert {:ok, second} = Definition.normalize(@valid_definition, @normalize_opts)

    assert first.definition_digest == second.definition_digest
    assert first.definition_json == second.definition_json
    assert first.sources == [@source_a]
    assert first.reducer == nil
    assert first.group_level == nil
    assert Definition.digest(first) == first.definition_digest
  end

  test "rejects non-object definitions" do
    for definition <- [nil, "definition", [], 1, {:definition}] do
      assert_error_code(definition, :invalid_request)
    end
  end

  test "rejects unknown top-level, map, and option fields" do
    assert_error_code(Map.put(@valid_definition, "unexpected", true), :invalid_request)

    assert_error_code(
      put_in(@valid_definition, ["map", "unexpected"], true),
      :invalid_request
    )

    assert_error_code(
      Map.put(@valid_definition, "options", %{"unexpected" => true}),
      :invalid_request
    )
  end

  test "rejects absent, empty, oversized, non-UTF-8, and non-binary names" do
    invalid_definitions = [
      Map.delete(@valid_definition, "name"),
      Map.put(@valid_definition, "name", ""),
      Map.put(@valid_definition, "name", String.duplicate("n", 129)),
      Map.put(@valid_definition, "name", <<0xFF>>),
      Map.put(@valid_definition, "name", 42)
    ]

    Enum.each(invalid_definitions, &assert_error_code(&1, :invalid_request))
  end

  test "rejects malformed, duplicate, and over-limit source lists" do
    invalid_definitions = [
      Map.delete(@valid_definition, "sources"),
      Map.put(@valid_definition, "sources", []),
      Map.put(@valid_definition, "sources", @source_a),
      Map.put(@valid_definition, "sources", ["not-a-uuid"]),
      Map.put(@valid_definition, "sources", [42]),
      Map.put(@valid_definition, "sources", [@source_a, String.upcase(@source_a)])
    ]

    Enum.each(invalid_definitions, &assert_error_code(&1, :invalid_request))

    assert_error_code(
      Map.put(@valid_definition, "sources", [@source_a, @source_b]),
      :resource_limit,
      max_sources: 1,
      enforce_host_limits: false
    )
  end

  test "rejects malformed map selectors, keys, and values with stable codes" do
    invalid_maps = [
      nil,
      Map.put(@valid_definition["map"], "selector", []),
      Map.put(@valid_definition["map"], "selector", %{"/kind" => self()}),
      Map.put(@valid_definition["map"], "key", []),
      Map.put(@valid_definition["map"], "key", "month"),
      Map.put(@valid_definition["map"], "key", [%{"path" => "month"}]),
      Map.put(@valid_definition["map"], "value", "amount"),
      Map.put(@valid_definition["map"], "value", %{"literal" => %{}})
    ]

    for map_definition <- invalid_maps do
      @valid_definition
      |> Map.put("map", map_definition)
      |> assert_error_code(:invalid_request)
    end
  end

  test "rejects arbitrary reducer values" do
    for reducer <- ["_average", true, 1, [], %{}, {:reduce}] do
      assert_error_code(Map.put(@valid_definition, "reduce", reducer), :invalid_request)
    end
  end

  test "requires group_level exactly when a reducer is present" do
    assert_error_code(Map.put(@valid_definition, "group_level", 0), :invalid_request)

    reduced = Map.put(@valid_definition, "reduce", "_count")
    assert_error_code(reduced, :invalid_request)

    for level <- [-1, 2, "1", nil] do
      assert_error_code(Map.put(reduced, "group_level", level), :invalid_request)
    end

    assert {:ok, %{group_level: 0, reducer: :_count}} =
             Definition.normalize(Map.put(reduced, "group_level", 0), @normalize_opts)
  end

  test "requires each persisted option to be a positive integer" do
    for field <- @option_fields,
        value <- [0, -1, "1"] do
      definition = Map.put(@valid_definition, "options", %{field => value})
      assert_error_code(definition, :invalid_request)
    end
  end

  test "rejects retry base delays above the maximum" do
    definition =
      Map.put(@valid_definition, "options", %{
        "retry_base_delay_ms" => 2,
        "retry_max_delay_ms" => 1
      })

    assert_error_code(definition, :invalid_request)
  end

  test "rejects non-boolean enabled values" do
    assert_error_code(Map.put(@valid_definition, "enabled", "true"), :invalid_request)
  end

  test "digest returns nil for invalid definitions" do
    assert Definition.digest(%{}) == nil
    assert Definition.digest("not-a-definition") == nil
  end

  test "rejects nested structs instead of raising during key validation" do
    definition = Map.put(@valid_definition, "options", MapSet.new([1]))
    assert_error_code(definition, :invalid_request)
  end

  @tag :slow
  property "normalization is total over junk maps" do
    check all(definition <- GarbageGenerators.junk_map(), max_runs: 400) do
      definition
      |> Definition.normalize(@normalize_opts)
      |> assert_typed_result()
    end
  end

  @tag :slow
  property "normalization is total near a valid definition" do
    check all(
            definition <- GarbageGenerators.near_valid(@valid_definition, @top_fields),
            max_runs: 400
          ) do
      definition
      |> Definition.normalize(@normalize_opts)
      |> assert_typed_result()
    end
  end

  defp assert_error_code(definition, code, opts \\ @normalize_opts) do
    assert {:error, %ElixirDB.Error{code: ^code}} = Definition.normalize(definition, opts)
  end

  defp assert_typed_result({:error, %ElixirDB.Error{}}), do: :ok

  defp assert_typed_result({:ok, normalized}) do
    assert %{
             version: 1,
             name: name,
             sources: sources,
             selector: selector,
             key: key,
             options: options,
             options_json: options_json,
             definition_json: definition_json,
             definition_digest: definition_digest,
             enabled: enabled
           } = normalized

    assert is_binary(name)
    assert is_list(sources)
    assert is_map(selector)
    assert is_list(key)
    assert is_map(options)
    assert is_binary(options_json)
    assert is_binary(definition_json)
    assert is_binary(definition_digest)
    assert is_boolean(enabled)
  end
end
