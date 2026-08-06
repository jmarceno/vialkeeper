defmodule ElixirDB.Query.PlannerTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Query.{Planner, Projection}

  defp structured(id, fields) do
    %{
      "index_id" => id,
      "name" => id,
      "type" => "structured",
      "fields" =>
        Enum.map(fields, fn {path, type} ->
          %{"path" => path, "type" => type, "direction" => "asc"}
        end)
    }
  end

  test "longer equality prefix beats more non-prefix equality matches" do
    # Index A: equality only on trailing fields (2 matches, but prefix length 0)
    short_prefix =
      structured("idx-scattered", [
        {"/a", "string"},
        {"/b", "string"},
        {"/c", "string"}
      ])

    # Index B: contiguous equality prefix of length 2
    long_prefix =
      structured("idx-prefix", [
        {"/status", "string"},
        {"/priority", "number"},
        {"/created_at", "string"}
      ])

    request = %{
      selector: %{
        "/status" => "open",
        "/priority" => 1,
        "/b" => "x",
        "/c" => "y"
      }
    }

    assert {:ok, selected} = Planner.select_index([short_prefix, long_prefix], request)
    assert selected["index_id"] == "idx-prefix"

    # Same outcome regardless of input order
    assert {:ok, selected} = Planner.select_index([long_prefix, short_prefix], request)
    assert selected["index_id"] == "idx-prefix"
  end

  test "range tier beats sort-only when equality prefixes tie" do
    eq_then_range =
      structured("idx-range", [
        {"/status", "string"},
        {"/priority", "number"},
        {"/created_at", "string"}
      ])

    eq_then_sort =
      structured("idx-sort", [
        {"/status", "string"},
        {"/created_at", "string"},
        {"/priority", "number"}
      ])

    request = %{
      selector: %{
        "/status" => "open",
        "/priority" => %{"$gte" => 3}
      },
      sort: [%{path: "/created_at", direction: "asc"}]
    }

    assert {:ok, selected} = Planner.select_index([eq_then_sort, eq_then_range], request)
    assert selected["index_id"] == "idx-range"

    {eq_r, range_r, sort_r} = Planner.score_components(eq_then_range, request)
    {eq_s, range_s, sort_s} = Planner.score_components(eq_then_sort, request)

    assert eq_r == eq_s
    assert range_r == 1
    assert range_s == 0
    assert sort_s > sort_r
  end

  test "explicit hint missing fails with invalid_index_hint" do
    index = structured("tasks", [{"/status", "string"}])

    assert {:error, %ElixirDB.Error{code: :invalid_index_hint}} =
             Planner.select_index([index], %{
               selector: %{"/status" => "open"},
               index: "missing-index"
             })
  end

  test "explicit hint incompatible fails with invalid_index_hint" do
    index = structured("by-status", [{"/status", "string"}])

    assert {:error, %ElixirDB.Error{code: :invalid_index_hint}} =
             Planner.select_index([index], %{
               selector: %{"/priority" => 1},
               index: "by-status"
             })
  end

  test "explicit hint does not silently fall back to another index" do
    compatible = structured("by-status", [{"/status", "string"}])
    other = structured("by-priority", [{"/priority", "number"}])

    assert {:error, %ElixirDB.Error{code: :invalid_index_hint}} =
             Planner.select_index([compatible, other], %{
               selector: %{"/status" => "open"},
               index: "by-priority"
             })
  end

  test "logical index id is the final tie-break" do
    a = structured("aaa", [{"/status", "string"}])
    z = structured("zzz", [{"/status", "string"}])

    request = %{selector: %{"/status" => "open"}}

    assert {:ok, selected} = Planner.select_index([z, a], request)
    assert selected["index_id"] == "aaa"
  end

  test "projection returns body when fields omitted and fields map when listed" do
    document = %{id: "doc-1", revision: "1-abc", body: %{"title" => "Hello", "n" => 1}}

    assert Projection.project(document, %{}) == %{
             id: "doc-1",
             revision: "1-abc",
             body: %{"title" => "Hello", "n" => 1}
           }

    assert Projection.project(document, %{fields: ["/title"]}) == %{
             id: "doc-1",
             revision: "1-abc",
             fields: %{"/title" => "Hello"}
           }
  end
end
