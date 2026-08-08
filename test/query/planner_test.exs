defmodule ElixirDB.Query.PlannerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.Query.{Normalizer, Plan, Planner, Projection, Regex}

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

  test "plan digest is storage-neutral and preserves ordered bindings" do
    assert {:ok, regex} = Regex.compile("^open$")

    attrs = %{
      kind: :union,
      scans: [
        %{
          branch: 0,
          index_id: "idx-a",
          constraint: {:eq, "open"},
          compiled: regex,
          physical_name: "sqlite_private_name"
        },
        %{
          branch: 1,
          index_id: "idx-b",
          constraint: {:eq, "queued"},
          sql: "private sql"
        }
      ],
      selected_indexes: [
        %{index_id: "idx-a", definition_digest: String.duplicate("a", 64)},
        %{index_id: "idx-b", definition_digest: String.duplicate("b", 64)}
      ],
      sort_compatible?: false,
      pagination: :union
    }

    assert {:ok, plan} = Plan.new(attrs)
    assert [_, _] = Plan.index_bindings(plan)
    refute Map.has_key?(Plan.canonical(plan)["scans"] |> hd(), "physical_name")
    refute Map.has_key?(Plan.canonical(plan)["scans"] |> hd(), "compiled")
    assert is_binary(plan.digest) and byte_size(plan.digest) == 64
  end

  test "enforces plan kind invariants and pagination" do
    binding = %{index_id: "idx-a", definition_digest: String.duplicate("a", 64)}

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Plan.new(%{
               kind: :single,
               scans: [],
               selected_indexes: [],
               sort_compatible?: false,
               pagination: :indexed
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Plan.new(%{
               kind: :union,
               scans: [%{"index_id" => "idx-a"}],
               selected_indexes: [binding],
               sort_compatible?: false,
               pagination: :union
             })

    assert {:ok, _plan} =
             Plan.new(%{
               kind: :bounded_scan,
               scans: [%{"constraint" => "post-filter"}],
               selected_indexes: [],
               sort_compatible?: false,
               pagination: :bounded_scan
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Plan.new(%{
               kind: :bounded_scan,
               scans: [%{"index_id" => "idx-a"}],
               selected_indexes: [],
               sort_compatible?: false,
               pagination: :bounded_scan
             })
  end

  test "strips forbidden runtime keys and rejects unsafe plan keys" do
    binding = %{index_id: "idx-a", definition_digest: String.duplicate("a", 64)}

    assert {:ok, plan} =
             Plan.new(%{
               kind: :single,
               scans: [%{"index_id" => "idx-a", "pid" => self(), "sql" => "private"}],
               selected_indexes: [binding],
               sort_compatible?: false,
               pagination: :indexed
             })

    refute Map.has_key?(hd(plan.scans), "pid")
    refute Map.has_key?(hd(plan.scans), "sql")

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Plan.new(%{
               kind: :single,
               scans: [%{:index_id => "idx-a", "index_id" => "idx-a"}],
               selected_indexes: [binding],
               sort_compatible?: false,
               pagination: :indexed
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Plan.new(%{
               kind: :single,
               scans: [%{"index_id" => "idx-a", "sql" => "one", :sql => "two"}],
               selected_indexes: [binding],
               sort_compatible?: false,
               pagination: :indexed
             })
  end

  test "plan selects one deterministic structured candidate from a normalized predicate" do
    first = structured("idx-status", [{"/status", "string"}])
    second = structured("idx-priority", [{"/priority", "number"}])

    {:ok, request} =
      Normalizer.normalize(%{selector: %{"/status" => "open", "/priority" => %{"$gte" => 3}}})

    assert {:ok, first_plan} = Planner.plan([second, first], request)
    assert {:ok, second_plan} = Planner.plan([first, second], request)
    assert first_plan.kind == :single
    assert first_plan.digest == second_plan.digest
    assert first_plan.selected_indexes == second_plan.selected_indexes
    assert [_] = first_plan.scans
  end

  test "plan unions every indexed OR branch and deduplicates binding order canonically" do
    status = structured("idx-status", [{"/status", "string"}])
    priority = structured("idx-priority", [{"/priority", "number"}])

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{
          "$or" => [
            %{"/status" => "open"},
            %{"/priority" => %{"$gte" => 3}}
          ]
        }
      })

    assert {:ok, plan} = Planner.plan([priority, status], request)
    assert plan.kind == :union
    assert Enum.map(plan.scans, & &1["branch"]) == [0, 1]
    assert Enum.map(plan.selected_indexes, & &1.index_id) == ["idx-status", "idx-priority"]
  end

  test "plan safely falls back when OR branches share one index" do
    status = structured("idx-status", [{"/status", "string"}])

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{
          "$or" => [
            %{"/status" => "open"},
            %{"/status" => "closed"}
          ]
        }
      })

    assert {:ok, plan} = Planner.plan([status], request)
    assert plan.kind == :bounded_scan
    assert plan.selected_indexes == []
  end

  test "plan falls back to bounded scan for an uncovered OR branch" do
    status = structured("idx-status", [{"/status", "string"}])

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{"$or" => [%{"/status" => "open"}, %{"/unindexed" => 7}]}
      })

    assert {:ok, plan} = Planner.plan([status], request)
    assert plan.kind == :bounded_scan
    assert plan.selected_indexes == []
  end

  test "plan digest is invariant under catalog permutations" do
    indexes = [
      structured("idx-status", [{"/status", "string"}]),
      structured("idx-priority", [{"/priority", "number"}]),
      structured("idx-kind", [{"/kind", "string"}])
    ]

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{"$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 3}}]}
      })

    plans =
      [
        indexes,
        Enum.reverse(indexes),
        [Enum.at(indexes, 1), Enum.at(indexes, 2), Enum.at(indexes, 0)],
        [Enum.at(indexes, 2), Enum.at(indexes, 0), Enum.at(indexes, 1)]
      ]
      |> Enum.map(fn order ->
        assert {:ok, plan} = Planner.plan(order, request)
        plan
      end)

    assert Enum.map(plans, & &1.kind) == List.duplicate(:union, length(plans))
    assert Enum.map(plans, & &1.digest) |> Enum.uniq() |> length() == 1

    assert Enum.map(plans, &Enum.map(&1.selected_indexes, fn binding -> binding.index_id end))
           |> Enum.uniq() == [["idx-status", "idx-priority"]]
  end

  test "an enclosing mandatory source covers an otherwise uncovered OR" do
    kind = structured("idx-kind", [{"/kind", "string"}])

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{
          "$and" => [
            %{"/kind" => "task"},
            %{"$or" => [%{"/status" => "open"}, %{"/unindexed" => 7}]}
          ]
        }
      })

    assert {:ok, %{kind: :single, selected_indexes: [%{index_id: "idx-kind"}]}} =
             Planner.plan([kind], request)
  end

  test "an explicit hint cannot cover only one independent OR branch" do
    status = structured("idx-status", [{"/status", "string"}])
    priority = structured("idx-priority", [{"/priority", "number"}])

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{"$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 3}}]},
        index: "idx-status"
      })

    assert {:error, %ElixirDB.Error{code: :invalid_index_hint}} =
             Planner.plan([status, priority], request)
  end

  test "negative-only predicates never use an index and hints cannot fall back" do
    status = structured("idx-status", [{"/status", "string"}])
    {:ok, request} = Normalizer.normalize(%{selector: %{"/status" => %{"$ne" => "closed"}}})

    assert {:ok, %{kind: :bounded_scan}} = Planner.plan([status], request)

    hinted = Map.put(request, :index, "idx-status")
    assert {:error, %ElixirDB.Error{code: :invalid_index_hint}} = Planner.plan([status], hinted)
  end

  test "full-text search produces one full-text binding" do
    full_text = %{
      "index_id" => "idx-text",
      "name" => "notes",
      "type" => "full_text",
      "fields" => ["/title"]
    }

    {:ok, request} =
      Normalizer.normalize(%{
        search: %{index: "notes", text: "replic", mode: "prefix"},
        selector: %{"/status" => "open"}
      })

    assert {:ok, plan} = Planner.plan([full_text], request)
    assert plan.kind == :full_text

    assert plan.selected_indexes == [
             %{
               index_id: "idx-text",
               definition_digest: plan.selected_indexes |> hd() |> Map.fetch!(:definition_digest)
             }
           ]

    assert plan.scans == [%{"branch" => 0, "index_id" => "idx-text", "type" => "full_text"}]
  end

  property "plan digest is invariant across generated catalog orderings" do
    indexes = [
      structured("idx-status", [{"/status", "string"}]),
      structured("idx-priority", [{"/priority", "number"}]),
      structured("idx-kind", [{"/kind", "string"}]),
      structured("idx-created", [{"/created_at", "string"}])
    ]

    {:ok, request} =
      Normalizer.normalize(%{
        selector: %{
          "$or" => [
            %{"/status" => "open"},
            %{"/priority" => %{"$gte" => 3}},
            %{"/kind" => "task"}
          ]
        }
      })

    check all(seed <- StreamData.integer()) do
      order = Enum.sort_by(indexes, &:erlang.phash2({seed, &1["index_id"]}))
      assert {:ok, plan} = Planner.plan(order, request)
      assert plan.digest == elem(Planner.plan(indexes, request), 1).digest
    end
  end
end
