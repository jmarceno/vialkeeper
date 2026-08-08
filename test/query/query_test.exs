defmodule ElixirDB.Query.QueryTest do
  use ExUnitProperties

  alias ElixirDB.Query.Normalizer
  alias ElixirDB.Query.Planner
  alias ElixirDB.Query.Selector
  alias ElixirDB.Storage.SQLite.Adapter
  alias ElixirDB.Storage.SQLite.Connection
  alias ElixirDB.Storage.SQLite.QueryCompiler
  alias ElixirDB.Storage.SQLite.QueryRunner
  use ExUnit.Case, async: false

  setup do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-query")
    path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})

    on_exit(fn ->
      Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter: adapter}
  end

  test "selector and pointer projection operate on materialized documents", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "a",
               body: %{"type" => "task", "priority" => 3, "title" => "A"}
             })

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "b",
               body: %{"type" => "note", "priority" => 1}
             })

    assert {:ok, %{results: [%{id: "a", fields: fields}]}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               fields: ["/title"],
               limit: 10
             })

    assert fields == %{"/title" => "A"}
  end

  test "adapter rebuilds predicates from the public selector", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "open",
               body: %{"state" => "open"}
             })

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "closed",
               body: %{"state" => "closed"}
             })

    assert {:ok, %{results: [%{id: "open"}]}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/state" => "open"},
               predicate: :match_all,
               limit: 10
             })

    # Foreign predicate-only maps are untrusted public input: the predicate is
    # ignored and an empty selector matches all documents.
    assert {:ok, %{results: results}} =
             Adapter.execute_query(adapter, %{predicate: :match_all, limit: 10})

    assert Enum.map(results, & &1.id) |> Enum.sort() == ["closed", "open"]

    # A client-forged normalization marker must not execute a foreign predicate.
    assert {:ok, %{results: [%{id: "open"}]}} =
             Adapter.execute_query(adapter, %{
               normalized: true,
               selector: %{"/state" => "open"},
               predicate: :match_all,
               limit: 10
             })
  end

  test "full scan is permitted only below scan_threshold", %{adapter: adapter} do
    # QUERY-011: "only when the number of candidate documents is BELOW the configured scan
    # threshold." With scan_threshold = 1000, a database with exactly 1000 candidate docs
    # must require an index; 999 must still scan.
    threshold = 1_000

    for n <- 1..(threshold - 1) do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: :erlang.integer_to_binary(n),
                 body: %{"type" => "note", "priority" => n}
               })
    end

    # below the threshold: selector with no matching index still scans successfully.
    assert {:ok, %{results: results}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "note"},
               limit: 5
             })

    assert [_, _, _, _, _] = results

    # seed exactly one more to reach exactly the threshold (1000 candidate docs).
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: :erlang.integer_to_binary(threshold),
               body: %{"type" => "note", "priority" => threshold}
             })

    assert {:error, %ElixirDB.Error{code: :index_required}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/type" => "note"},
               limit: 5
             })
  end

  test "executes an indexed OR plan once per matching document", %{adapter: adapter} do
    for {document_id, body} <- [
          {"open", %{"status" => "open", "priority" => 1}},
          {"high", %{"status" => "closed", "priority" => 5}},
          {"both", %{"status" => "open", "priority" => 5}},
          {"neither", %{"status" => "closed", "priority" => 1}}
        ] do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: body
               })
    end

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-status",
               "type" => "structured",
               "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-priority",
               "type" => "structured",
               "fields" => [%{"path" => "/priority", "type" => "number", "direction" => "asc"}]
             })

    assert {:ok,
            %{plan_kind: :union, results: results, index_bindings: selected, examined: examined}} =
             Adapter.execute_query(adapter, %{
               selector: %{
                 "$or" => [
                   %{"/status" => "open"},
                   %{"/priority" => %{"$gte" => 5}}
                 ]
               },
               limit: 10
             })

    assert Enum.map(results, & &1.id) |> Enum.sort() == ["both", "high", "open"]
    assert [_, _] = selected
    assert examined == 3
  end

  property "generated indexed candidates are supersets of authoritative matches", %{
    adapter: adapter
  } do
    for definition <- [
          %{
            "name" => "property-status",
            "type" => "structured",
            "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
          },
          %{
            "name" => "property-priority",
            "type" => "structured",
            "fields" => [%{"path" => "/priority", "type" => "number", "direction" => "asc"}]
          }
        ] do
      assert {:ok, _} = Adapter.create_index(adapter, definition)
    end

    assert {:ok, indexes} = Adapter.list_indexes(adapter)

    check all(
            rows <-
              StreamData.list_of(
                StreamData.tuple({
                  StreamData.member_of(["open", "closed", "queued"]),
                  StreamData.integer(0..9)
                }),
                min_length: 1,
                max_length: 8
              ),
            max_runs: 20
          ) do
      batch = "property-#{System.unique_integer([:positive, :monotonic])}"

      documents =
        rows
        |> Enum.with_index()
        |> Enum.map(fn {{status, priority}, index} ->
          %{
            id: "#{batch}-#{index}",
            body: %{"batch" => batch, "status" => status, "priority" => priority}
          }
        end)

      for document <- documents do
        assert {:ok, _} =
                 Adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: document.id,
                   body: document.body
                 })
      end

      request = %{
        selector: %{
          "$and" => [
            %{"/batch" => batch},
            %{"$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 7}}]}
          ]
        }
      }

      assert {:ok, normalized} = Normalizer.normalize(request)
      assert {:ok, plan} = Planner.plan(indexes, normalized)

      candidate_ids =
        plan.scans
        |> Enum.flat_map(fn scan ->
          index = Enum.find(indexes, &(&1["index_id"] == scan["index_id"]))
          assert {:ok, conditions} = QueryCompiler.compile_scan(scan, index["fields"] || [])
          where = ["winning_deleted = 0" | Enum.map(conditions, &elem(&1, 0))] |> Enum.join(" AND ")
          params = Enum.flat_map(conditions, fn {_sql, value} -> List.wrap(value) end)

          assert {:ok, rows} =
                   Connection.query(
                     adapter.conn,
                     "SELECT document_id FROM documents WHERE #{where}",
                     params
                   )

          Enum.map(rows, fn [id] -> id end)
        end)
        |> MapSet.new()

      expected_ids =
        documents
        |> Enum.filter(fn document ->
          assert {:ok, result} = Selector.matches?(document.body, normalized.predicate)
          result
        end)
        |> Enum.map(& &1.id)
        |> MapSet.new()

      assert MapSet.subset?(expected_ids, candidate_ids)
      assert {:ok, %{results: results}} = Adapter.execute_query(adapter, request)
      assert MapSet.new(Enum.map(results, & &1.id)) == expected_ids
    end
  end

  test "query runner enforces the configured execution deadline", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.update_config(adapter, %{"queries" => %{"max_execution_ms" => 1}})

    for n <- 1..128 do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "deadline-#{n}",
                 body: %{"value" => n}
               })
    end

    assert {:ok, request} = Normalizer.normalize(%{selector: %{}})

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             QueryRunner.execute(adapter, request)
  end

  test "executes OR branches sharing one structured index", %{adapter: adapter} do
    for {document_id, status} <- [{"open", "open"}, {"closed", "closed"}, {"queued", "queued"}] do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: %{"status" => status}
               })
    end

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-status",
               "type" => "structured",
               "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{plan_kind: :bounded_scan, results: results, index_bindings: []}} =
             Adapter.execute_query(adapter, %{
               selector: %{"$or" => [%{"/status" => "open"}, %{"/status" => "closed"}]},
               limit: 10
             })

    assert Enum.map(results, & &1.id) |> Enum.sort() == ["closed", "open"]
  end

  test "pushes down null equality without losing null-valued documents", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "null",
               body: %{"value" => nil}
             })

    assert {:ok, _} =
             Adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "missing",
               body: %{}
             })

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-null",
               "type" => "structured",
               "fields" => [%{"path" => "/value", "type" => "null", "direction" => "asc"}]
             })

    assert {:ok, %{results: [%{id: "null"}]}} =
             Adapter.execute_query(adapter, %{
               selector: %{"/value" => %{"$eq" => nil}},
               index: "by-null",
               limit: 10
             })
  end

  test "indexed candidates are a conservative superset of authoritative matches", %{
    adapter: adapter
  } do
    documents = [
      {"task-open", %{"kind" => "task", "status" => "open", "priority" => 1}},
      {"task-high", %{"kind" => "task", "status" => "closed", "priority" => 5}},
      {"note-open", %{"kind" => "note", "status" => "open", "priority" => 4}},
      {"note-low", %{"kind" => "note", "status" => "closed", "priority" => 1}},
      {"missing-status", %{"kind" => "task", "priority" => 9}}
    ]

    for {document_id, body} <- documents do
      assert {:ok, _} =
               Adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: body
               })
    end

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-status",
               "type" => "structured",
               "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-priority",
               "type" => "structured",
               "fields" => [%{"path" => "/priority", "type" => "number", "direction" => "asc"}]
             })

    selector = %{
      "$or" => [
        %{"/status" => "open", "/kind" => "task"},
        %{"/priority" => %{"$gte" => 3}, "/kind" => "task"}
      ]
    }

    assert {:ok, %{predicate: predicate}} = Normalizer.normalize(%{selector: selector})

    expected =
      documents
      |> Enum.filter(fn {_id, body} -> Selector.matches?(body, predicate) == {:ok, true} end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    assert {:ok, %{plan_kind: :union, results: results}} =
             Adapter.execute_query(adapter, %{selector: selector, limit: 10})

    assert Enum.map(results, & &1.id) |> Enum.sort() == expected
  end

  test "explain uses the executable plan and remains storage-neutral", %{adapter: adapter} do
    assert {:ok, _} =
             Adapter.create_index(adapter, %{
               "name" => "by-status",
               "type" => "structured",
               "fields" => [%{"path" => "/status", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, explanation} =
             Adapter.explain_query(adapter, %{
               selector: %{
                 "$or" => [%{"/status" => "open"}, %{"/priority" => %{"$gte" => 3}}]
               }
             })

    assert explanation.plan_kind in [:union, :bounded_scan]
    assert is_binary(explanation.plan_digest)
    assert is_list(explanation.selected_indexes)
    assert is_list(explanation.pushdown_predicates)
    assert is_list(explanation.post_filter_predicates)

    refute inspect(explanation) =~ "SELECT"
    refute inspect(explanation) =~ "physical_name"
    refute inspect(explanation) =~ "sqlite_private"
  end
end
