defmodule ElixirDB.StorageAdapter.QueryContractTest do
  @moduledoc """
  Dual-backend query contract: product order and explain stay backend-neutral
  even when candidates arrive disordered (Memory shuffles bounded-scan rows).
  """

  for {name, adapter_module} <- [
        {"SQLite", ElixirDB.Storage.SQLite.Adapter},
        {"Memory", ElixirDB.Storage.Memory.Adapter}
      ] do
    defmodule Module.concat([ElixirDB.StorageAdapter, "#{name}QueryContractTest"]) do
      use ElixirDB.Storage.AdapterCase, adapter: adapter_module

      alias ElixirDB.Storage.Ports.Access

      test "selector queries return canonical document-id order", %{adapter: adapter} do
        for {id, body} <- [
              {"zeta", %{"kind" => "task", "priority" => 2}},
              {"alpha", %{"kind" => "task", "priority" => 1}},
              {"mu", %{"kind" => "note", "priority" => 9}}
            ] do
          assert {:ok, _} =
                   @adapter.apply_local_mutation(adapter, %{
                     operation: :put,
                     document_id: id,
                     body: body
                   })
        end

        assert {:ok, %{results: results, plan_kind: :bounded_scan}} =
                 @adapter.execute_query(adapter, %{
                   selector: %{"/kind" => "task"},
                   limit: 10
                 })

        assert Enum.map(results, & &1.id) == ["alpha", "zeta"]
      end

      test "explicit sort order is identical across backends", %{adapter: adapter} do
        for {id, priority} <- [{"c", 3}, {"a", 1}, {"b", 2}] do
          assert {:ok, _} =
                   @adapter.apply_local_mutation(adapter, %{
                     operation: :put,
                     document_id: id,
                     body: %{"priority" => priority}
                   })
        end

        assert {:ok, %{results: results}} =
                 @adapter.execute_query(adapter, %{
                   selector: %{},
                   sort: [%{"path" => "/priority", "direction" => "asc"}],
                   limit: 10
                 })

        assert Enum.map(results, & &1.id) == ["a", "b", "c"]
      end

      test "after_id pagination follows product order", %{adapter: adapter} do
        for id <- ["doc-c", "doc-a", "doc-b"] do
          assert {:ok, _} =
                   @adapter.apply_local_mutation(adapter, %{
                     operation: :put,
                     document_id: id,
                     body: %{"ok" => true}
                   })
        end

        assert {:ok, %{results: [%{id: "doc-a"}], has_more: true}} =
                 @adapter.execute_query(adapter, %{selector: %{}, limit: 1})

        assert {:ok, %{results: [%{id: "doc-b"}], has_more: true}} =
                 @adapter.execute_query(adapter, %{
                   selector: %{},
                   limit: 1,
                   after_id: "doc-a"
                 })

        assert {:ok, %{results: [%{id: "doc-c"}], has_more: false}} =
                 @adapter.execute_query(adapter, %{
                   selector: %{},
                   limit: 1,
                   after_id: "doc-b"
                 })
      end

      test "explain omits SQL and physical names", %{adapter: adapter} do
        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "doc",
                   body: %{"state" => "open"}
                 })

        assert {:ok, explanation} =
                 @adapter.explain_query(adapter, %{
                   selector: %{"/state" => "open"},
                   limit: 10
                 })

        encoded = Jason.encode!(explanation)
        refute encoded =~ "SELECT"
        refute encoded =~ "physical_name"
        refute encoded =~ "sqlite"
        refute encoded =~ "rowid"
        assert explanation.plan_kind == :bounded_scan
        assert explanation.full_scan == true
        assert is_integer(explanation.candidate_count)

        assert %{ready_index_count: 0, candidate_retrieval: :bounded_scan} =
                 explanation.backend_detail
      end

      test "index-candidate port strips physical names from shared definitions", %{
        adapter: adapter
      } do
        context = @adapter.to_context(adapter)
        port = Access.port(context, :index_candidates)

        if @adapter == ElixirDB.Storage.SQLite.Adapter do
          assert {:ok, _} =
                   @adapter.create_index(adapter, %{
                     "name" => "by-state",
                     "type" => "structured",
                     "fields" => [
                       %{"path" => "/state", "type" => "string", "direction" => "asc"}
                     ]
                   })

          assert {:ok, indexes} = port.list_indexes(context)
          assert indexes != []

          for index <- indexes do
            refute Map.has_key?(index, "physical_name")
            refute Map.has_key?(index, :physical_name)
            refute Map.has_key?(index, "_metadata")
            refute Map.has_key?(index, :_metadata)
            refute Map.has_key?(index, :backend_meta)
          end

          refute Jason.encode!(indexes) =~ "physical_name"

          assert {:ok, raw} = @adapter.list_indexes(adapter)
          assert is_binary(get_in(hd(raw), ["_metadata", "physical_name"]))
        else
          assert {:ok, []} = port.list_indexes(context)
        end
      end

      test "field projection and deleted winners stay consistent", %{adapter: adapter} do
        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "keep",
                   body: %{"title" => "Keep", "secret" => "no"}
                 })

        assert {:ok, %{revision: drop_revision}} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :put,
                   document_id: "drop",
                   body: %{"title" => "Drop"}
                 })

        assert {:ok, _} =
                 @adapter.apply_local_mutation(adapter, %{
                   operation: :delete,
                   document_id: "drop",
                   if_revision: drop_revision
                 })

        assert {:ok, %{results: [%{id: "keep", fields: fields}]}} =
                 @adapter.execute_query(adapter, %{
                   selector: %{},
                   fields: ["/title"],
                   limit: 10
                 })

        assert fields == %{"/title" => "Keep"}
      end
    end
  end
end
