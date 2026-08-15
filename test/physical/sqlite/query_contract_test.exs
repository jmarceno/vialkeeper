defmodule VialKeeper.Storage.SQLite.QueryContractTest do
  use VialKeeper.Storage.Contracts.Query, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.Ports.Access

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
  end
end
