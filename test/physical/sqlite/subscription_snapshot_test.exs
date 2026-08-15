defmodule VialKeeper.StorageAdapter.SubscriptionSnapshotTest do
  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias VialKeeper.Query.SubscriptionRequest

  @config VialKeeper.Config.defaults()

  defp snapshot(adapter, selector, opts) do
    {:ok, normalized} =
      SubscriptionRequest.normalize(%{"query" => %{"selector" => selector}}, @config)

    request =
      normalized
      |> Map.take([:selector, :predicate, :fields])
      |> Map.merge(Map.new(opts))

    @adapter.execute_subscription_snapshot(adapter, request)
  end

  test "rejects sort in subscription snapshot command request", %{adapter: adapter} do
    assert {:error, %VialKeeper.Error{code: :invalid_request, message: message}} =
             @adapter.execute_subscription_snapshot(adapter, %{
               selector: %{"/type" => "task"},
               sort: [%{"/priority" => "desc"}]
             })

    assert message =~ "sort"
  end

  test "rejects max_members above configured ceiling", %{adapter: adapter} do
    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             snapshot(adapter, %{"/type" => "task"}, max_members: 10_000)
  end

  test "returns complete membership up to max_members with exact sequence", %{adapter: adapter} do
    for id <- ["a", "b", "c"] do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: id,
                 body: %{"type" => "task"}
               })
    end

    assert {:ok, %{current_sequence: sequence}} = @adapter.identity(adapter)

    assert {:ok, %{documents: documents, member_ids: member_ids, sequence: ^sequence}} =
             snapshot(adapter, %{"/type" => "task"}, max_members: 10)

    assert Enum.sort(member_ids) == ["a", "b", "c"]
    assert Enum.count(documents) == 3
    assert Enum.all?(documents, &match?(%{id: _, revision: _, body: _}, &1))
  end

  test "rejects membership above max_members without partial results", %{adapter: adapter} do
    for index <- 1..3 do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: "doc-#{index}",
                 body: %{"type" => "task"}
               })
    end

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             snapshot(adapter, %{"/type" => "task"}, max_members: 2)
  end

  test "projection matches ordinary Projection.project", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"type" => "task", "title" => "Hello"}
             })

    {:ok, normalized} =
      SubscriptionRequest.normalize(
        %{"query" => %{"selector" => %{"/type" => "task"}, "fields" => ["/title"]}},
        @config
      )

    request = Map.take(normalized, [:selector, :predicate, :fields])

    assert {:ok, %{documents: [projected]}} =
             @adapter.execute_subscription_snapshot(adapter, Map.put(request, :max_members, 10))

    assert {:ok, %{results: [query_projected]}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               fields: ["/title"],
               limit: 10
             })

    assert projected == query_projected
    assert projected.fields == %{"/title" => "Hello"}
    refute Map.has_key?(projected, :body)
  end

  test "zero matches returns empty snapshot", %{adapter: adapter} do
    assert {:ok, %{documents: [], member_ids: [], sequence: 0}} =
             snapshot(adapter, %{"/type" => "missing"}, max_members: 10)
  end
end
