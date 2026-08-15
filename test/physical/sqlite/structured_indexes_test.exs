defmodule VialKeeper.StorageAdapter.StructuredIndexesTest do
  alias VialKeeper.Storage.SQLite.QueryCompiler
  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  test "structured index creation, query selection, and delete", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "t1",
               body: %{"type" => "task", "priority" => 1}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "n1",
               body: %{"type" => "note", "priority" => 9}
             })

    assert {:ok, %{"index_id" => index_id, "name" => "by-type"}} =
             @adapter.create_index(adapter, %{
               "name" => "by-type",
               "type" => "structured",
               "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{results: [%{id: "t1"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })

    assert {:ok, %{results: [%{id: "t1"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })

    assert {:ok, indexes} = @adapter.list_indexes(adapter)
    assert Enum.any?(indexes, &(&1["index_id"] == index_id))

    assert {:ok, _} = @adapter.delete_index(adapter, index_id)
    assert {:ok, remaining} = @adapter.list_indexes(adapter)
    refute Enum.any?(remaining, &(&1["index_id"] == index_id))

    assert {:error, %VialKeeper.Error{code: :invalid_index_hint}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/type" => "task"},
               index: "by-type",
               limit: 10
             })
  end

  test "duplicate structured index name with same definition replays", %{adapter: adapter} do
    definition = %{
      "name" => "by-kind",
      "type" => "structured",
      "fields" => [%{"path" => "/kind", "type" => "string", "direction" => "asc"}]
    }

    assert {:ok, first} = @adapter.create_index(adapter, definition)
    assert {:ok, second} = @adapter.create_index(adapter, definition)
    assert first["index_id"] == second["index_id"]
  end

  test "compiling an internally invalid pointer returns an error, not a crash" do
    # A pointer must begin with '/'. This exercises the defensive error path in
    # QueryCompiler.sqlite_path/1 (consumed by index DDL and query SQL) instead of a bare
    # match that would raise MatchError on storage-layer misuse.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             QueryCompiler.sqlite_path("no-leading-slash")
  end

  test "$and selector compiles against structured index fields without raising", %{adapter: adapter} do
    # The planner extracts the positive $and constraint before the scan compiler
    # receives it, so candidate compilation remains independent of raw selectors.
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "t1",
               body: %{"type" => "task", "priority" => 1}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "n1",
               body: %{"type" => "note", "priority" => 9}
             })

    assert {:ok, %{"index_id" => index_id}} =
             @adapter.create_index(adapter, %{
               "name" => "by-type",
               "type" => "structured",
               "fields" => [%{"path" => "/type", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, %{results: [%{id: "t1"}], selected_index: ^index_id}} =
             @adapter.execute_query(adapter, %{
               selector: %{"$and" => [%{"/type" => %{"$eq" => "task"}}]},
               index: "by-type",
               limit: 10
             })
  end

  test "$beginsWith candidate bounds are complete across Unicode and metacharacters", %{
    adapter: adapter
  } do
    prefix_cases = [
      {"ascii", "alpha-value", "alpha"},
      {"accented", "éclair-value", "écl"},
      {"non-latin", "Жизнь-value", "Жиз"},
      {"emoji", "😀rocket-value", "😀"},
      {"quote", ~s("quoted-value), ~s("quo)},
      {"percent", "%percent-value", "%per"},
      {"underscore", "_underscore-value", "_under"},
      {"asterisk", "*asterisk-value", "*ast"},
      {"surrogate-gap", <<0xED, 0x9F, 0xBF>> <> "-tail", <<0xED, 0x9F, 0xBF>>},
      {"maximum-scalar", <<0xF4, 0x8F, 0xBF, 0xBF>> <> "-tail", <<0xF4, 0x8F, 0xBF, 0xBF>>}
    ]

    for {document_id, value, _prefix} <- prefix_cases do
      assert {:ok, _} =
               @adapter.apply_local_mutation(adapter, %{
                 operation: :put,
                 document_id: document_id,
                 body: %{"value" => value}
               })
    end

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "pointer-slash",
               body: %{"a/b" => "special-slash-value"}
             })

    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "pointer-tilde",
               body: %{"a~b" => "special-tilde-value"}
             })

    assert {:ok, _} =
             @adapter.create_index(adapter, %{
               "name" => "by-value",
               "type" => "structured",
               "fields" => [%{"path" => "/value", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, _} =
             @adapter.create_index(adapter, %{
               "name" => "by-pointer-slash",
               "type" => "structured",
               "fields" => [%{"path" => "/a~1b", "type" => "string", "direction" => "asc"}]
             })

    assert {:ok, _} =
             @adapter.create_index(adapter, %{
               "name" => "by-pointer-tilde",
               "type" => "structured",
               "fields" => [%{"path" => "/a~0b", "type" => "string", "direction" => "asc"}]
             })

    for {document_id, _value, prefix} <- prefix_cases do
      assert {:ok, %{results: results}} =
               @adapter.execute_query(adapter, %{
                 selector: %{"/value" => %{"$beginsWith" => prefix}},
                 index: "by-value",
                 limit: 100
               })

      assert Enum.any?(results, &(&1.id == document_id))
    end

    assert {:ok, %{results: results}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/a~1b" => %{"$beginsWith" => "special-"}},
               index: "by-pointer-slash",
               limit: 10
             })

    assert Enum.any?(results, &(&1.id == "pointer-slash"))

    assert {:ok, %{results: results}} =
             @adapter.execute_query(adapter, %{
               selector: %{"/a~0b" => %{"$beginsWith" => "special-"}},
               index: "by-pointer-tilde",
               limit: 10
             })

    assert Enum.any?(results, &(&1.id == "pointer-tilde"))
  end
end
