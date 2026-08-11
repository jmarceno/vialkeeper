defmodule ElixirDB.Federation.RuntimeTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Error
  alias ElixirDB.Federation
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    root = ElixirDB.Config.database_root()
    first_path = "federation-runtime-a-#{System.unique_integer([:positive])}.elixirdb"
    second_path = "federation-runtime-b-#{System.unique_integer([:positive])}.elixirdb"

    Enum.each([first_path, second_path], fn path ->
      ElixirDB.TempDatabase.cleanup(Path.join(root, path))
    end)

    assert {:ok, first} = DatabaseCatalog.create(first_path)
    assert {:ok, second} = DatabaseCatalog.create(second_path)

    on_exit(fn ->
      for {identity, path} <- [{first, first_path}, {second, second_path}] do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, first_uuid: first.database_uuid, second_uuid: second.database_uuid}
  end

  test "executes ordinary source queries through catalog admission", %{
    first_uuid: first_uuid,
    second_uuid: second_uuid
  } do
    assert {:ok, _} = Documents.put(first_uuid, %{id: "first", body: %{"value" => 1}})
    assert {:ok, _} = Documents.put(second_uuid, %{id: "second", body: %{"value" => 2}})

    assert {:ok, %{documents: documents, sources: sources, bookmark: nil}} =
             Federation.query(%{
               databases: [first_uuid, second_uuid],
               query: %{
                 selector: %{},
                 sort: [%{path: "/value", direction: "asc"}],
                 limit: 10
               }
             })

    assert Enum.map(documents, & &1.id) == ["first", "second"]

    assert Enum.map(documents, & &1.source_database_uuid) == [first_uuid, second_uuid]
    assert Enum.map(sources, & &1.database_uuid) == [first_uuid, second_uuid]
    assert Enum.all?(sources, &is_integer(&1.sequence))
  end

  test "returns a typed registration error for an unavailable source", %{
    first_uuid: first_uuid
  } do
    missing_uuid = "123e4567-e89b-12d3-a456-426614174099"

    assert {:error, %Error{code: :database_not_registered}} =
             Federation.query(%{
               databases: [first_uuid, missing_uuid],
               query: %{selector: %{}, limit: 1}
             })
  end
end
