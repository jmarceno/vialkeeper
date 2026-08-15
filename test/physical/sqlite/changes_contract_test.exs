defmodule VialKeeper.Storage.SQLite.ChangesContractTest do
  use VialKeeper.Storage.Contracts.Changes, adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.{Connection, TermBlob}

  test "malformed trusted leaf terms surface as integrity errors", %{adapter: adapter} do
    assert {:ok, _} =
             @adapter.apply_local_mutation(adapter, %{
               operation: :put,
               document_id: "doc",
               body: %{"n" => 1}
             })

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE changes SET leaf_set_term = ? WHERE sequence = 1",
               [TermBlob.bind(<<0, 1, 2>>)]
             )

    assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
             @adapter.read_changes(adapter, %{since: 0, limit: 10})
  end
end
