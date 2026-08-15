defmodule VialKeeper.Storage.SQLite.RevisionTransferContractTest do
  use VialKeeper.Storage.Contracts.RevisionTransfer,
    adapter: VialKeeper.Storage.SQLite.Adapter

  @moduletag :sqlite_physical

  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.Storage.SQLite.TermBlob
  alias VialKeeper.TestRevisionId, as: Id

  test "different content under one revision id is rejected", %{adapter: adapter} do
    {:ok, root} = Id.calculate("doc", nil, false, %{"n" => 0})
    {:ok, leaf} = Id.calculate("doc", root, false, %{"n" => 1})

    assert {:ok, _} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: leaf,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", leaf, root, false, %{"n" => 1})
                   ]
                 }
               ]
             })

    {:ok, [[doc_key]]} =
      Connection.query(
        adapter.conn,
        "SELECT doc_key FROM documents WHERE document_id = ?",
        ["doc"]
      )

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET body_json = ? WHERE doc_key = ? AND revision_id = ?",
               [~s({"n":99}), doc_key, leaf]
             )

    assert {:error, %VialKeeper.Error{code: :integrity_violation, message: message}} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: leaf,
                   revisions: [
                     wire("doc", root, nil, false, %{"n" => 0}),
                     wire("doc", leaf, root, false, %{"n" => 1})
                   ]
                 }
               ]
             })

    assert message =~ "differs" or message =~ "content"
  end

  test "winning ancestor chain ignores a corrupted conflict sibling body", %{adapter: adapter} do
    root_body = %{"n" => 0}
    left_body = %{"n" => 1, "side" => "left"}
    right_body = %{"n" => 1, "side" => "right"}

    {:ok, root} = Id.calculate("doc", nil, false, root_body)
    {:ok, left} = Id.calculate("doc", root, false, left_body)
    {:ok, right} = Id.calculate("doc", root, false, right_body)

    assert {:ok, _} =
             @adapter.import_revision_chains(adapter, %{
               chains: [
                 %{
                   document_id: "doc",
                   leaf_revision: left,
                   revisions: [
                     wire("doc", root, nil, false, root_body),
                     wire("doc", left, root, false, left_body)
                   ]
                 },
                 %{
                   document_id: "doc",
                   leaf_revision: right,
                   revisions: [
                     wire("doc", root, nil, false, root_body),
                     wire("doc", right, root, false, right_body)
                   ]
                 }
               ]
             })

    assert {:ok, %{revision: winner, body: winner_body}} =
             @adapter.get_document(adapter, %{document_id: "doc"})

    sibling = if winner == left, do: right, else: left

    {:ok, [[doc_key]]} =
      Connection.query(
        adapter.conn,
        "SELECT doc_key FROM documents WHERE document_id = ?",
        ["doc"]
      )

    assert :ok =
             Connection.execute(
               adapter.conn,
               "UPDATE revisions SET body_json = ?, body_term = ? WHERE doc_key = ? AND revision_id = ?",
               ["not-json", TermBlob.bind(<<0, 1, 2>>), doc_key, sibling]
             )

    assert {:ok, %{chains: [chain]}} =
             @adapter.get_revision_chains(adapter, %{
               documents: [%{document_id: "doc", leaf_revisions: [winner]}]
             })

    assert Enum.map(chain.revisions, & &1.revision_id) == [root, winner]
    assert Enum.map(chain.revisions, & &1.body) == [root_body, winner_body]
    refute sibling in Enum.map(chain.revisions, & &1.revision_id)
  end
end
