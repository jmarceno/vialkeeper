defmodule VialKeeper.Storage.ImportIdempotencePropertiesTest do
  @moduledoc "Replaying an already-applied valid revision chain is a no-op."

  use VialKeeper.Storage.AdapterCase, adapter: VialKeeper.Storage.Memory.Adapter
  use ExUnitProperties

  alias VialKeeper.ModelGenerators
  alias VialKeeper.Storage.AdapterCase
  alias VialKeeper.TestRevisionId, as: Id

  property "identical imports insert no further revisions and keep the winner", %{
    adapter: adapter
  } do
    check all(
            document_id <- ModelGenerators.document_id(),
            root_body <- ModelGenerators.document_body(),
            leaf_body <- ModelGenerators.document_body(),
            max_runs: 15
          ) do
      document_id = "#{document_id}-#{System.unique_integer([:positive])}"
      {:ok, root} = Id.calculate(document_id, nil, false, root_body)
      {:ok, leaf} = Id.calculate(document_id, root, false, leaf_body)

      chain = %{
        document_id: document_id,
        leaf_revision: leaf,
        revisions: [
          AdapterCase.wire_revision(document_id, root, nil, false, root_body),
          AdapterCase.wire_revision(document_id, leaf, root, false, leaf_body)
        ]
      }

      assert {:ok, first} = @adapter.import_revision_chains(adapter, %{chains: [chain]})
      assert first.revisions_inserted == 2
      assert first.documents_changed == 1

      assert {:ok, second} = @adapter.import_revision_chains(adapter, %{chains: [chain]})
      assert second.revisions_inserted == 0
      assert second.documents_changed == 0

      assert {:ok, %{revision: ^leaf, body: ^leaf_body, deleted: false}} =
               @adapter.get_document(adapter, %{document_id: document_id})
    end
  end
end
