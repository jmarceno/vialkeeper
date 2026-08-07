defmodule ElixirDB.Revisions.Wire do
  @moduledoc "Canonical wire revision maps shared by replication and revision fixtures."

  alias ElixirDB.Domain.Revision

  @spec new(binary(), binary(), pos_integer(), binary() | nil, boolean(), map() | nil) :: map()
  def new(document_id, revision_id, generation, parent_revision, deleted, body) do
    %{
      document_id: document_id,
      revision_id: revision_id,
      generation: generation,
      parent_revision: parent_revision,
      deleted: deleted,
      body: body
    }
  end

  @spec from_revision(Revision.t(), binary()) :: map()
  def from_revision(%Revision{} = revision, document_id),
    do:
      new(
        document_id,
        revision.revision_id,
        revision.generation,
        revision.parent_revision,
        revision.deleted,
        revision.body
      )
end
