defmodule ElixirDB.Domain.Change do
  @enforce_keys [:sequence, :document_id, :winning_revision, :deleted, :leaf_revisions]
  defstruct [:sequence, :document_id, :winning_revision, :deleted, :leaf_revisions, :origin]

  @type t :: %__MODULE__{
          sequence: pos_integer(),
          document_id: binary(),
          winning_revision: binary(),
          deleted: boolean(),
          leaf_revisions: [ElixirDB.Domain.Leaf.t()],
          origin: binary() | nil
        }
end
