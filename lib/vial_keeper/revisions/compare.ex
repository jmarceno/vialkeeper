defmodule VialKeeper.Revisions.Compare do
  @moduledoc """
  Storage-neutral revision equality and leaf-set encoding helpers.

  Used by shared mutation and import services when comparing stored revisions
  or writing change-feed leaf snapshots.
  """

  alias VialKeeper.Domain.Revision
  alias VialKeeper.JSON.Canonical

  @doc "True when two revision structs describe the same stored content."
  @spec same?(Revision.t(), Revision.t()) :: boolean()
  def same?(a, b),
    do:
      a.revision_id == b.revision_id and a.generation == b.generation and
        a.parent_revision == b.parent_revision and a.history_id == b.history_id and
        a.deleted == b.deleted and a.body == b.body and a.attachments == b.attachments

  @doc "Encodes leaf revisions for a change-feed `leaf_set_json` payload."
  @spec encode_leaf_set([Revision.t()]) :: {:ok, binary()} | {:error, term()}
  def encode_leaf_set(leaves),
    do:
      Canonical.encode(
        Enum.map(leaves, fn leaf ->
          %{
            "revision" => leaf.revision_id,
            "history_id" => leaf.history_id,
            "deleted" => leaf.deleted
          }
        end)
      )
end
