defmodule VialKeeper.Revisions.Wire do
  @moduledoc "Canonical wire revision maps shared by replication and revision fixtures."

  alias VialKeeper.Attachments.Manifest
  alias VialKeeper.Domain.Revision

  @spec new(
          binary(),
          binary(),
          binary(),
          pos_integer(),
          binary() | nil,
          boolean(),
          map() | nil,
          Manifest.t() | map()
        ) :: map()
  def new(
        document_id,
        history_id,
        revision_id,
        generation,
        parent_revision,
        deleted,
        body,
        attachments \\ %{}
      ) do
    %{
      document_id: document_id,
      history_id: history_id,
      revision_id: revision_id,
      generation: generation,
      parent_revision: parent_revision,
      deleted: deleted,
      body: body,
      attachments: wire_attachments(attachments, deleted)
    }
  end

  @spec from_revision(Revision.t(), binary()) :: map()
  def from_revision(%Revision{} = revision, document_id),
    do:
      new(
        document_id,
        revision.history_id,
        revision.revision_id,
        revision.generation,
        revision.parent_revision,
        revision.deleted,
        revision.body,
        revision.attachments
      )

  defp wire_attachments(_attachments, true), do: %{}

  defp wire_attachments(attachments, false) when is_map(attachments) do
    case Manifest.canonical_for_hash(attachments) do
      {:ok, canonical} -> canonical
      {:error, _} -> %{}
    end
  end

  defp wire_attachments(_, _), do: %{}
end
