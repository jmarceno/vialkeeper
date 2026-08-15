defmodule VialKeeper.TestRevisionId do
  @moduledoc "Test helper for calculating revisions with shared fixture history IDs."

  alias VialKeeper.Attachments.Manifest
  alias VialKeeper.RevisionFixtures
  alias VialKeeper.Revisions.Id

  @spec calculate(binary(), binary() | nil, boolean(), map() | nil, Manifest.t() | map()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def calculate(document_id, parent, deleted, body, attachments \\ %{}) do
    Id.calculate(
      document_id,
      RevisionFixtures.shared_history_id(),
      parent,
      deleted,
      body,
      attachments
    )
  end

  @spec calculate(binary(), binary(), binary() | nil, boolean(), map() | nil, Manifest.t() | map()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def calculate(document_id, history_id, parent, deleted, body, attachments) do
    Id.calculate(document_id, history_id, parent, deleted, body, attachments)
  end
end
