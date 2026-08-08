defmodule ElixirDB.TestRevisionId do
  @moduledoc false

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.RevisionFixtures
  alias ElixirDB.Revisions.Id

  @spec calculate(binary(), binary() | nil, boolean(), map() | nil, Manifest.t() | map()) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
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
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(document_id, history_id, parent, deleted, body, attachments) do
    Id.calculate(document_id, history_id, parent, deleted, body, attachments)
  end
end
