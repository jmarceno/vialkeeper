defmodule ElixirDB.TestRevisionId do
  @moduledoc false

  alias ElixirDB.RevisionFixtures
  alias ElixirDB.Revisions.Id

  @spec calculate(binary(), binary() | nil, boolean(), map() | nil) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(document_id, parent, deleted, body) do
    Id.calculate(document_id, RevisionFixtures.shared_history_id(), parent, deleted, body)
  end

  @spec calculate(binary(), binary(), binary() | nil, boolean(), map() | nil) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(document_id, history_id, parent, deleted, body) do
    Id.calculate(document_id, history_id, parent, deleted, body)
  end
end
