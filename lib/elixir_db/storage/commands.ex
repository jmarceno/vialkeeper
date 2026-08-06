defmodule ElixirDB.Storage.Commands do
  @moduledoc "Typed command envelopes used at the database-owner boundary."

  for name <-
        ~w(CreateDocument PutDocument DeleteDocument ResolveConflict BulkWrite GetDocument GetRevision ReadChanges DiffRevisions GetRevisionChains ImportRevisionChains GetCheckpoint PutCheckpoint CreateIndex DeleteIndex RebuildIndex ExecuteQuery ExplainQuery IntegrityCheck)a do
    defmodule name do
      @enforce_keys []
      defstruct []
    end
  end
end
