defmodule ElixirDB.Storage.Ports.DocumentFacts do
  @moduledoc """
  Document and revision fact port.

  Fact-level reads and writes only. Product workflows such as local mutation,
  conflict resolution, and import orchestration belong in shared services
  rather than in this port.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}
  @type document_fact :: %{
          required(:document_id) => binary(),
          required(:winning_revision) => binary() | nil,
          required(:winning_deleted) => boolean(),
          required(:update_sequence) => non_neg_integer(),
          optional(:body) => map() | nil,
          optional(:backend_meta) => map()
        }

  @callback find_document(BackendContext.t(), binary()) :: result(document_fact() | nil)
  @callback find_documents(BackendContext.t(), [binary()]) ::
              result(%{optional(binary()) => document_fact() | nil})
  @callback find_revision(BackendContext.t(), binary(), binary()) :: result(Revision.t() | nil)
  @callback list_leaves(BackendContext.t(), binary()) :: result([Revision.t()])
  @callback list_ancestors(BackendContext.t(), binary(), binary()) :: result([Revision.t()])
  @callback list_document_page(BackendContext.t(), binary() | nil, pos_integer()) ::
              result(%{document_ids: [binary()], next_cursor: binary() | nil})
  @callback ensure_document(BackendContext.t(), binary()) :: result(document_fact())
  @callback ensure_parent(BackendContext.t(), binary(), binary() | nil) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback insert_revision(BackendContext.t(), binary(), Revision.t()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback insert_revision_for_document(BackendContext.t(), document_fact(), Revision.t()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback insert_or_accept_revision(BackendContext.t(), binary(), Revision.t()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback update_winning(BackendContext.t(), binary(), Revision.t(), non_neg_integer()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback update_winning_for_document(
              BackendContext.t(),
              document_fact(),
              Revision.t(),
              non_neg_integer()
            ) :: :ok | {:error, ElixirDB.Error.t()}
  @callback empty_document(BackendContext.t(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback delete_history(BackendContext.t(), binary(), binary()) ::
              :ok | {:error, ElixirDB.Error.t()}
  @callback list_compaction_documents(BackendContext.t(), non_neg_integer()) ::
              result([
                %{
                  document_id: binary(),
                  latest_change_sequence: non_neg_integer(),
                  winning_revision: binary() | nil,
                  revisions: [Revision.t()]
                }
              ])
  @callback delete_revisions(BackendContext.t(), binary(), [binary()]) ::
              :ok | {:error, ElixirDB.Error.t()}
end
