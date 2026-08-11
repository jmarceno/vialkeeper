defmodule ElixirDB.Storage.Ports.IndexCandidates do
  @moduledoc """
  Logical index definition and candidate-retrieval port.

  The backend stores definitions and returns opaque candidate rows. Shared query
  code owns selector, ordering, projection, and bookmark semantics.
  """

  alias ElixirDB.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, ElixirDB.Error.t()}
  @type index_definition :: map()
  @type candidate :: map()

  @callback list_indexes(BackendContext.t()) :: result([index_definition()])
  @callback create_index(BackendContext.t(), map()) :: result(index_definition())
  @callback delete_index(BackendContext.t(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  @callback rebuild_index(BackendContext.t(), binary()) :: result(map())
  @callback winning_document_count(BackendContext.t()) :: result(non_neg_integer())
  @callback lookup_candidates(BackendContext.t(), map()) :: result([candidate()])
  @callback range_scan_candidates(BackendContext.t(), map()) ::
              result([candidate()] | %{candidates: [candidate()], examined: non_neg_integer()})
  @callback full_text_candidates(BackendContext.t(), map()) :: result([candidate()])
  @callback ready_definitions(BackendContext.t()) :: result([term()])
  @callback refresh_document(BackendContext.t(), binary(), map(), term()) ::
              :ok | {:error, ElixirDB.Error.t()}
end
