defmodule ElixirDB.ModelGenerators do
  @moduledoc """
  StreamData generators for revision histories, documents, and conflict scenarios.

  These generators scaffold Phase 2 property/model tests (gap D4): random
  histories should produce identical revision trees, winners, conflicts, and
  tombstones in the pure model and storage adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Revisions.Id

  @doc """
  Generates a non-empty document ID suitable for Version 1 identity rules.
  """
  @spec document_id() :: StreamData.t(binary())
  def document_id do
    StreamData.string(:alphanumeric, min_length: 1, max_length: 32)
  end

  @doc """
  Generates a small JSON object body with string keys and scalar values.
  """
  @spec document_body() :: StreamData.t(map())
  def document_body do
    StreamData.map_of(
      StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
      StreamData.one_of([
        StreamData.integer(-1_000..1_000),
        StreamData.boolean(),
        StreamData.string(:alphanumeric, max_length: 24),
        StreamData.constant(nil)
      ]),
      max_length: 4
    )
  end

  @doc """
  Generates a linear put history for one document as Domain.Revision structs.
  """
  @spec linear_revision_history() :: StreamData.t([Revision.t()])
  def linear_revision_history do
    StreamData.bind(document_id(), fn document_id ->
      StreamData.bind(
        StreamData.list_of(document_body(), min_length: 1, max_length: 6),
        fn bodies ->
          StreamData.constant(build_linear_history(document_id, bodies))
        end
      )
    end)
  end

  @doc """
  Generates a two-sibling conflict under a shared root revision.
  """
  @spec conflict_scenario() :: StreamData.t(map())
  def conflict_scenario do
    StreamData.bind(document_id(), fn document_id ->
      StreamData.bind(
        StreamData.tuple({document_body(), document_body(), document_body()}),
        fn {root_body, left_body, right_body} ->
          left_body =
            if left_body == right_body, do: Map.put(left_body, "_side", "left"), else: left_body

          right_body = Map.put(right_body, "_side", "right")

          {:ok, root_id} = Id.calculate(document_id, nil, false, root_body)
          {:ok, left_id} = Id.calculate(document_id, root_id, false, left_body)
          {:ok, right_id} = Id.calculate(document_id, root_id, false, right_body)

          revisions = [
            revision!(document_id, root_id, nil, false, root_body),
            revision!(document_id, left_id, root_id, false, left_body),
            revision!(document_id, right_id, root_id, false, right_body)
          ]

          StreamData.constant(%{
            document_id: document_id,
            root_revision: root_id,
            left_revision: left_id,
            right_revision: right_id,
            revisions: revisions
          })
        end
      )
    end)
  end

  @doc """
  Generates a root put followed by an optional tombstone child.
  """
  @spec put_then_optional_delete() :: StreamData.t([Revision.t()])
  def put_then_optional_delete do
    StreamData.bind(
      StreamData.tuple({document_id(), document_body(), StreamData.boolean()}),
      fn {document_id, body, delete?} ->
        {:ok, root_id} = Id.calculate(document_id, nil, false, body)
        root = revision!(document_id, root_id, nil, false, body)

        revisions =
          if delete? do
            {:ok, tomb_id} = Id.calculate(document_id, root_id, true, nil)
            [root, revision!(document_id, tomb_id, root_id, true, nil)]
          else
            [root]
          end

        StreamData.constant(revisions)
      end
    )
  end

  defp build_linear_history(document_id, bodies) do
    {revisions, _parent} =
      Enum.map_reduce(bodies, nil, fn body, parent ->
        {:ok, revision_id} = Id.calculate(document_id, parent, false, body)
        {revision!(document_id, revision_id, parent, false, body), revision_id}
      end)

    revisions
  end

  defp revision!(document_id, revision_id, parent, deleted, body) do
    {:ok, generation} = Id.generation(revision_id)

    {:ok, revision} =
      Revision.new(%{
        document_id: document_id,
        revision_id: revision_id,
        generation: generation,
        parent_revision: parent,
        deleted: deleted,
        body: body
      })

    revision
  end
end
