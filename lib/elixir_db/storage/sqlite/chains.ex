defmodule ElixirDB.Storage.SQLite.Chains do
  @moduledoc """
  Revision-chain read helpers for the Version 1 SQLite adapter.

  Owns revision-diff and chain-walk orchestration used by replication wire
  reads. Import write workflows live in `Import`; transaction boundaries remain
  in the adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Storage.SQLite.{Documents, Revisions}

  @doc """
  Diffs requested leaf revisions against stored leaf sets.
  """
  @spec diff(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def diff(adapter, request) do
    documents = request[:documents] || request["documents"] || []

    with :ok <- validate_documents_batch(documents),
         {:ok, result} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             id = entry[:document_id] || entry["document_id"]
             leaves_requested = entry[:leaf_revisions] || entry["leaf_revisions"] || []

             case Documents.find(adapter.conn, id) do
               {:ok, nil} ->
                 {:cont, {:ok, [%{document_id: id, missing_revisions: leaves_requested} | acc]}}

               {:ok, doc} ->
                 existing =
                   Revisions.leaves(adapter.conn, doc.doc_key)
                   |> Enum.map(& &1.revision_id)
                   |> MapSet.new()

                 {:cont,
                  {:ok,
                   [
                     %{
                       document_id: id,
                       missing_revisions:
                         Enum.reject(leaves_requested, &MapSet.member?(existing, &1))
                     }
                     | acc
                   ]}}

               {:error, error} ->
                 {:halt, {:error, error}}
             end
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{documents: result}}
    end
  end

  @doc """
  Loads parent-ordered revision chains for requested leaves.
  """
  @spec get(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get(adapter, request) do
    documents = request[:documents] || request["documents"] || []

    with :ok <- validate_documents_batch(documents),
         {:ok, chains} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             id = entry[:document_id] || entry["document_id"]
             requested = entry[:leaf_revisions] || entry["leaf_revisions"] || []

             case Documents.find(adapter.conn, id) do
               {:ok, nil} ->
                 {:cont, {:ok, acc}}

               {:ok, doc} ->
                 case Enum.reduce_while(requested, {:ok, []}, fn leaf, {:ok, chain_acc} ->
                        case chain_for_leaf(adapter, doc, leaf) do
                          {:ok, chain} -> {:cont, {:ok, [chain | chain_acc]}}
                          {:error, error} -> {:halt, {:error, error}}
                        end
                      end) do
                   {:ok, values} -> {:cont, {:ok, Enum.reverse(values) ++ acc}}
                   {:error, error} -> {:halt, {:error, error}}
                 end

               {:error, error} ->
                 {:halt, {:error, error}}
             end
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{chains: chains}}
    end
  end

  defp chain_for_leaf(adapter, doc, leaf_id) do
    case Revisions.find(adapter.conn, doc.doc_key, leaf_id) do
      {:ok, leaf} ->
        case chain(adapter, doc.doc_key, leaf, []) do
          {:ok, revisions} ->
            {:ok,
             %{
               document_id: doc.document_id,
               leaf_revision: leaf_id,
               revisions: Enum.map(revisions, &revision_wire(&1, doc.document_id))
             }}

          {:error, error} ->
            {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp chain(_adapter, _doc_key, %Revision{parent_revision: nil} = revision, acc),
    do: {:ok, [revision | acc]}

  defp chain(adapter, doc_key, %Revision{parent_revision: parent} = revision, acc) do
    case Revisions.find(adapter.conn, doc_key, parent) do
      {:ok, parent_revision} ->
        chain(adapter, doc_key, parent_revision, [revision | acc])

      {:error, _} ->
        {:error,
         ElixirDB.Error.integrity_violation("revision chain contains a dangling parent", %{
           parent_revision: parent
         })}
    end
  end

  defp validate_documents_batch(documents) when is_list(documents) do
    max = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    cond do
      length(documents) > max ->
        {:error, ElixirDB.Error.resource_limit("replication document count exceeds the host limit")}

      not Enum.all?(documents, &valid_replication_document_request?/1) ->
        {:error, ElixirDB.Error.invalid_request("replication document request is invalid")}

      true ->
        :ok
    end
  end

  defp validate_documents_batch(_),
    do: {:error, ElixirDB.Error.invalid_request("replication documents must be an array")}

  defp valid_replication_document_request?(entry) when is_map(entry) do
    id = entry[:document_id] || entry["document_id"]
    leaves = entry[:leaf_revisions] || entry["leaf_revisions"] || []
    allowed = [:document_id, :leaf_revisions, "document_id", "leaf_revisions"]

    Enum.all?(Map.keys(entry), &(&1 in allowed)) and is_binary(id) and is_list(leaves) and
      length(leaves) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500) and
      Enum.all?(leaves, &(is_binary(&1) and &1 != ""))
  end

  defp valid_replication_document_request?(_), do: false

  defp revision_wire(%Revision{} = revision, document_id),
    do: %{
      document_id: document_id,
      revision_id: revision.revision_id,
      generation: revision.generation,
      parent_revision: revision.parent_revision,
      deleted: revision.deleted,
      body: revision.body
    }
end
