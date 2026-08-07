defmodule ElixirDB.Storage.SQLite.Chains do
  @moduledoc """
  Revision-chain read helpers for the Version 1 SQLite adapter.

  Owns revision-diff and chain-walk orchestration used by replication wire
  reads. Import write workflows live in `Import`; transaction boundaries remain
  in the adapter.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Wire
  alias ElixirDB.Storage.SQLite.{Adapter, Documents, RetentionRecords, Revisions}

  @doc """
  Diffs requested leaf revisions against stored leaf sets.
  """
  @spec diff(map(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def diff(adapter, request) do
    documents = MapAccess.get(request, :documents, [])

    with :ok <- validate_documents_batch(documents),
         {:ok, boundaries} <- RetentionRecords.list_boundaries(adapter.conn),
         {:ok, result} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             diff_document(adapter, entry, boundaries, acc)
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
    bootstrap? =
      MapAccess.get(request, :bootstrap) == true or MapAccess.get(request, "bootstrap") == true

    if bootstrap? do
      get_bootstrap(adapter, request)
    else
      get_requested_documents(adapter, request)
    end
  end

  defp get_requested_documents(adapter, request) do
    documents = MapAccess.get(request, :documents, [])

    with :ok <- validate_documents_batch(documents),
         {:ok, chains} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             get_document_chains(adapter, entry, acc)
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{chains: chains}}
    end
  end

  defp get_bootstrap(adapter, request) do
    cursor =
      MapAccess.get(request, :cursor) || MapAccess.get(request, :page_cursor) ||
        MapAccess.get(request, "cursor") || MapAccess.get(request, "page_cursor")

    limit =
      MapAccess.get(request, :limit) || MapAccess.get(request, "limit") ||
        bootstrap_page_size()

    with {:ok, identity} <- Adapter.identity(adapter),
         {:ok, {document_ids, next_cursor}} <- Documents.list_page(adapter.conn, cursor, limit),
         {:ok, chains} <- bootstrap_chains(adapter, document_ids) do
      {:ok,
       %{
         source_history_epoch: Map.get(identity, :history_epoch),
         compaction_epoch: Map.get(identity, :compaction_epoch, 0),
         retention_floor: Map.get(identity, :retention_floor_sequence, 0),
         retention_boundary_digest: Map.get(identity, :retention_boundary_digest),
         continuation_cursor: next_cursor,
         chains: chains
       }}
    end
  end

  defp bootstrap_page_size do
    ElixirDB.Config.host_limits()[:max_bulk_operations] || 500
  end

  defp bootstrap_chains(adapter, document_ids) do
    Enum.reduce_while(document_ids, {:ok, []}, fn document_id, {:ok, acc} ->
      bootstrap_document_chain(adapter, document_id, acc)
    end)
  end

  defp bootstrap_document_chain(adapter, document_id, acc) do
    case Documents.find(adapter.conn, document_id) do
      {:ok, nil} ->
        {:cont, {:ok, acc}}

      {:ok, doc} ->
        append_bootstrap_chains(adapter, doc, acc)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp append_bootstrap_chains(adapter, doc, acc) do
    case bootstrap_document_chains(adapter, doc) do
      {:ok, chains} -> {:cont, {:ok, acc ++ chains}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp bootstrap_document_chains(adapter, doc) do
    case Revisions.leaves(adapter.conn, doc.doc_key) do
      [] ->
        {:ok, []}

      leaves ->
        build_bootstrap_chains(adapter, doc, leaves)
    end
  end

  defp build_bootstrap_chains(adapter, doc, leaves) do
    truncated = truncated_history?(adapter, doc.document_id)

    Enum.reduce_while(leaves, {:ok, []}, fn leaf, {:ok, acc} ->
      append_leaf_chain(adapter, Map.put(doc, :truncated, truncated), leaf, truncated, acc)
    end)
    |> then(fn
      {:ok, chains} -> {:ok, Enum.reverse(chains)}
      error -> error
    end)
  end

  defp append_leaf_chain(adapter, doc, leaf, truncated, acc) do
    case chain_for_leaf(adapter, doc, leaf.revision_id, truncated) do
      {:ok, chain} -> {:cont, {:ok, [chain | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp truncated_history?(adapter, document_id) do
    case RetentionRecords.list_boundaries(adapter.conn) do
      {:ok, boundaries} ->
        Enum.any?(boundaries, fn %{boundary: boundary} ->
          boundary.document_id == document_id and boundary.retired
        end)

      _ ->
        false
    end
  end

  defp diff_document(adapter, entry, boundaries, acc) do
    id = MapAccess.get(entry, :document_id)
    leaves_requested = normalize_leaves(MapAccess.get(entry, :leaf_revisions, []))

    case Documents.find(adapter.conn, id) do
      {:ok, nil} ->
        {:cont,
         {:ok,
          [
            %{
              document_id: id,
              missing_revisions: leaf_wire(leaves_requested),
              compacted_revisions: []
            }
            | acc
          ]}}

      {:ok, doc} ->
        existing =
          Revisions.leaves(adapter.conn, doc.doc_key)
          |> Enum.map(& &1.revision_id)
          |> MapSet.new()

        {absent, _present} =
          Enum.split_with(leaves_requested, fn {revision_id, _} ->
            not MapSet.member?(existing, revision_id)
          end)

        {compacted, missing} =
          Enum.split_with(absent, fn {revision_id, history_id} ->
            boundary_covers_leaf?(boundaries, id, history_id, revision_id)
          end)

        {:cont,
         {:ok,
          [
            %{
              document_id: id,
              missing_revisions: leaf_wire(missing),
              compacted_revisions: leaf_wire(compacted)
            }
            | acc
          ]}}

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp boundary_covers_leaf?(boundaries, document_id, history_id, revision_id) do
    Enum.any?(boundaries, fn %{boundary: boundary} ->
      boundary.document_id == document_id and
        (history_id == nil or boundary.history_id == history_id) and
        boundary_covers_revision?(boundary, revision_id)
    end)
  end

  defp boundary_covers_revision?(%{retired: true}, _revision_id), do: true

  defp boundary_covers_revision?(%{retired_branch_roots: roots}, revision_id) when is_list(roots) do
    revision_id in roots or roots != []
  end

  defp boundary_covers_revision?(%{minimum_retained_generation: generation}, revision_id)
       when is_integer(generation) do
    revision_generation(revision_id) < generation
  end

  defp boundary_covers_revision?(_boundary, _revision_id), do: true

  defp revision_generation(revision_id) when is_binary(revision_id) do
    case Integer.parse(revision_id) do
      {generation, _} -> generation
      :error -> 0
    end
  end

  defp normalize_leaves(leaves) when is_list(leaves) do
    Enum.map(leaves, &normalize_leaf/1)
  end

  defp normalize_leaf(leaf) when is_binary(leaf), do: {leaf, nil}

  defp normalize_leaf(%{"revision" => revision, "history_id" => history_id}),
    do: {revision, history_id}

  defp normalize_leaf(%{revision: revision, history_id: history_id}),
    do: {revision, history_id}

  defp leaf_wire(leaves) do
    Enum.map(leaves, fn
      {revision, nil} -> revision
      {revision, history_id} -> %{"revision" => revision, "history_id" => history_id}
    end)
  end

  defp get_document_chains(adapter, entry, acc) do
    id = MapAccess.get(entry, :document_id)
    requested = MapAccess.get(entry, :leaf_revisions, [])
    truncated = MapAccess.get(entry, :truncated, false)

    case Documents.find(adapter.conn, id) do
      {:ok, nil} ->
        {:cont, {:ok, acc}}

      {:ok, doc} ->
        get_requested_chains(adapter, Map.put(doc, :truncated, truncated), requested, acc)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp get_requested_chains(adapter, doc, requested, acc) do
    case Enum.reduce_while(requested, {:ok, []}, &get_requested_chain(adapter, doc, &1, &2)) do
      {:ok, values} -> {:cont, {:ok, Enum.reverse(values, acc)}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp get_requested_chain(adapter, doc, leaf, {:ok, chain_acc}) do
    leaf_id =
      case leaf do
        leaf when is_binary(leaf) -> leaf
        %{"revision" => revision} -> revision
        %{revision: revision} -> revision
      end

    truncated = MapAccess.get(doc, :truncated, false)

    case chain_for_leaf(adapter, doc, leaf_id, truncated) do
      {:ok, chain} -> {:cont, {:ok, [chain | chain_acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp chain_for_leaf(adapter, doc, leaf_id, truncated) do
    case Revisions.find(adapter.conn, doc.doc_key, leaf_id) do
      {:ok, leaf} ->
        case chain(adapter, doc.doc_key, leaf, [], truncated: truncated) do
          {:ok, revisions, truncated?} ->
            {:ok,
             %{
               document_id: doc.document_id,
               history_id: leaf.history_id,
               leaf_revision: leaf_id,
               truncated: truncated?,
               revisions: Enum.map(revisions, &revision_wire(&1, doc.document_id))
             }}

          {:error, error} ->
            {:error, error}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp chain(_adapter, _doc_key, %Revision{parent_revision: nil} = revision, acc, _opts),
    do: {:ok, [revision | acc], false}

  defp chain(adapter, doc_key, %Revision{parent_revision: parent} = revision, acc, opts) do
    truncated = Keyword.get(opts, :truncated, false)

    case Revisions.find(adapter.conn, doc_key, parent) do
      {:ok, parent_revision} ->
        chain(adapter, doc_key, parent_revision, [revision | acc], opts)

      {:error, _} when truncated ->
        {:ok, [revision | acc], true}

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
    id = MapAccess.get(entry, :document_id)
    leaves = MapAccess.get(entry, :leaf_revisions, [])
    bootstrap = MapAccess.get(entry, :bootstrap)

    allowed = [
      :document_id,
      :leaf_revisions,
      :truncated,
      :bootstrap,
      "document_id",
      "leaf_revisions",
      "truncated",
      "bootstrap"
    ]

    Enum.all?(Map.keys(entry), &(&1 in allowed)) and is_binary(id) and
      (is_list(leaves) or bootstrap == true) and
      length(leaves) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500) and
      Enum.all?(leaves, &valid_leaf_revision?/1)
  end

  defp valid_replication_document_request?(_), do: false

  defp valid_leaf_revision?(leaf) when is_binary(leaf) and leaf != "", do: true

  defp valid_leaf_revision?(%{"revision" => revision, "history_id" => history_id}),
    do: is_binary(revision) and revision != "" and is_binary(history_id) and history_id != ""

  defp valid_leaf_revision?(%{revision: revision, history_id: history_id}),
    do: is_binary(revision) and revision != "" and is_binary(history_id) and history_id != ""

  defp valid_leaf_revision?(_), do: false

  defp revision_wire(%Revision{} = revision, document_id),
    do: Wire.from_revision(revision, document_id)
end
