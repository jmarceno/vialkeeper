defmodule ElixirDB.Storage.Services.Chains do
  @moduledoc """
  Shared revision-chain read helpers for replication wire reads.

  Owns revision-diff and chain-walk orchestration against storage ports.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.MapAccess
  alias ElixirDB.Revisions.Wire
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Services.Facts

  @doc """
  Diffs requested leaf revisions against stored leaf sets.
  """
  @spec diff(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def diff(%BackendContext{} = context, request) do
    documents = MapAccess.get(request, :documents, [])
    source_uuid = MapAccess.get(request, :source_database_uuid)

    with :ok <- validate_documents_batch(documents),
         {:ok, boundaries} <- boundaries_for_diff(context, source_uuid),
         {:ok, result} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             diff_document(context, entry, boundaries, acc)
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{documents: result}}
    end
  end

  defp boundaries_for_diff(context, nil), do: Facts.list_boundaries(context)

  defp boundaries_for_diff(context, source_uuid) when is_binary(source_uuid),
    do: Facts.list_boundaries(context, source_database_uuid: source_uuid)

  @doc """
  Loads parent-ordered revision chains for requested leaves.
  """
  @spec get(BackendContext.t(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def get(%BackendContext{} = context, request) do
    bootstrap? =
      MapAccess.get(request, :bootstrap) == true or MapAccess.get(request, "bootstrap") == true

    if bootstrap? do
      get_bootstrap(context, request)
    else
      get_requested_documents(context, request)
    end
  end

  defp get_requested_documents(context, request) do
    documents = MapAccess.get(request, :documents, [])

    with :ok <- validate_documents_batch(documents),
         {:ok, chains} <-
           Enum.reduce_while(documents, {:ok, []}, fn entry, {:ok, acc} ->
             get_document_chains(context, entry, acc)
           end)
           |> then(fn
             {:ok, values} -> {:ok, Enum.reverse(values)}
             error -> error
           end) do
      {:ok, %{chains: chains}}
    end
  end

  defp get_bootstrap(context, request) do
    cursor =
      MapAccess.get(request, :cursor) || MapAccess.get(request, :page_cursor) ||
        MapAccess.get(request, "cursor") || MapAccess.get(request, "page_cursor")

    limit =
      MapAccess.get(request, :limit) || MapAccess.get(request, "limit") ||
        bootstrap_page_size()

    with :ok <- validate_bootstrap_limit(limit),
         :ok <- validate_bootstrap_cursor(cursor),
         {:ok, identity} <- {:ok, Facts.identity(context)},
         {:ok, %{document_ids: document_ids, next_cursor: next_cursor}} <-
           Facts.list_document_page(context, cursor, limit),
         {:ok, boundaries} <- Facts.list_boundaries(context),
         {:ok, chains} <- bootstrap_chains(context, document_ids, boundaries),
         {:ok, purged_boundaries} <- bootstrap_purged_boundaries(context, document_ids, boundaries) do
      {:ok,
       %{
         source_history_epoch: Map.get(identity, :history_epoch),
         compaction_epoch: Map.get(identity, :compaction_epoch, 0),
         retention_floor: Map.get(identity, :retention_floor_sequence, 0),
         retention_boundary_digest: Map.get(identity, :retention_boundary_digest),
         continuation_cursor: next_cursor,
         chains: chains,
         purged_boundaries: purged_boundaries
       }}
    end
  end

  defp bootstrap_page_size do
    ElixirDB.Config.host_limits()[:max_bulk_operations] || 500
  end

  defp bootstrap_chains(context, document_ids, boundaries) do
    Enum.reduce_while(document_ids, {:ok, []}, fn document_id, {:ok, acc} ->
      bootstrap_document_chain(context, document_id, boundaries, acc)
    end)
  end

  defp bootstrap_purged_boundaries(context, document_ids, boundaries) do
    document_ids = MapSet.new(document_ids)

    {:ok,
     boundaries
     |> Enum.filter(fn %{boundary: boundary} ->
       boundary.retired and MapSet.member?(document_ids, boundary.document_id)
     end)
     |> Enum.map(&Facts.encode_stored_boundary(context, &1))}
  end

  defp bootstrap_document_chain(context, document_id, boundaries, acc) do
    case Facts.find_document(context, document_id) do
      {:ok, nil} ->
        {:cont, {:ok, acc}}

      {:ok, doc} ->
        append_bootstrap_chains(context, doc, boundaries, acc)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp append_bootstrap_chains(context, doc, boundaries, acc) do
    case bootstrap_document_chains(context, doc, boundaries) do
      {:ok, chains} -> {:cont, {:ok, acc ++ chains}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp bootstrap_document_chains(context, doc, boundaries) do
    case leaves(context, doc.document_id) do
      [] ->
        {:ok, []}

      leaves ->
        build_bootstrap_chains(context, doc, leaves, boundaries)
    end
  end

  defp build_bootstrap_chains(context, doc, leaves, boundaries) do
    truncated = truncated_history?(boundaries, doc.document_id)

    Enum.reduce_while(leaves, {:ok, []}, fn leaf, {:ok, acc} ->
      append_leaf_chain(context, Map.put(doc, :truncated, truncated), leaf, truncated, acc)
    end)
    |> then(fn
      {:ok, chains} -> {:ok, Enum.reverse(chains)}
      error -> error
    end)
  end

  defp append_leaf_chain(context, doc, leaf, truncated, acc) do
    case chain_for_leaf(context, doc, leaf.revision_id, truncated) do
      {:ok, chain} -> {:cont, {:ok, [chain | acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp truncated_history?(boundaries, document_id),
    do:
      Enum.any?(boundaries, fn %{boundary: boundary} ->
        boundary.document_id == document_id and boundary.retired
      end)

  defp diff_document(context, entry, boundaries, acc) do
    id = MapAccess.get(entry, :document_id)
    leaves_requested = normalize_leaves(MapAccess.get(entry, :leaf_revisions, []))

    case Facts.find_document(context, id) do
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
          leaves(context, doc.document_id)
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

  defp boundary_covers_leaf?(_boundaries, _document_id, nil, _revision_id), do: false

  defp boundary_covers_leaf?(boundaries, document_id, history_id, revision_id) do
    Enum.any?(boundaries, fn %{boundary: boundary} ->
      boundary.document_id == document_id and boundary.history_id == history_id and
        boundary_covers_revision?(boundary, revision_id)
    end)
  end

  defp boundary_covers_revision?(%{retired: true}, _revision_id), do: true

  defp boundary_covers_revision?(boundary, revision_id) do
    generation_covers?(boundary, revision_id) or root_covers?(boundary, revision_id)
  end

  defp generation_covers?(%{minimum_retained_generation: generation}, revision_id)
       when is_integer(generation) do
    revision_generation(revision_id) < generation
  end

  defp generation_covers?(_boundary, _revision_id), do: false

  defp root_covers?(%{retired_branch_roots: roots}, revision_id) when is_list(roots),
    do: revision_id in roots

  defp root_covers?(_boundary, _revision_id), do: false

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

  defp get_document_chains(context, entry, acc) do
    id = MapAccess.get(entry, :document_id)
    requested = MapAccess.get(entry, :leaf_revisions, [])
    truncated = MapAccess.get(entry, :truncated, false)

    case validate_truncated_flag(truncated) do
      :ok -> find_document_chains(context, id, truncated, requested, acc)
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp find_document_chains(context, id, truncated, requested, acc) do
    case Facts.find_document(context, id) do
      {:ok, nil} ->
        {:cont, {:ok, acc}}

      {:ok, doc} ->
        get_requested_chains(context, Map.put(doc, :truncated, truncated), requested, acc)

      {:error, error} ->
        {:halt, {:error, error}}
    end
  end

  defp get_requested_chains(context, doc, requested, acc) do
    case Enum.reduce_while(requested, {:ok, []}, &get_requested_chain(context, doc, &1, &2)) do
      {:ok, values} -> {:cont, {:ok, Enum.reverse(values, acc)}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp get_requested_chain(context, doc, leaf, {:ok, chain_acc}) do
    leaf_id =
      case leaf do
        leaf when is_binary(leaf) -> leaf
        %{"revision" => revision} -> revision
        %{revision: revision} -> revision
      end

    truncated = MapAccess.get(doc, :truncated, false)

    case chain_for_leaf(context, doc, leaf_id, truncated) do
      {:ok, chain} -> {:cont, {:ok, [chain | chain_acc]}}
      {:error, error} -> {:halt, {:error, error}}
    end
  end

  defp chain_for_leaf(context, doc, leaf_id, truncated) do
    case Facts.find_revision(context, doc.document_id, leaf_id) do
      {:ok, leaf} when not is_nil(leaf) ->
        case chain(context, doc.document_id, leaf, [], truncated: truncated) do
          {:ok, revisions, truncated?} ->
            {:ok,
             %{
               document_id: doc.document_id,
               history_id: leaf.history_id,
               leaf_revision: leaf_id,
               source_update_sequence: doc.update_sequence,
               truncated: truncated?,
               revisions: Enum.map(revisions, &revision_wire(&1, doc.document_id))
             }}

          {:error, error} ->
            {:error, error}
        end

      {:ok, nil} ->
        {:error, ElixirDB.Error.revision_not_found("revision not found", %{revision: leaf_id})}

      {:error, error} ->
        {:error, error}
    end
  end

  defp chain(_context, _document_id, %Revision{parent_revision: nil} = revision, acc, _opts),
    do: {:ok, [revision | acc], false}

  defp chain(context, document_id, %Revision{parent_revision: parent} = revision, acc, opts) do
    truncated = Keyword.get(opts, :truncated, false)

    case Facts.find_revision(context, document_id, parent) do
      {:ok, parent_revision} when not is_nil(parent_revision) ->
        chain(context, document_id, parent_revision, [revision | acc], opts)

      {:ok, nil} when truncated ->
        {:ok, [revision | acc], true}

      {:error, _} when truncated ->
        {:ok, [revision | acc], true}

      {:ok, nil} ->
        {:error,
         ElixirDB.Error.integrity_violation("revision chain contains a dangling parent", %{
           parent_revision: parent
         })}

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

  defp leaves(context, document_id) do
    case Facts.list_leaves(context, document_id) do
      {:ok, value} -> value
      _ -> []
    end
  end

  defp revision_wire(%Revision{} = revision, document_id),
    do: Wire.from_revision(revision, document_id)

  defp validate_bootstrap_limit(limit) when is_integer(limit) and limit > 0, do: :ok

  defp validate_bootstrap_limit(_),
    do: {:error, ElixirDB.Error.invalid_request("bootstrap limit must be a positive integer")}

  defp validate_bootstrap_cursor(nil), do: :ok
  defp validate_bootstrap_cursor(cursor) when is_binary(cursor), do: :ok

  defp validate_bootstrap_cursor(_),
    do: {:error, ElixirDB.Error.invalid_request("bootstrap cursor must be a binary or null")}

  defp validate_truncated_flag(value) when value in [true, false], do: :ok

  defp validate_truncated_flag(_),
    do: {:error, ElixirDB.Error.invalid_request("truncated must be a boolean")}
end
