defmodule VialKeeper.Replication.Wire do
  @moduledoc "Storage-neutral replication wire encodings for HTTP and fixtures."

  alias VialKeeper.Domain.{BoundaryPage, RetentionBoundary}
  alias VialKeeper.MapAccess

  @spec identity(map()) :: map()
  def identity(identity) when is_map(identity) do
    %{
      "database_uuid" => MapAccess.get(identity, :database_uuid),
      "database_kind" => MapAccess.get(identity, :database_kind, :ordinary) |> to_string(),
      "history_epoch" => MapAccess.get(identity, :history_epoch),
      "current_sequence" => MapAccess.get(identity, :current_sequence),
      "retention_floor" => retention_floor(identity),
      "compaction_epoch" => MapAccess.get(identity, :compaction_epoch, 0),
      "retention_boundary_digest" => MapAccess.get(identity, :retention_boundary_digest),
      "retention_mode" => MapAccess.get(identity, :retention_mode, "disabled"),
      "replication_protocol_major" => MapAccess.get(identity, :replication_protocol_major),
      "revision_algorithm_version" => MapAccess.get(identity, :revision_algorithm_version),
      "canonicalization_version" => MapAccess.get(identity, :canonicalization_version)
    }
  end

  defp retention_floor(identity) do
    MapAccess.get(identity, :retention_floor) ||
      MapAccess.get(identity, :retention_floor_sequence, 0)
  end

  @spec boundary_page(BoundaryPage.t()) :: map()
  def boundary_page(%BoundaryPage{} = page) do
    %{
      "source_database_uuid" => page.source_database_uuid,
      "source_history_epoch" => page.source_history_epoch,
      "compaction_epoch" => page.compaction_epoch,
      "boundary_digest" => page.boundary_digest,
      "next_page" => page.next_page,
      "boundaries" => Enum.map(page.boundaries, &boundary/1),
      "install_id" => page.install_id,
      "replace" => page.replace
    }
  end

  @spec boundary(RetentionBoundary.t()) :: map()
  def boundary(%RetentionBoundary{} = boundary) do
    %{
      "document_id" => boundary.document_id,
      "history_id" => boundary.history_id,
      "minimum_retained_generation" => boundary.minimum_retained_generation,
      "retired" => boundary.retired,
      "retired_branch_roots" => boundary.retired_branch_roots
    }
  end

  @spec compact_stats(map()) :: map()
  def compact_stats(stats) when is_map(stats) do
    %{
      "noop" => Map.get(stats, :noop?, false),
      "old_floor" => Map.get(stats, :old_floor, 0),
      "new_floor" => Map.get(stats, :new_floor, 0),
      "old_compaction_epoch" => Map.get(stats, :old_compaction_epoch, 0),
      "new_compaction_epoch" => Map.get(stats, :new_compaction_epoch, 0),
      "removed_changes" => Map.get(stats, :removed_changes, 0),
      "removed_revisions" => Map.get(stats, :removed_revisions, 0),
      "removed_boundaries" => Map.get(stats, :removed_boundaries, 0),
      "active_peer_count" => Map.get(stats, :active_peer_count, 0),
      "expired_peer_count" => Map.get(stats, :expired_peer_count, 0)
    }
  end
end
