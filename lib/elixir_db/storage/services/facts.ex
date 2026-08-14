defmodule ElixirDB.Storage.Services.Facts do
  @moduledoc """
  Port-facing helpers for shared mutation, import, chain, and retention services.

  Resolves document/revision/change/attachment/retention/index ports from an
  opaque `BackendContext` so services never unwrap backend handles.
  """

  alias ElixirDB.Domain.Revision
  alias ElixirDB.Revisions.Compare
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Access
  alias ElixirDB.Storage.Services.Attachments

  @type change_entry :: %{
          required(:sequence) => pos_integer(),
          required(:document_id) => binary(),
          required(:winner) => Revision.t(),
          required(:leaf_json) => binary(),
          required(:origin) => binary(),
          required(:backend_meta) => map()
        }

  @doc "Loads one document fact or `nil`."
  @spec find_document(BackendContext.t(), binary()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def find_document(%BackendContext{} = ctx, document_id),
    do: Access.port(ctx, :document_facts).find_document(ctx, document_id)

  @doc "Loads many document facts keyed by document id."
  @spec find_documents(BackendContext.t(), [binary()]) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def find_documents(%BackendContext{} = ctx, document_ids),
    do: Access.port(ctx, :document_facts).find_documents(ctx, document_ids)

  @doc "Loads one revision; missing document yields `{:ok, nil}`."
  @spec find_revision(BackendContext.t(), binary(), binary()) ::
          {:ok, Revision.t() | nil} | {:error, ElixirDB.Error.t()}
  def find_revision(%BackendContext{} = ctx, document_id, revision_id),
    do: Access.port(ctx, :document_facts).find_revision(ctx, document_id, revision_id)

  @doc "Loads requested document/revision pairs in request order."
  @spec find_revision_batch(BackendContext.t(), list()) ::
          {:ok, list()} | {:error, ElixirDB.Error.t()}
  def find_revision_batch(%BackendContext{} = ctx, requests),
    do: Access.port(ctx, :document_facts).find_revision_batch(ctx, requests)

  @doc "Lists leaf revisions for a document."
  @spec list_leaves(BackendContext.t(), binary()) ::
          {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def list_leaves(%BackendContext{} = ctx, document_id),
    do: Access.port(ctx, :document_facts).list_leaves(ctx, document_id)

  @doc "Lists ancestors of a revision."
  @spec list_ancestors(BackendContext.t(), binary(), binary()) ::
          {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def list_ancestors(%BackendContext{} = ctx, document_id, revision_id),
    do: Access.port(ctx, :document_facts).list_ancestors(ctx, document_id, revision_id)

  @doc "Paginates document ids for bootstrap."
  @spec list_document_page(BackendContext.t(), binary() | nil, pos_integer()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def list_document_page(%BackendContext{} = ctx, cursor, limit),
    do: Access.port(ctx, :document_facts).list_document_page(ctx, cursor, limit)

  @doc "Ensures a document row exists and returns its fact."
  @spec ensure_document(BackendContext.t(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def ensure_document(%BackendContext{} = ctx, document_id),
    do: Access.port(ctx, :document_facts).ensure_document(ctx, document_id)

  @doc "Inserts a new document with its first revision already materialized."
  @spec insert_document_with_revision(
          BackendContext.t(),
          binary(),
          Revision.t(),
          non_neg_integer(),
          binary() | nil
        ) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def insert_document_with_revision(
        %BackendContext{} = ctx,
        document_id,
        revision,
        sequence,
        body_json
      ),
      do:
        Access.port(ctx, :document_facts).insert_document_with_revision(
          ctx,
          document_id,
          revision,
          sequence,
          body_json
        )

  @doc "Ensures a parent revision exists when required."
  @spec ensure_parent(BackendContext.t(), binary(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_parent(%BackendContext{} = ctx, document_id, parent),
    do: Access.port(ctx, :document_facts).ensure_parent(ctx, document_id, parent)

  @doc "Inserts a revision for a document."
  @spec insert_revision(BackendContext.t(), binary(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_revision(%BackendContext{} = ctx, document_id, revision),
    do: Access.port(ctx, :document_facts).insert_revision(ctx, document_id, revision)

  @doc "Inserts a revision using a pre-encoded body when available."
  @spec insert_revision_with_body(BackendContext.t(), binary(), Revision.t(), binary() | nil) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_revision_with_body(%BackendContext{} = ctx, document_id, revision, body_json),
    do:
      Access.port(ctx, :document_facts).insert_revision_with_body(
        ctx,
        document_id,
        revision,
        body_json
      )

  @doc "Inserts a revision using an already loaded document fact."
  @spec insert_revision_for_document(BackendContext.t(), map(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_revision_for_document(%BackendContext{} = ctx, document, revision),
    do: Access.port(ctx, :document_facts).insert_revision_for_document(ctx, document, revision)

  @doc "Inserts a revision or accepts an identical existing row."
  @spec insert_or_accept_revision(BackendContext.t(), binary(), Revision.t()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def insert_or_accept_revision(%BackendContext{} = ctx, document_id, revision),
    do: Access.port(ctx, :document_facts).insert_or_accept_revision(ctx, document_id, revision)

  @doc "Updates the winning projection for a document."
  @spec update_winning(BackendContext.t(), binary(), Revision.t(), non_neg_integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def update_winning(%BackendContext{} = ctx, document_id, winner, sequence),
    do: Access.port(ctx, :document_facts).update_winning(ctx, document_id, winner, sequence)

  @doc "Updates the winning projection using a pre-encoded live body."
  @spec update_winning_with_body(
          BackendContext.t(),
          binary(),
          Revision.t(),
          non_neg_integer(),
          binary() | nil
        ) ::
          :ok | {:error, ElixirDB.Error.t()}
  def update_winning_with_body(%BackendContext{} = ctx, document_id, winner, sequence, body_json),
    do:
      Access.port(ctx, :document_facts).update_winning_with_body(
        ctx,
        document_id,
        winner,
        sequence,
        body_json
      )

  @doc "Updates the winning projection using an already loaded document fact."
  @spec update_winning_for_document(BackendContext.t(), map(), Revision.t(), non_neg_integer()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def update_winning_for_document(%BackendContext{} = ctx, document, winner, sequence),
    do:
      Access.port(ctx, :document_facts).update_winning_for_document(
        ctx,
        document,
        winner,
        sequence
      )

  @doc "Empties a document after full history purge."
  @spec empty_document(BackendContext.t(), binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def empty_document(%BackendContext{} = ctx, document_id),
    do: Access.port(ctx, :document_facts).empty_document(ctx, document_id)

  @doc "Deletes all revisions for a history id on a document."
  @spec delete_history(BackendContext.t(), binary(), binary()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_history(%BackendContext{} = ctx, document_id, history_id),
    do: Access.port(ctx, :document_facts).delete_history(ctx, document_id, history_id)

  @doc "Lists documents and revisions eligible for compaction at a floor."
  @spec list_compaction_documents(BackendContext.t(), non_neg_integer()) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_compaction_documents(%BackendContext{} = ctx, candidate_floor),
    do: Access.port(ctx, :document_facts).list_compaction_documents(ctx, candidate_floor)

  @doc "Deletes specific revisions for a document."
  @spec delete_revisions(BackendContext.t(), binary(), [binary()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def delete_revisions(%BackendContext{} = ctx, document_id, revision_ids),
    do: Access.port(ctx, :document_facts).delete_revisions(ctx, document_id, revision_ids)

  @doc "Allocates one change sequence."
  @spec allocate_sequence(BackendContext.t()) ::
          {:ok, integer()} | {:error, ElixirDB.Error.t()}
  def allocate_sequence(%BackendContext{} = ctx) do
    case Access.port(ctx, :change_log).allocate_sequences(ctx, 1) do
      {:ok, [sequence]} -> {:ok, sequence}
      {:ok, []} -> {:error, ElixirDB.Error.internal_error("sequence allocation returned empty")}
      {:error, _} = error -> error
    end
  end

  @doc "Allocates `count` change sequences."
  @spec allocate_sequences(BackendContext.t(), non_neg_integer()) ::
          {:ok, [integer()]} | {:error, ElixirDB.Error.t()}
  def allocate_sequences(%BackendContext{} = ctx, count),
    do: Access.port(ctx, :change_log).allocate_sequences(ctx, count)

  @doc "Builds the backend-neutral change entry accepted by the change-log port."
  @spec change_entry(pos_integer(), binary(), Revision.t(), binary(), binary(), map()) ::
          change_entry()
  def change_entry(sequence, document_id, winner, leaf_json, origin, backend_meta) do
    %{
      sequence: sequence,
      document_id: document_id,
      winner: winner,
      leaf_json: leaf_json,
      origin: origin,
      backend_meta: backend_meta
    }
  end

  @doc "Appends one change-log entry."
  @spec append_change(BackendContext.t(), change_entry()) :: :ok | {:error, ElixirDB.Error.t()}
  def append_change(%BackendContext{} = ctx, entry),
    do: Access.port(ctx, :change_log).append_change(ctx, entry)

  @doc "Appends an ordered batch of change-log entries."
  @spec append_changes(BackendContext.t(), [change_entry()]) :: :ok | {:error, ElixirDB.Error.t()}
  def append_changes(%BackendContext{} = ctx, entries) when is_list(entries),
    do: Access.port(ctx, :change_log).append_changes(ctx, entries)

  @doc "Ensures every digest in a manifest is reachable."
  @spec ensure_manifest_reachable(BackendContext.t(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def ensure_manifest_reachable(%BackendContext{} = ctx, manifest),
    do: Attachments.ensure_manifest_reachable(ctx, manifest)

  @doc "Clears pending protection for digests in a manifest."
  @spec clear_pending_for_manifest(BackendContext.t(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def clear_pending_for_manifest(%BackendContext{} = ctx, manifest),
    do: Attachments.clear_pending_for_manifest(ctx, manifest)

  @doc "Verifies physical attachment digests before import."
  @spec verify_physical_digests(BackendContext.t(), [{binary(), non_neg_integer()}]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def verify_physical_digests(%BackendContext{} = ctx, digests),
    do: Attachments.verify_physical_digests(ctx, digests)

  @doc "Lists retention boundaries."
  @spec list_boundaries(BackendContext.t(), keyword()) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_boundaries(%BackendContext{} = ctx, opts \\ []),
    do: Access.port(ctx, :retention_records).list_boundaries(ctx, opts)

  @doc "Lists peer ledger positions."
  @spec list_peer_positions(BackendContext.t()) ::
          {:ok, [map()]} | {:error, ElixirDB.Error.t()}
  def list_peer_positions(%BackendContext{} = ctx),
    do: Access.port(ctx, :retention_records).list_peer_positions(ctx)

  @doc "Applies a compaction effect atomically."
  @spec apply_compaction_effect(BackendContext.t(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def apply_compaction_effect(%BackendContext{} = ctx, effect),
    do: Access.port(ctx, :retention_records).apply_compaction_effect(ctx, effect)

  @doc "Loads boundary install state for a source database."
  @spec boundary_install_state(BackendContext.t(), binary()) ::
          {:ok, map() | nil} | {:error, ElixirDB.Error.t()}
  def boundary_install_state(%BackendContext{} = ctx, source_uuid),
    do: Access.port(ctx, :retention_records).boundary_install_state(ctx, source_uuid)

  @doc "Begins a staged boundary install."
  @spec begin_boundary_install(BackendContext.t(), binary(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def begin_boundary_install(%BackendContext{} = ctx, install_id, state),
    do: Access.port(ctx, :retention_records).begin_boundary_install(ctx, install_id, state)

  @doc "Stages one boundary page into an install."
  @spec stage_boundary_page(BackendContext.t(), binary(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def stage_boundary_page(%BackendContext{} = ctx, install_id, page),
    do: Access.port(ctx, :retention_records).stage_boundary_page(ctx, install_id, page)

  @doc "Completes a staged boundary install."
  @spec complete_boundary_install(BackendContext.t(), binary()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def complete_boundary_install(%BackendContext{} = ctx, install_id),
    do: Access.port(ctx, :retention_records).complete_boundary_install(ctx, install_id)

  @doc "Replaces the authoritative boundary set for a source."
  @spec replace_boundary_set(BackendContext.t(), map(), [term()]) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def replace_boundary_set(%BackendContext{} = ctx, state, boundaries),
    do: Access.port(ctx, :retention_records).replace_boundary_set(ctx, state, boundaries)

  @doc "Reads the retention maintenance counter."
  @spec maintenance_counter(BackendContext.t()) ::
          {:ok, non_neg_integer()} | {:error, ElixirDB.Error.t()}
  def maintenance_counter(%BackendContext{} = ctx),
    do: Access.port(ctx, :retention_records).maintenance_counter(ctx)

  @doc "Updates a peer ledger wire value without CAS."
  @spec update_peer_wire(BackendContext.t(), binary(), map()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def update_peer_wire(%BackendContext{} = ctx, peer_uuid, wire),
    do: Access.port(ctx, :retention_records).update_peer_wire(ctx, peer_uuid, wire)

  @doc "Compare-and-swaps a peer ledger record."
  @spec put_peer_position_record(BackendContext.t(), map()) ::
          {:ok, map()} | {:error, ElixirDB.Error.t()}
  def put_peer_position_record(%BackendContext{} = ctx, request),
    do: Access.port(ctx, :retention_records).put_peer_position_record(ctx, request)

  @doc "Installs imported purged boundaries."
  @spec install_imported_boundaries(BackendContext.t(), [map()]) ::
          :ok | {:error, ElixirDB.Error.t()}
  def install_imported_boundaries(%BackendContext{} = ctx, boundaries),
    do: Access.port(ctx, :retention_records).install_imported_boundaries(ctx, boundaries)

  @doc "Marks pending local causal state after a local write."
  @spec mark_pending_local_causal(BackendContext.t()) :: :ok | {:error, ElixirDB.Error.t()}
  def mark_pending_local_causal(%BackendContext{} = ctx),
    do: Access.port(ctx, :retention_records).mark_pending_local_causal(ctx)

  @doc "Returns whether pending local causal work exists."
  @spec pending_local_causal?(BackendContext.t()) ::
          {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def pending_local_causal?(%BackendContext{} = ctx),
    do: Access.port(ctx, :retention_records).pending_local_causal?(ctx)

  @doc "Encodes a stored boundary for bootstrap wire payloads."
  @spec encode_stored_boundary(BackendContext.t(), map()) :: map()
  def encode_stored_boundary(%BackendContext{} = ctx, stored),
    do: Access.port(ctx, :retention_records).encode_stored_boundary(stored)

  @doc "Loads ready index definitions for bulk mutation reuse."
  @spec ready_definitions(BackendContext.t()) :: {:ok, [term()]} | {:error, ElixirDB.Error.t()}
  def ready_definitions(%BackendContext{} = ctx),
    do: Access.port(ctx, :index_candidates).ready_definitions(ctx)

  @doc "Refreshes ready indexes for a document winner."
  @spec refresh_document(BackendContext.t(), binary(), map(), term()) ::
          :ok | {:error, ElixirDB.Error.t()}
  def refresh_document(%BackendContext{} = ctx, document_id, winner, ready \\ :load),
    do: Access.port(ctx, :index_candidates).refresh_document(ctx, document_id, winner, ready)

  @doc "Delegates revision equality to shared Compare."
  @spec same_revision?(Revision.t(), Revision.t()) :: boolean()
  def same_revision?(a, b), do: Compare.same?(a, b)

  @doc "Delegates leaf-set encoding to shared Compare."
  @spec encode_leaf_set([Revision.t()]) :: {:ok, binary()} | {:error, term()}
  def encode_leaf_set(leaves), do: Compare.encode_leaf_set(leaves)

  @doc "Returns opaque backend identity from the context."
  @spec identity(BackendContext.t()) :: map()
  def identity(%BackendContext{identity: identity}) when is_map(identity), do: identity
  def identity(_), do: %{}

  @doc "Returns backend_meta from a document fact."
  @spec backend_meta(map() | nil) :: map()
  def backend_meta(%{backend_meta: meta}) when is_map(meta), do: meta
  def backend_meta(_), do: %{}
end
