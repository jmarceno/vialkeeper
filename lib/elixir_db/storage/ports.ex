defmodule ElixirDB.Storage.Ports do
  @moduledoc """
  Neutral storage port families for backend-agnostic ElixirDB.

  Product and runtime code depend on these capability-oriented ports, not on
  engine APIs. A physical backend (for example `ElixirDB.Storage.SQLite`)
  implements the ports; shared services own mutation, replication, retention,
  query, view, and integrity semantics.

  Port families:

  * `:lifecycle` — create/open/close, optional snapshot readers, identity,
    configuration, capabilities, backend diagnostics, and an opaque
    `ElixirDB.Storage.BackendContext`
  * `:transaction` — run an atomic write or a read snapshot with a
    backend-selected isolation strategy; callers never supply engine
    transaction text
  * `:ownership` — acquire/release single-database runtime ownership
  * `:document_facts` — document/revision/leaf/winner fact reads and writes
  * `:change_log` — allocate/append/read changes and causal state
  * `:local_records` — versioned get/put-CAS/list/delete over typed namespaces
  * `:shadow_state` — immutable shadow binding and monotonic source origins
  * `:retention_records` — peer positions, boundary pages, retention state
  * `:index_candidates` — logical index definitions and candidate retrieval
  * `:view_state` — view definition/state/row fact persistence and range scans
  * `:derived_state` — derived metadata, cursors, contribution/group fact maps
  * `:replication_jobs` — durable replication-job definitions and state
  * `:attachment_metadata` — manifests, pending protection, reachability
  * `:inspection` — normalized integrity snapshots and capability probes

  Every port return value is a normalized domain value or a typed storage
  error. Engine handles, SQL, row identifiers, prepared statements, and
  physical path fields must not appear in port types. Product workflows for
  views and derived materialization live in `ElixirDB.Storage.Services`.
  """

  @port_families [
    :lifecycle,
    :transaction,
    :ownership,
    :document_facts,
    :change_log,
    :local_records,
    :shadow_state,
    :retention_records,
    :index_candidates,
    :view_state,
    :derived_state,
    :replication_jobs,
    :attachment_metadata,
    :inspection
  ]

  @doc "Returns the approved storage port family atoms."
  @spec families() :: [atom()]
  def families, do: @port_families

  @doc "Returns true when `family` is an approved port family."
  @spec family?(term()) :: boolean()
  def family?(family) when is_atom(family), do: family in @port_families
  def family?(_), do: false

  @behaviour_modules %{
    lifecycle: ElixirDB.Storage.Ports.Lifecycle,
    transaction: ElixirDB.Storage.Ports.Transaction,
    ownership: ElixirDB.Storage.Ports.Ownership,
    document_facts: ElixirDB.Storage.Ports.DocumentFacts,
    change_log: ElixirDB.Storage.Ports.ChangeLog,
    local_records: ElixirDB.Storage.Ports.LocalRecords,
    shadow_state: ElixirDB.Storage.Ports.ShadowState,
    retention_records: ElixirDB.Storage.Ports.RetentionRecords,
    index_candidates: ElixirDB.Storage.Ports.IndexCandidates,
    view_state: ElixirDB.Storage.Ports.ViewState,
    derived_state: ElixirDB.Storage.Ports.DerivedState,
    replication_jobs: ElixirDB.Storage.Ports.ReplicationJobs,
    attachment_metadata: ElixirDB.Storage.Ports.AttachmentMetadata,
    inspection: ElixirDB.Storage.Ports.Inspection
  }

  @doc "Returns the behaviour module for an approved port family."
  @spec behaviour(atom()) :: module()
  def behaviour(family) when is_atom(family) do
    Map.fetch!(@behaviour_modules, family)
  end

  @doc "Returns the map of port family atoms to behaviour modules."
  @spec behaviours() :: %{atom() => module()}
  def behaviours, do: @behaviour_modules
end
