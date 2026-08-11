defmodule ElixirDB.Storage.Ports do
  @moduledoc """
  Neutral storage port families for backend-agnostic ElixirDB.

  Product and runtime code depend on these capability-oriented ports, not on
  engine APIs. A physical backend (for example `ElixirDB.Storage.SQLite`)
  implements the ports; shared services own mutation, replication, retention,
  query, view, and integrity semantics.

  Port families:

  * `:lifecycle` — create/open/close, identity, configuration, capabilities,
    backend diagnostics, and an opaque `ElixirDB.Storage.BackendContext`
  * `:transaction` — run an atomic operation with a backend-selected isolation
    strategy; callers never supply engine transaction text
  * `:ownership` — acquire/release single-database runtime ownership
  * `:document_facts` — document/revision/leaf/winner fact reads and writes
  * `:change_log` — allocate/append/read changes and causal state
  * `:local_records` — versioned get/put-CAS/list/delete over typed namespaces
  * `:retention_records` — peer positions, boundary pages, retention state
  * `:index_candidates` — logical index definitions and candidate retrieval
  * `:view_state` — view definition/state/row persistence
  * `:derived_state` — derived metadata, cursors, contributions, aggregates
  * `:attachment_metadata` — manifests, pending protection, reachability
  * `:inspection` — normalized integrity snapshots and capability probes

  Every port return value is a normalized domain value or a typed storage
  error. Engine handles, SQL, row identifiers, prepared statements, and
  physical path fields must not appear in port types.
  """

  @port_families [
    :lifecycle,
    :transaction,
    :ownership,
    :document_facts,
    :change_log,
    :local_records,
    :retention_records,
    :index_candidates,
    :view_state,
    :derived_state,
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
end
