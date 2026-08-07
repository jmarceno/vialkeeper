defmodule ElixirDB.Storage.SQLite.StructuredIndexes do
  @moduledoc """
  Structured logical-index facade for the Version 1 SQLite adapter.

  Physical DDL and probes live in `Indexes`. Catalog-row transactions live in
  `IndexCatalog` (via the adapter); this module thin-wraps the structured
  physical path.
  """

  use ElixirDB.Storage.SQLite.IndexFacade
end
