defmodule VialKeeper.Storage.SQLite.FullTextIndexes do
  @moduledoc """
  Full-text logical-index facade for the Version 1 SQLite adapter.

  Catalog rows and adapter metadata live in SQLite. Token posting lists are
  owned by `VialKeeper.Search`; this module only wraps catalog physical create
  and drop (no SQLite FTS table).
  """

  use VialKeeper.Storage.SQLite.IndexFacade
end
