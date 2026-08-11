[
  layers: [
    transport: ["ElixirDB.HTTP.*", "ElixirDB.WebUI", "ElixirDB.WebUI.*"],
    application: [
      "ElixirDB.Documents",
      "ElixirDB.Changes",
      "ElixirDB.Query",
      "ElixirDB.Views",
      "ElixirDB.MaterializedViews",
      "ElixirDB.Federation",
      "ElixirDB.Federation.Executor",
      "ElixirDB.Federation.Normalizer",
      "ElixirDB.Federation.BookmarkCodec",
      "ElixirDB.Federation.Ordering",
      "ElixirDB.Federation.SourceCursor",
      "ElixirDB.Federation.SourceDocument",
      "ElixirDB.Federation.SavedQueries",
      "ElixirDB.Replication",
      "ElixirDB.Attachments",
      "ElixirDB.Observability.Dashboard"
    ],
    runtime: [
      "ElixirDB.Runtime.*",
      "ElixirDB.Replication.JobManager",
      "ElixirDB.Query.Subscriptions",
      "ElixirDB.Query.Subscription",
      "ElixirDB.Query.SubscriptionHub",
      "ElixirDB.Query.SubscriptionSupervisor"
    ],
    storage: [
      "ElixirDB.Storage",
      "ElixirDB.Storage.Adapter",
      "ElixirDB.Storage.Results",
      "ElixirDB.Storage.Ports",
      "ElixirDB.Storage.Ports.*",
      "ElixirDB.Storage.BackendContext",
      "ElixirDB.Storage.PhysicalAllowlist",
      "ElixirDB.Storage.BoundaryGuard",
      "ElixirDB.Storage.Registry",
      "ElixirDB.Storage.Sentinel",
      "ElixirDB.Storage.Sentinel.*",
      "ElixirDB.Attachments.FilesystemStore",
      "ElixirDB.Attachments.Compression"
    ],
    sqlite_backend: ["ElixirDB.Storage.SQLite", "ElixirDB.Storage.SQLite.*"],
    core: [
      "ElixirDB.Domain.*",
      "ElixirDB.Revisions.*",
      "ElixirDB.JSON.*",
      "ElixirDB.Commands",
      "ElixirDB.Query.*",
      "ElixirDB.Error",
      "ElixirDB.UUID",
      "ElixirDB.DurableFS",
      "ElixirDB.PathSafety",
      "ElixirDB.DatabaseBundle",
      "ElixirDB.Attachments.Manifest",
      "ElixirDB.Attachments.Ticket",
      "ElixirDB.Attachments.Store",
      "ElixirDB.Replication.BlobStream"
    ],
    observability: "ElixirDB.Observability.*"
  ],
  deps: [
    forbidden: [
      {:core, :transport},
      {:core, :runtime},
      {:core, :storage},
      {:core, :sqlite_backend},
      {:storage, :transport},
      {:storage, :sqlite_backend},
      {:runtime, :transport},
      {:runtime, :sqlite_backend},
      {:application, :sqlite_backend},
      {:transport, :sqlite_backend},
      {:observability, :sqlite_backend}
    ]
  ],
  calls: [
    forbidden: [
      {
        [
          "ElixirDB.Runtime.*",
          "ElixirDB.Application",
          "ElixirDB.Diagnostics",
          "ElixirDB.Domain.*",
          "ElixirDB.Query.*",
          "ElixirDB.View.*",
          "ElixirDB.Retention.*",
          "ElixirDB.Federation.*",
          "ElixirDB.Documents",
          "ElixirDB.Changes",
          "ElixirDB.Views",
          "ElixirDB.MaterializedViews",
          "ElixirDB.Replication",
          "ElixirDB.Replication.*",
          "ElixirDB.Attachments",
          "ElixirDB.Commands",
          "ElixirDB.DatabaseBundle",
          "ElixirDB.Storage.Ports",
          "ElixirDB.Storage.Ports.*",
          "ElixirDB.Storage.BackendContext",
          "ElixirDB.Storage.BoundaryGuard",
          "ElixirDB.Storage.Registry",
          "ElixirDB.Storage.Sentinel.*"
        ],
        ["ElixirDB.Storage.SQLite.*", "Exqlite.*", "Exqlite.Sqlite3.*"]
      }
    ]
  ]
]
