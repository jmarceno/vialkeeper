[
  layers: [
    transport: "ElixirDB.HTTP.*",
    application: [
      "ElixirDB.Documents",
      "ElixirDB.Changes",
      "ElixirDB.Query",
      "ElixirDB.Federation",
      "ElixirDB.Federation.Executor",
      "ElixirDB.Federation.Normalizer",
      "ElixirDB.Federation.BookmarkCodec",
      "ElixirDB.Federation.Ordering",
      "ElixirDB.Federation.SourceCursor",
      "ElixirDB.Federation.SavedQueries",
      "ElixirDB.Replication",
      "ElixirDB.Attachments",
      "ElixirDB.Observability.Dashboard"
    ],
    runtime: [
      "ElixirDB.Runtime.*",
      "ElixirDB.Query.Subscriptions",
      "ElixirDB.Query.Subscription",
      "ElixirDB.Query.SubscriptionHub",
      "ElixirDB.Query.SubscriptionSupervisor"
    ],
    storage: [
      "ElixirDB.Storage.*",
      "ElixirDB.Attachments.FilesystemStore",
      "ElixirDB.Attachments.Compression"
    ],
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
      {:storage, :transport},
      {:runtime, :transport}
    ]
  ]
]
