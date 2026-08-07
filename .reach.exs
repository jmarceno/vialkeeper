[
  layers: [
    transport: "ElixirDB.HTTP.*",
    application: [
      "ElixirDB.Documents",
      "ElixirDB.Changes",
      "ElixirDB.Query",
      "ElixirDB.Replication",
      "ElixirDB.Observability.Dashboard"
    ],
    runtime: "ElixirDB.Runtime.*",
    storage: "ElixirDB.Storage.*",
    core: [
      "ElixirDB.Domain.*",
      "ElixirDB.Revisions.*",
      "ElixirDB.JSON.*",
      "ElixirDB.Commands",
      "ElixirDB.Query.*",
      "ElixirDB.Error",
      "ElixirDB.UUID"
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
