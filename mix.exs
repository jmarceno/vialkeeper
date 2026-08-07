defmodule ElixirDB.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :elixir_db,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      dialyzer: [
        plt_add_apps: [
          :ex_unit,
          :mix,
          :opentelemetry_api,
          :opentelemetry_api_experimental,
          :opentelemetry,
          :opentelemetry_experimental,
          :opentelemetry_exporter
        ]
      ],
      releases: releases()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def cli do
    [
      preferred_envs: [
        bench: :test,
        "check.fast": :test,
        "check.full": :test,
        "release.build": :prod
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto], mod: {ElixirDB.Application, []}]
  end

  defp deps do
    [
      {:exqlite, "0.39.0"},
      {:plug, "1.20.3"},
      {:bandit, "1.12.4"},
      {:req, "0.7.2"},
      {:telemetry, "1.4.2"},
      # TOML parser for the host configuration file (<database_root>/host.toml).
      {:toml, "~> 0.7"},
      # OpenTelemetry. runtime: false keeps the SDK/exporter apps from auto-starting;
      # ElixirDB.Observability.Supervisor starts them only when an OTLP endpoint is
      # configured (see config/runtime.exs). The lockfile pins exact versions.
      {:opentelemetry_api, "~> 1.4", runtime: false},
      {:opentelemetry, "~> 1.5", runtime: false},
      {:opentelemetry_exporter, "~> 1.8", runtime: false},
      # The stable opentelemetry SDK ships tracing only; the metrics signal
      # (otel_meter/otel_counter/otel_histogram and the metric reader) lives in
      # the experimental package as of opentelemetry 1.7.
      {:opentelemetry_experimental, "~> 0.5.1", runtime: false},
      {:stream_data, "1.4.0", only: [:test, :dev]},
      {:dialyxir, "1.4.7", only: [:dev, :test], runtime: false},
      # Development-only quality and agent tooling. None of these packages are
      # reachable from the assembled production release.
      {:credo, "1.7.19", only: [:dev, :test], runtime: false},
      {:ex_slop, "0.4.4", only: [:dev, :test], runtime: false},
      {:ex_dna, "1.5.4", only: [:dev, :test], runtime: false},
      # Reach 2.8 currently requires the 0.12 ExAST API line.
      {:ex_ast, "0.12.10", only: [:dev, :test], runtime: false},
      {:reach, "2.8.2", only: [:dev, :test], runtime: false}
    ]
  end

  defp releases do
    [
      elixir_db: [
        include_executables_for: [:unix],
        applications: [runtime_tools: :permanent],
        steps: [:assemble, &ElixirDB.ReleaseSteps.patch_launcher/1]
      ]
    ]
  end

  defp aliases do
    [
      "check.fast": [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --strict",
        "ex_dna --max-clones 0",
        "test --warnings-as-errors --exclude slow"
      ],
      "check.full": [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "credo --strict",
        "ex_dna --max-clones 0",
        "reach.check --arch --smells --strict",
        "test --warnings-as-errors",
        "dialyzer"
      ],
      bench: ["run bench/elixirdb_benchmark.exs"],
      "release.build": ["deps.get", "compile", "release --overwrite"]
    ]
  end
end
