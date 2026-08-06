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
      dialyzer: [plt_add_apps: [:ex_unit, :mix]],
      releases: releases()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def cli do
    [preferred_envs: ["check.fast": :test, "check.full": :test, "release.build": :prod]]
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
      {:stream_data, "1.4.0", only: [:test, :dev]},
      {:dialyxir, "1.4.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp releases do
    [
      elixir_db: [
        include_executables_for: [:unix],
        applications: [runtime_tools: :permanent]
      ]
    ]
  end

  defp aliases do
    [
      "check.fast": [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "test --warnings-as-errors --exclude slow"
      ],
      "check.full": [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "test --warnings-as-errors",
        "dialyzer"
      ],
      "release.build": ["deps.get", "compile", "release --overwrite"]
    ]
  end
end
