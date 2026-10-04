defmodule Exqlite.MixProject do
  use Mix.Project

  # Trimmed vendor of exqlite 0.39.0 (MIT, see LICENSE and VENDORED.md).
  @version "0.39.0-vialkeeper.1"

  def project do
    [
      app: :exqlite,
      version: @version,
      elixir: "~> 1.16",
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [{:elixir_make, "~> 0.8", runtime: false}]
  end
end
