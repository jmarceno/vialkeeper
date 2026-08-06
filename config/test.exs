import Config

config :elixir_db,
  database_root: Path.expand("tmp/test-databases", File.cwd!()),
  registration_manifest: Path.expand("tmp/test-databases/registrations.json", File.cwd!()),
  listener: [ip: {127, 0, 0, 1}, port: 0]
