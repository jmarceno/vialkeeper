import Config

if System.get_env("ELIXIR_DB_ROOT") do
  config :elixir_db, database_root: Path.expand(System.fetch_env!("ELIXIR_DB_ROOT"))
end
