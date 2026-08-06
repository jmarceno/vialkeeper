defmodule ElixirDB.Diagnostics do
  @moduledoc "Runtime and SQLite compatibility metadata for releases and support."

  alias ElixirDB.Storage.SQLite.Connection

  def runtime do
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[sqlite_version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         {:ok, compile_options} <- Connection.query(conn, "PRAGMA compile_options") do
      _ = Connection.close(conn)

      %{
        elixir: System.version(),
        otp: :erlang.system_info(:otp_release) |> List.to_string(),
        exqlite: Application.spec(:exqlite, :vsn) |> to_string(),
        sqlite: sqlite_version,
        sqlite_compile_options: Enum.map(compile_options, &List.first/1),
        protocol_major: ElixirDB.protocol_major(),
        revision_algorithm_version: ElixirDB.revision_algorithm_version(),
        canonicalization_version: ElixirDB.canonicalization_version()
      }
    else
      _ -> %{elixir: System.version(), otp: :erlang.system_info(:otp_release) |> List.to_string()}
    end
  end

  def validate_sqlite! do
    with {:ok, conn} <- Connection.open(":memory:"),
         {:ok, [[version]]} <- Connection.query(conn, "SELECT sqlite_version()"),
         true <- version_at_least?(version, "3.45.0"),
         {:ok, _} <- Connection.query(conn, "CREATE VIRTUAL TABLE fts_probe USING fts5(body)") do
      _ = Connection.close(conn)
      version
    else
      reason -> raise "SQLite runtime does not satisfy Version 1 capabilities: #{inspect(reason)}"
    end
  end

  defp version_at_least?(actual, minimum) do
    parse = fn value -> value |> String.split(".") |> Enum.map(&String.to_integer/1) end
    parse.(actual) >= parse.(minimum)
  end
end
