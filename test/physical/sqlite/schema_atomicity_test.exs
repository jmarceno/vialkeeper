defmodule VialKeeper.Storage.SQLite.SchemaAtomicityTest do
  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Storage.SQLite.{Connection, Schema}

  test "failed initialization rolls back the schema and metadata together" do
    {:ok, bundle_path} = VialKeeper.TempDatabase.create(prefix: "vialkeeper-schema-atomic")
    path = VialKeeper.TempDatabase.sqlite_path(bundle_path)
    {:ok, conn} = Connection.open(path)

    on_exit(fn ->
      _ = Connection.close(conn)
      VialKeeper.TempDatabase.cleanup(bundle_path)
    end)

    :ok = Schema.configure(conn)
    config_json = Canonical.encode!(VialKeeper.Config.defaults())

    assert {:error, %VialKeeper.Error{code: :internal_error}} =
             Schema.create(conn, VialKeeper.UUID.v4(), config_json,
               database_kind: :ordinary,
               initial_derived_view: %{}
             )

    assert {:ok, []} =
             Connection.query(
               conn,
               "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'db_meta'"
             )
  end
end
