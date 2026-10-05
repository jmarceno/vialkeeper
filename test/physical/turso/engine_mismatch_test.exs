defmodule VialKeeper.Storage.Turso.EngineMismatchTest do
  @moduledoc "A bundle belongs to the engine that created it and never opens on the other one."
  use ExUnit.Case, async: true

  @moduletag :turso_physical

  alias VialKeeper.Error
  alias VialKeeper.Storage.SQLite.Adapter, as: SQLiteAdapter
  alias VialKeeper.Storage.Turso.Adapter, as: TursoAdapter

  setup do
    {:ok, bundle} = VialKeeper.TempDatabase.create(prefix: "engine-mismatch")
    on_exit(fn -> File.rm_rf(bundle) end)
    %{bundle: bundle}
  end

  for {creator, creator_name, opener, opener_name} <- [
        {SQLiteAdapter, "sqlite", TursoAdapter, "turso"},
        {TursoAdapter, "turso", SQLiteAdapter, "sqlite"}
      ] do
    test "a #{creator_name} bundle does not open or get recreated on #{opener_name}", %{
      bundle: bundle
    } do
      creator = unquote(creator)
      opener = unquote(opener)
      message = "bundle was created by the #{unquote(creator_name)} storage engine"

      {:ok, adapter} = creator.create(creator.artifact_path(bundle), %{})
      :ok = creator.close(adapter)

      assert {:error, %Error{code: :unsupported_format, message: ^message}} =
               opener.open(opener.artifact_path(bundle))

      assert {:error, %Error{code: :unsupported_format, message: ^message}} =
               opener.create(opener.artifact_path(bundle), %{})

      refute File.exists?(opener.artifact_path(bundle))
      assert {:ok, reopened} = creator.open(creator.artifact_path(bundle))
      assert :ok = creator.close(reopened)
    end

    test "a #{creator_name} file renamed to the #{opener_name} artifact is unsupported", %{
      bundle: bundle
    } do
      creator = unquote(creator)
      opener = unquote(opener)

      {:ok, adapter} = creator.create(creator.artifact_path(bundle), %{})
      :ok = creator.close(adapter)
      File.rename!(creator.artifact_path(bundle), opener.artifact_path(bundle))

      assert {:error, %Error{code: :unsupported_format}} =
               opener.open(opener.artifact_path(bundle))
    end
  end
end
