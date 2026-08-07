defmodule ElixirDB.Runtime.AdmissionTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{DatabaseAdmission, DatabaseCatalog}

  setup do
    previous = Application.get_env(:elixir_db, :host_limits)
    limits = Keyword.put(previous || [], :admission_limit, 1)
    Application.put_env(:elixir_db, :host_limits, limits)

    on_exit(fn ->
      Application.put_env(:elixir_db, :host_limits, previous)
    end)

    relative = "admission-#{System.unique_integer([:positive])}.db"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: identity.database_uuid}
  end

  test "admission saturates at the configured limit", %{uuid: uuid} do
    parent = self()
    gate = make_ref()

    holder =
      Task.async(fn ->
        DatabaseAdmission.with_token(uuid, fn ->
          send(parent, {:held, gate})

          receive do
            {:release, ^gate} -> :ok
          after
            5_000 -> :ok
          end

          :held
        end)
      end)

    assert_receive {:held, ^gate}, 1_000
    assert {:ok, 1} = DatabaseAdmission.active_count(uuid)

    assert {:error, %ElixirDB.Error{code: :database_overloaded}} =
             DatabaseAdmission.with_token(uuid, fn -> :should_not_run end)

    send(holder.pid, {:release, gate})
    assert :held = Task.await(holder)

    assert :after = DatabaseAdmission.with_token(uuid, fn -> :after end)

    assert {:ok, 0} = DatabaseAdmission.active_count(uuid)
  end
end
