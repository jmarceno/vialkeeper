defmodule ElixirDB.Runtime.AdmissionTest do
  @moduledoc "Admission behavior tests for runtime commands."
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.{DatabaseAdmission, DatabaseCatalog}
  alias ElixirDB.View.Manager

  setup do
    previous_limits = Application.get_env(:elixir_db, :host_limits)
    previous_policy = Application.get_env(:elixir_db, :admission_policy)

    limits = Keyword.put(previous_limits || [], :admission_limit, 1)

    policy = [
      foreground_weight: 8,
      subscription_weight: 4,
      replication_weight: 2,
      maintenance_weight: 1,
      foreground_reserved_slots: 0,
      subscription_reserved_slots: 0,
      replication_reserved_slots: 0,
      maintenance_reserved_slots: 0
    ]

    Application.put_env(:elixir_db, :host_limits, limits)
    Application.put_env(:elixir_db, :admission_policy, policy)

    on_exit(fn ->
      Application.put_env(:elixir_db, :host_limits, previous_limits)
      Application.put_env(:elixir_db, :admission_policy, previous_policy)
    end)

    relative = "admission-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    assert {:ok, _} = DatabaseCatalog.open(identity.database_uuid)
    assert :ok = Manager.await_resumed(identity.database_uuid)

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

  test "execute_with_deadline fails when absolute deadline is already past", %{uuid: uuid} do
    deadline_ms = System.monotonic_time(:millisecond) - 1

    assert {:error,
            %ElixirDB.Error{
              code: :internal_error,
              retryable: true,
              details: %{reason: :deadline_exhausted}
            }} =
             ElixirDB.Query.execute_with_deadline(uuid, %{selector: %{}, limit: 1}, deadline_ms)
  end

  test "execute_owner completes with :infinity timeout", %{uuid: uuid} do
    assert {:ok, :done} =
             DatabaseAdmission.execute_owner(uuid, :foreground, fn -> {:ok, :done} end, :infinity)
  end

  test "catalog command completes with :infinity timeout", %{uuid: uuid} do
    assert {:ok, identity} =
             DatabaseCatalog.command(uuid, {:command, :identity, %{}}, :infinity)

    assert identity.database_uuid == uuid
  end
end
