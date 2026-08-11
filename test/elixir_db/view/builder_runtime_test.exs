defmodule ElixirDB.View.BuilderRuntimeTest do
  @moduledoc "Runtime tests for declarative view rebuild and catch-up behavior."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestSupport.{AdmissionClassProbe, ViewBuilderProbe}
  alias ElixirDB.View.Manager
  alias ElixirDB.Views

  @view %{
    "name" => "counts",
    "key" => [%{"path" => "/k"}],
    "reducer" => "_count"
  }

  setup do
    rel = "view-runtime-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    abs = Path.join(root, rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      AdmissionClassProbe.uninstall()
      ViewBuilderProbe.uninstall()
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "consistent waiting holds no admission permit or owner call", %{uuid: uuid} do
    admission_probe = AdmissionClassProbe.install()
    builder_probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"k" => "x"}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(builder_probe, :generation_activated)
    AdmissionClassProbe.drain(admission_probe)

    body_gate = ViewBuilderProbe.install_apply_view_batch_barrier(uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "b", body: %{"k" => "y"}}}
             )

    assert {:ok, %{current_sequence: target}} =
             DatabaseCatalog.command(uuid, {:command, :identity, %{}})

    {metadata, executor} =
      ViewBuilderProbe.await_incremental_apply_blocked(builder_probe, body_gate)

    assert metadata.expected < target
    assert metadata.through == target

    AdmissionClassProbe.drain(admission_probe)

    waiter =
      Task.async(fn ->
        Views.await_consistent(uuid, view_id, target, 5_000)
      end)

    refute match?({:ok, :ok}, Task.yield(waiter, 200))
    assert AdmissionClassProbe.drain(admission_probe, 100) == []

    :ok = ViewBuilderProbe.release_apply_view_batch_barrier(body_gate, executor)
    assert :ok = Task.await(waiter, 6_000)

    assert {:ok, %{indexed_through: indexed_through}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert indexed_through >= target
  end

  test "incremental batches keep only the latest change for each document", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :generation_activated)
    assert :ok = Manager.stop_builder(uuid, view_id)

    assert :ok =
             await_builder_absent(uuid, view_id,
               message: "builder did not stop before incremental changes"
             )

    assert {:ok, first} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"k" => "first"}}}
             )

    assert {:ok, _second} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put,
                %{
                  document_id: "a",
                  body: %{"k" => "latest"},
                  if_revision: first.revision
                }}
             )

    assert :ok = Manager.start_builder(uuid, view_id)

    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             ) do
          {:ok, %{results: [%{"key" => ["latest"], "value" => 1}]}} -> true
          _ -> false
        end
      end,
      timeout: 30_000,
      message: "view did not converge to the latest document revision"
    )
  end

  test "history truncation abandons and restarts generation", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    for id <- ["a", "b"] do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: id, body: %{"k" => id}}}
               )
    end

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert :ok = Manager.stop_builder(uuid, view_id)

    assert :ok =
             await_builder_absent(uuid, view_id,
               message: "builder did not stop before history truncation"
             )

    for id <- ["c", "d"] do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: id, body: %{"k" => id}}}
               )
    end

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "retention" => %{
                    "mode" => "stable_frontier",
                    "history_depth" => 0,
                    "peer_expiry_ms" => 86_400_000,
                    "schedule" => "disabled"
                  }
                }}
             )

    assert {:ok, %{new_floor: floor}} =
             DatabaseCatalog.command(uuid, {:command, :compact_retention, %{trigger: :explicit}})

    assert floor > 2

    assert {:error, %ElixirDB.Error{code: :history_truncated}} =
             ElixirDB.Changes.read(uuid, %{since: 2, limit: 10}, admission_class: :maintenance)

    assert :ok = Manager.start_builder(uuid, view_id)
    ViewBuilderProbe.await(probe, :rebuild_started)

    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(uuid, {:command, :view_state, view_id}) do
          {:ok, %{indexed_through: indexed_through, status: "ready"}} when indexed_through >= 4 ->
            true

          _ ->
            false
        end
      end,
      timeout: 30_000,
      message: "view indexed_through did not catch up after history truncation"
    )

    assert {:ok, %{results: [_ | _] = results}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             )

    assert Enum.count(results) == 4
  end

  defp await_builder_absent(uuid, view_id, opts) do
    deadline = System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 5_000)
    message = Keyword.fetch!(opts, :message)
    await_builder_absent(uuid, view_id, deadline, message)
  end

  defp await_builder_absent(uuid, view_id, deadline, message) do
    if System.monotonic_time(:millisecond) >= deadline do
      flunk(message)
    else
      case Manager.builder_pid(uuid, view_id) do
        :error -> :ok
        {:ok, _pid} -> await_builder_absent(uuid, view_id, deadline, message)
      end
    end
  end
end
