defmodule ElixirDB.View.BuilderTest do
  @moduledoc "Unit tests for declarative view builder state transitions."
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestSupport.{AdmissionScenario, ViewBuilderProbe}
  alias ElixirDB.View.Manager
  alias ElixirDB.Views

  @view %{
    "name" => "scores",
    "selector" => %{"/kind" => "task"},
    "key" => [%{"path" => "/kind"}],
    "value" => %{"path" => "/score"},
    "reducer" => "_sum"
  }

  setup do
    rel = "view-builder-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    abs = Path.join(root, rel)
    ElixirDB.TempDatabase.cleanup(abs)

    assert {:ok, identity} = DatabaseCatalog.create(rel)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      ViewBuilderProbe.uninstall()
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(abs)
    end)

    {:ok, uuid: uuid}
  end

  test "startup resumes persisted views exactly once", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :rebuild_started)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert [{pid, _}] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id})

    assert :ok = DatabaseCatalog.close(uuid)

    assert [] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id})

    assert {:ok, _} = DatabaseCatalog.open(uuid)

    resumed_pid = await_single_builder!(uuid, view_id)
    assert resumed_pid != pid
    assert_no_duplicate_builders!(uuid, view_id)
  end

  test "mutation after page cursor is corrected by changes replay", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    for id <- ["a", "b"] do
      assert {:ok, _} =
               DatabaseCatalog.command(
                 uuid,
                 {:command, :put, %{document_id: id, body: %{"kind" => "task", "score" => 1}}}
               )
    end

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view, page_size: 1)
    ViewBuilderProbe.await(probe, :rebuild_started)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "c", body: %{"kind" => "task", "score" => 3}}}
             )

    ViewBuilderProbe.await(probe, :snapshot_page_applied)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{results: results}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             )

    assert Enum.sort(Enum.map(results, & &1["key"])) == [["task"]]

    assert {:ok, %{indexed_through: indexed_through}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert indexed_through >= 3
  end

  test "create before and after scanned position are not missed", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"kind" => "task", "score" => 1}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view, page_size: 1)
    ViewBuilderProbe.await(probe, :rebuild_started)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "b", body: %{"kind" => "task", "score" => 2}}}
             )

    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{results: [%{"value" => value}]}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             )

    assert value == 3.0
  end

  test "delete recreate racing scan converges", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"kind" => "task", "score" => 1}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view, page_size: 1)
    ViewBuilderProbe.await(probe, :rebuild_started)
    assert {:ok, _} = Views.delete(uuid, view_id)
    assert {:ok, %{"view_id" => new_view_id}} = Views.create(uuid, @view)

    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{results: [%{"value" => value}]}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => new_view_id, "limit" => 10}}
             )

    assert value == 1.0
    assert :error = Manager.builder_pid(uuid, view_id)
    assert {:ok, _pid} = Manager.builder_pid(uuid, new_view_id)
  end

  test "crash before apply_view_batch repeats at worst", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"kind" => "task", "score" => 1}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{indexed_through: cursor_before}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    body_gate = ViewBuilderProbe.install_apply_view_batch_barrier(uuid)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "b", body: %{"kind" => "task", "score" => 2}}}
             )

    {metadata, executor} =
      ViewBuilderProbe.await_incremental_apply_blocked(probe, body_gate)

    assert metadata.expected == cursor_before

    {:ok, builder_pid} = Manager.builder_pid(uuid, view_id)
    Process.exit(builder_pid, :kill)
    Process.exit(executor, :kill)
    assert :ok = Manager.stop_builder(uuid, view_id)
    ViewBuilderProbe.uninstall_apply_view_batch_barrier()

    AdmissionScenario.await_stats(uuid, &(&1.total_occupancy == 0), timeout: 15_000)

    assert {:ok, %{indexed_through: ^cursor_before}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert :ok = Manager.start_builder(uuid, view_id)

    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(uuid, {:command, :view_state, view_id}) do
          {:ok, %{indexed_through: cursor, status: "ready"}} when cursor > cursor_before -> true
          _ -> false
        end
      end,
      message: "view did not catch up after builder crash before apply"
    )
  end

  test "crash after apply_view_batch resumes from advanced cursor", %{uuid: uuid} do
    probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"kind" => "task", "score" => 1}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{indexed_through: cursor_before}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "b", body: %{"kind" => "task", "score" => 2}}}
             )

    %{indexed_through: advanced} = ViewBuilderProbe.await(probe, :after_incremental_apply)
    assert advanced > cursor_before

    {:ok, builder_pid} = Manager.builder_pid(uuid, view_id)
    Process.exit(builder_pid, :kill)

    assert {:ok, %{indexed_through: ^advanced}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "c", body: %{"kind" => "task", "score" => 3}}}
             )

    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(
               uuid,
               {:command, :query_view, %{"view_id" => view_id, "limit" => 10}}
             ) do
          {:ok, %{results: [%{"value" => value}]}} when value == 6.0 -> true
          _ -> false
        end
      end,
      message: "view did not resume incremental catch-up after crash after apply"
    )
  end

  test "database close terminates builders and reopen resumes from persisted cursor", %{
    uuid: uuid
  } do
    probe = ViewBuilderProbe.install()

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put, %{document_id: "a", body: %{"kind" => "task", "score" => 1}}}
             )

    assert {:ok, %{"view_id" => view_id}} = Views.create(uuid, @view)
    ViewBuilderProbe.await(probe, :generation_activated)

    assert {:ok, %{indexed_through: cursor}} =
             DatabaseCatalog.command(uuid, {:command, :view_state, view_id})

    assert cursor > 0

    assert :ok = DatabaseCatalog.close(uuid)

    assert [] =
             Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id})

    assert {:ok, _} = DatabaseCatalog.open(uuid)

    Eventual.eventually(
      fn ->
        case DatabaseCatalog.command(uuid, {:command, :view_state, view_id}) do
          {:ok, %{indexed_through: resumed}} when resumed >= cursor -> true
          _ -> false
        end
      end,
      message: "view did not resume from persisted cursor"
    )
  end

  defp await_single_builder!(uuid, view_id, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_single_builder_loop(uuid, view_id, deadline)
  end

  defp await_single_builder_loop(uuid, view_id, deadline) do
    case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
      [{pid, _}] ->
        pid

      [] ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("view builder did not resume after reopen")
        else
          await_single_builder_loop(uuid, view_id, deadline)
        end

      entries ->
        flunk("expected one view builder after reopen, got #{inspect(entries)}")
    end
  end

  defp assert_no_duplicate_builders!(uuid, view_id, samples \\ 20) do
    for _ <- 1..samples do
      case Registry.lookup(ElixirDB.Runtime.DatabaseRegistry, {:view_builder, uuid, view_id}) do
        [{_pid, _}] -> :ok
        [] -> :ok
        entries -> flunk("duplicate view builders observed: #{inspect(entries)}")
      end
    end
  end
end
