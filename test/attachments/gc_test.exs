defmodule ElixirDB.Attachments.GCTest do
  @moduledoc """
  §22.7 attachment GC and stable-frontier post-compact seam proofs.

  Uses explicit barriers (coordinator guards + Application delete hook); no
  arbitrary sleeps for concurrency assertions.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.DatabaseBundle
  alias ElixirDB.Documents
  alias ElixirDB.Eventual
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}

  setup do
    previous_hook = Application.get_env(:elixir_db, :attachment_gc_hook)
    on_exit(fn -> restore_gc_hook(previous_hook) end)

    relative = "attachment-gc-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), relative)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, absolute: absolute}
  end

  test "shared digest survives compact of one referencing revision", %{uuid: uuid} do
    enable_frontier(uuid)
    payload = "shared-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)

    assert {:ok, put1} =
             Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{"n" => 1},
               "attachments" => %{
                 "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    rev1 = put1.revision

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "doc",
               "if_revision" => rev1,
               "body" => %{"n" => 2},
               "attachments" => %{
                 "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    assert {:ok, _} = DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})
    await_gc_idle(uuid)
    assert blob_exists?(uuid, digest)
    assert {:ok, %{deleted: 0}} = Attachments.gc(uuid)
    assert blob_exists?(uuid, digest)
  end

  test "overlapping GC triggers serialize or coalesce without overload", %{uuid: uuid} do
    gate = make_ref()
    parent = self()

    Application.put_env(:elixir_db, :attachment_gc_hook, fn
      {:before_delete, _} ->
        send(parent, {:gc_entered, gate})

        receive do
          {:release, ^gate} -> :ok
        after
          5_000 -> raise "GC was not released"
        end

      _ ->
        :ok
    end)

    # Pending-only blob so GC has delete work and hits the hook.
    drop = upload!(uuid, "overlap-#{System.unique_integer([:positive])}")
    expire_pending!(uuid, drop)

    first =
      Task.async(fn ->
        Attachments.gc(uuid)
      end)

    assert_receive {:gc_entered, ^gate}, 2_000

    second =
      Task.async(fn ->
        Attachments.gc(uuid)
      end)

    third =
      Task.async(fn ->
        Attachments.gc(uuid)
      end)

    # Let the second caller enqueue behind the barrier before releasing.
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_queued: true} -> true
          _ -> false
        end
      end,
      timeout: 2_000,
      message: "overlapping GC was not queued"
    )

    send(first.pid, {:release, gate})

    assert {:ok, _} = Task.await(first, 5_000)
    assert {:ok, second_result} = Task.await(second, 5_000)
    assert {:ok, third_result} = Task.await(third, 5_000)

    assert match?(%{deleted: _}, second_result) or match?(%{coalesced: true}, second_result)
    assert match?(%{deleted: _}, third_result) or match?(%{coalesced: true}, third_result)
    refute blob_exists?(uuid, drop)
  end

  test "killed GC process releases exclusive barrier", %{uuid: uuid} do
    gate = make_ref()
    parent = self()

    Application.put_env(:elixir_db, :attachment_gc_hook, fn
      {:before_delete, _} ->
        send(parent, {:gc_entered, gate})

        receive do
          {:never, ^gate} -> :ok
        after
          60_000 -> :ok
        end

      _ ->
        :ok
    end)

    drop = upload!(uuid, "kill-gc-#{System.unique_integer([:positive])}")
    expire_pending!(uuid, drop)

    {:ok, gc_pid} =
      Task.start(fn ->
        _ = Attachments.gc(uuid)
      end)

    assert_receive {:gc_entered, ^gate}, 2_000

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_barrier: true, gc_active: true} -> true
                 _ -> false
               end
             end,
             timeout: 1_000,
             message: "GC barrier was not active during physical delete"
           )

    true = Process.exit(gc_pid, :kill)

    await_gc_idle(uuid)

    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, token)

    Application.delete_env(:elixir_db, :attachment_gc_hook)
    assert {:ok, _} = Attachments.gc(uuid)
    refute blob_exists?(uuid, drop)
  end

  test "last retained reference removed then GC deletes the blob", %{uuid: uuid} do
    enable_frontier(uuid)
    payload = "last-ref-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)

    assert {:ok, put1} =
             Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{"n" => 1},
               "attachments" => %{
                 "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "doc",
               "if_revision" => put1.revision,
               "body" => %{"n" => 2},
               "attachments" => %{}
             })

    assert blob_exists?(uuid, digest)
    assert {:ok, _} = DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert Eventual.eventually(
             fn -> not blob_exists?(uuid, digest) end,
             timeout: 5_000,
             message: "GC after compact did not delete last-ref blob"
           )
  end

  test "expired pending upload with no revision is reclaimed", %{uuid: uuid} do
    payload = "expired-pending-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)
    assert blob_exists?(uuid, digest)

    expire_pending!(uuid, digest)

    assert {:ok, %{deleted: 1, deleted_digests: [^digest], expired_pending_removed: 1}} =
             Attachments.gc(uuid)

    refute blob_exists?(uuid, digest)
  end

  test "unexpired pending upload survives GC", %{uuid: uuid} do
    payload = "live-pending-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)

    assert {:ok, %{deleted: 0}} = Attachments.gc(uuid)
    assert blob_exists?(uuid, digest)
  end

  test "GC waits for read guard then reader completes", %{uuid: uuid} do
    payload = "guard-wait-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{},
               "attachments" => %{
                 "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    parent = self()
    gate = make_ref()

    reader =
      Task.async(fn ->
        assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid, self())
        send(parent, {:held, gate, token})

        receive do
          {:release, ^gate} -> :ok
        after
          10_000 -> flunk("reader was not released")
        end

        assert :ok = AttachmentCoordinator.release(uuid, token)
        :released
      end)

    assert_receive {:held, ^gate, _token}, 1_000

    gc_task =
      Task.async(fn ->
        result = Attachments.gc(uuid)
        send(parent, {:gc_done, result})
        result
      end)

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_barrier: true, gc_active: false} -> true
                 _ -> false
               end
             end,
             timeout: 1_000,
             message: "gc barrier did not wait for read guard"
           )

    refute_receive {:gc_done, _}, 200

    send(reader.pid, {:release, gate})
    assert :released = Task.await(reader)
    assert {:ok, _} = Task.await(gc_task)
    assert blob_exists?(uuid, digest)
  end

  test "new attachment references are rejected while GC barrier is held", %{uuid: uuid} do
    payload = "race-ref-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)
    parent = self()
    delete_gate = make_ref()

    Application.put_env(:elixir_db, :attachment_gc_hook, fn
      {:before_delete, ^digest} ->
        send(parent, {:delete_blocked, delete_gate})

        receive do
          {:release, ^delete_gate} -> :ok
        after
          10_000 -> flunk("delete hook was not released")
        end

      _ ->
        :ok
    end)

    # Orphan the blob so GC will attempt physical delete.
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :remove_pending_blob_protection, %{digest: digest}}
             )

    gc_task = Task.async(fn -> Attachments.gc(uuid) end)
    assert_receive {:delete_blocked, ^delete_gate}, 2_000

    assert {:error, %ElixirDB.Error{code: :attachment_overloaded}} =
             Attachments.resolve_manifest_for_mutation(uuid, %{
               "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
             })

    send(gc_task.pid, {:release, delete_gate})
    assert {:ok, %{deleted: 1}} = Task.await(gc_task)
  end

  test "attachment-free put proceeds while physical deletion is blocked", %{uuid: uuid} do
    payload = "blocked-delete-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)
    parent = self()
    delete_gate = make_ref()

    Application.put_env(:elixir_db, :attachment_gc_hook, fn
      {:before_delete, ^digest} ->
        send(parent, {:delete_blocked, delete_gate})

        receive do
          {:release, ^delete_gate} -> :ok
        after
          10_000 -> flunk("delete hook was not released")
        end

      _ ->
        :ok
    end)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :remove_pending_blob_protection, %{digest: digest}}
             )

    gc_task = Task.async(fn -> Attachments.gc(uuid) end)
    assert_receive {:delete_blocked, ^delete_gate}, 2_000

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put,
                %{
                  document_id: "during-gc",
                  if_revision: nil,
                  body: %{"ok" => true},
                  attachments: %{}
                }}
             )

    send(gc_task.pid, {:release, delete_gate})
    assert {:ok, %{deleted: 1}} = Task.await(gc_task)
  end

  test "compact retention schedules GC that reclaims unreachable blobs", %{uuid: uuid} do
    enable_frontier(uuid)
    payload = "compact-seam-#{System.unique_integer([:positive])}"
    digest = upload!(uuid, payload)

    assert {:ok, put1} =
             Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{"n" => 1},
               "attachments" => %{
                 "a.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
               }
             })

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "doc",
               "if_revision" => put1.revision,
               "body" => %{"n" => 2},
               "attachments" => %{}
             })

    assert {:ok, _} = DatabaseCatalog.command(uuid, {:command, :compact_retention, %{}})

    assert Eventual.eventually(
             fn -> not blob_exists?(uuid, digest) end,
             timeout: 5_000,
             message: "post-compact GC did not delete unreachable blob"
           )
  end

  test "crash mid-GC may leave garbage but never dangling retained refs", %{uuid: uuid} do
    keep_payload = "keep-#{System.unique_integer([:positive])}"
    drop_payload = "drop-#{System.unique_integer([:positive])}"
    keep = upload!(uuid, keep_payload)
    drop = upload!(uuid, drop_payload)

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "kept",
               "body" => %{},
               "attachments" => %{
                 "keep.bin" => %{"blob" => keep, "content_type" => "application/octet-stream"}
               }
             })

    # Drop digest is pending-only; expire it so GC targets it.
    expire_pending!(uuid, drop)

    Application.put_env(:elixir_db, :attachment_gc_hook, fn
      {:before_delete, ^drop} ->
        raise "simulated mid-gc crash"

      _ ->
        :ok
    end)

    assert_raise RuntimeError, "simulated mid-gc crash", fn ->
      Attachments.gc(uuid)
    end

    assert blob_exists?(uuid, keep)

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{
               "id" => "kept",
               "revision" => nil,
               "name" => "keep.bin"
             })

    assert Enum.into(stream.body, <<>>) == keep_payload

    # Barrier must not stick after the crashed GC caller.
    assert {:ok, token} = AttachmentCoordinator.acquire_read(uuid)
    assert :ok = AttachmentCoordinator.release(uuid, token)

    Application.delete_env(:elixir_db, :attachment_gc_hook)
    assert {:ok, _} = Attachments.gc(uuid)
    assert blob_exists?(uuid, keep)
    refute blob_exists?(uuid, drop)
  end

  defp enable_frontier(uuid) do
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
  end

  defp upload!(uuid, payload) when is_binary(payload) do
    assert {:ok, %{blob: digest}} = Attachments.upload_stream(uuid, [payload])
    digest
  end

  defp blob_exists?(uuid, digest) do
    {:ok, root} = DatabaseCatalog.bundle_root(uuid)
    FilesystemStore.exists?(root, digest)
  end

  defp await_gc_idle(uuid) do
    Eventual.eventually(
      fn ->
        case AttachmentCoordinator.status(uuid) do
          %{gc_barrier: false, gc_active: false, gc_queued: false, gc_scheduled: false} -> true
          _ -> false
        end
      end,
      timeout: 5_000,
      message: "attachment GC did not become idle"
    )
  end

  defp expire_pending!(uuid, digest) do
    {:ok, root} = DatabaseCatalog.bundle_root(uuid)
    {:ok, bundle} = DatabaseBundle.open(root)
    path = DatabaseBundle.sqlite_path(bundle)
    past = DateTime.utc_now() |> DateTime.add(-3_600, :second) |> DateTime.to_iso8601()

    {:ok, conn} = Exqlite.Sqlite3.open(path)

    try do
      {:ok, stmt} =
        Exqlite.Sqlite3.prepare(
          conn,
          "UPDATE pending_blobs SET expires_at = ? WHERE blob_digest = ?"
        )

      :ok = Exqlite.Sqlite3.bind(stmt, [past, digest])
      :done = Exqlite.Sqlite3.step(conn, stmt)
      :ok = Exqlite.Sqlite3.release(conn, stmt)
    after
      Exqlite.Sqlite3.close(conn)
    end
  end

  defp restore_gc_hook(nil), do: Application.delete_env(:elixir_db, :attachment_gc_hook)
  defp restore_gc_hook(hook), do: Application.put_env(:elixir_db, :attachment_gc_hook, hook)
end
