defmodule VialKeeper.Attachments.BatchUploadTest do
  @moduledoc "Correctness and lifecycle coverage for bounded attachment batch uploads."

  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Attachments
  alias VialKeeper.Attachments.FilesystemStore
  alias VialKeeper.Eventual
  alias VialKeeper.Runtime.{AttachmentCoordinator, DatabaseCatalog}

  setup do
    relative = "attachment-batch-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  test "publishes the measured bounded-concurrency default" do
    assert Attachments.default_batch_concurrency() == 16
  end

  test "streams in bounded parallelism, preserves order, deduplicates, and batch-protects", %{
    uuid: uuid
  } do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{"attachments" => %{"max_concurrent_attachment_writes" => 2}}}
             )

    payload_a = :crypto.strong_rand_bytes(128 * 1024)
    payload_b = :crypto.strong_rand_bytes(128 * 1024)

    assert {:ok, results} =
             Attachments.upload_batch(
               uuid,
               [
                 %{key: :first, source: chunks(payload_a, 17_000)},
                 {:second, chunks(payload_b, 31_000)},
                 %{key: :duplicate, source: chunks(payload_a, 8_000)}
               ]
             )

    assert Enum.map(results, & &1.key) == [:first, :second, :duplicate]

    assert Enum.map(results, & &1.length) ==
             Enum.map([payload_a, payload_b, payload_a], &byte_size/1)

    assert Enum.at(results, 0).blob == Enum.at(results, 2).blob
    assert Enum.all?(results, &is_binary(&1.expires_at))
    size_a = byte_size(payload_a)
    size_b = byte_size(payload_b)

    assert {:ok, %{logical_size: ^size_a}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :resolve_blob_metadata, %{digest: Enum.at(results, 0).blob}}
             )

    assert {:ok, %{logical_size: ^size_b}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :resolve_blob_metadata, %{digest: Enum.at(results, 1).blob}}
             )
  end

  test "reference guard keeps GC behind physical writes and metadata protection", %{uuid: uuid} do
    parent = self()
    gate = make_ref()
    payload = :crypto.strong_rand_bytes(64 * 1024)

    source = fn ->
      send(parent, {:batch_source_started, self(), gate})

      receive do
        {:release, ^gate} -> {:ok, payload, fn -> :done end}
      after
        10_000 -> {:error, :source_timeout}
      end
    end

    upload =
      Task.async(fn -> Attachments.upload_batch(uuid, [%{key: :guarded, source: source}]) end)

    assert_receive {:batch_source_started, worker, ^gate}, 2_000
    gc = Task.async(fn -> Attachments.gc(uuid) end)

    assert Eventual.eventually(
             fn ->
               case AttachmentCoordinator.status(uuid) do
                 %{gc_barrier: true, gc_active: false, active_references: 1} -> true
                 _ -> false
               end
             end,
             timeout: 2_000,
             message: "GC did not wait behind the batch reference guard"
           )

    send(worker, {:release, gate})

    assert {:ok, [%{blob: digest, key: :guarded}]} = Task.await(upload, 10_000)
    assert {:ok, %{deleted: 0}} = Task.await(gc, 10_000)
    assert blob_exists?(uuid, digest)
  end

  test "partial physical failure leaves earlier blob unprotected and reclaimable", %{uuid: uuid} do
    payload = :crypto.strong_rand_bytes(64 * 1024)
    digest = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    failed_source = fn -> {:error, :fixture_failed} end

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Attachments.upload_batch(
               uuid,
               [{:written, [payload]}, {:failed, failed_source}],
               max_concurrency: 1
             )

    assert blob_exists?(uuid, digest)

    assert {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} =
             DatabaseCatalog.command(
               uuid,
               {:command, :resolve_blob_metadata, %{digest: digest}}
             )

    assert {:ok, %{deleted: 1}} = Attachments.gc(uuid)
    refute blob_exists?(uuid, digest)
  end

  test "rejects unbounded or unsupported batch input", %{uuid: uuid} do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Attachments.upload_batch(uuid, [])

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Attachments.upload_batch(uuid, [["x"]], max_concurrency: 1_000_000)

    conn = Plug.Test.conn(:post, "/", "payload")

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Attachments.upload_batch(uuid, [%{source: conn}])
  end

  defp chunks(payload, size) do
    payload
    |> byte_size()
    |> then(fn total ->
      0..(total - 1)//size
      |> Enum.map(fn offset -> binary_part(payload, offset, min(size, total - offset)) end)
    end)
  end

  defp blob_exists?(uuid, digest) do
    {:ok, root} = DatabaseCatalog.bundle_root(uuid)
    FilesystemStore.exists?(root, digest)
  end
end
