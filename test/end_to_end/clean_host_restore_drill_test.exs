defmodule VialKeeper.EndToEnd.CleanHostRestoreDrillTest do
  @moduledoc """
  MAINT-007 clean-host restore drill.

  Seeds a closed bundle on a production release daemon, copies it to a fresh
  destination root, and restores it inside a glibc container that has only the
  release artifact and destination data — no Mix, repo, or test catalog.
  """

  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :slow

  alias VialKeeper.Eventual
  alias VialKeeper.TestSupport.{ContainerEngine, ProdRelease}

  @tag timeout: 300_000
  test "closed bundle restores on a container host over HTTP /v1 only" do
    ContainerEngine.require_engine!()

    work =
      Path.join(
        System.tmp_dir!(),
        "vialkeeper-clean-restore-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(work)
    on_exit(fn -> File.rm_rf(work) end)

    release_dir = Path.join(work, "rel")
    ProdRelease.ensure_portable_for_drill!(release_dir)

    source_root = Path.join(work, "source")
    dest_root = Path.join(work, "dest")
    File.mkdir_p!(source_root)
    File.mkdir_p!(dest_root)

    bundle_rel = "maint007-#{System.unique_integer([:positive])}.vialkeeper"
    peer_rel = "maint007-peer-#{System.unique_integer([:positive])}.vialkeeper"

    source_port = ProdRelease.allocate_loopback_port!()
    dest_port = ProdRelease.allocate_loopback_port!()

    ProdRelease.write_host_toml!(source_root, source_port)
    assert :ok = ProdRelease.start_daemon!(release_dir, source_root)

    on_exit(fn -> ProdRelease.stop_daemon!(release_dir, source_root) end)

    source_base = ProdRelease.base_url(source_port)

    Eventual.eventually(
      fn ->
        case Req.get(source_base <> "/v1/databases", receive_timeout: 1_000) do
          {:ok, %{status: 200}} -> true
          _ -> false
        end
      end,
      timeout: 30_000,
      message: "source release daemon did not become ready"
    )

    assert {:ok, %{status: 201, body: created}} =
             Req.post(source_base <> "/v1/databases", json: %{"path" => bundle_rel})

    uuid = created["data"]["database_uuid"]
    assert is_binary(uuid)

    assert {:ok, %{status: 201, body: peer_created}} =
             Req.post(source_base <> "/v1/databases", json: %{"path" => peer_rel})

    peer_uuid = peer_created["data"]["database_uuid"]
    assert is_binary(peer_uuid)

    payload = "maint007-attachment-#{System.unique_integer([:positive])}"

    assert {:ok, %{status: 201, body: upload}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/attachments/upload",
               body: payload,
               headers: [{"content-type", "application/octet-stream"}]
             )

    blob = upload["data"]["blob"]
    assert is_binary(blob)

    doc_body = %{"kind" => "restore-drill", "label" => "portable"}

    assert {:ok, %{status: 201, body: put_body}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/documents/put",
               json: %{
                 "id" => "maint007-doc",
                 "body" => doc_body,
                 "attachments" => %{
                   "note.txt" => %{"blob" => blob, "content_type" => "text/plain"}
                 }
               }
             )

    revision = put_body["data"]["revision"]
    assert is_binary(revision)

    assert {:ok, %{status: 201, body: fts_body}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/indexes",
               json: %{
                 "name" => "kind-text",
                 "type" => "full_text",
                 "fields" => ["/kind"]
               }
             )

    fts_index_id = fts_body["data"]["index_id"]
    assert is_binary(fts_index_id)

    assert {:ok, %{status: status_job, body: job_body}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/replications",
               json: %{
                 "persist" => true,
                 "mode" => "one_shot",
                 "direction" => "push",
                 "enabled" => false,
                 "endpoint" => %{"kind" => "local", "database_uuid" => peer_uuid}
               }
             )

    assert status_job in [200, 201]
    job_id = job_body["data"]["job_id"]
    assert is_binary(job_id)

    assert {:ok, %{status: 200, body: integrity_before}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/integrity-check", json: %{})

    assert integrity_before["data"]["ok"] == true

    assert {:ok, %{status: 200}} =
             Req.post(source_base <> "/v1/databases/#{uuid}/close", json: %{})

    assert :ok = ProdRelease.stop_daemon!(release_dir, source_root)

    source_bundle = Path.join(source_root, bundle_rel)
    dest_bundle = Path.join(dest_root, bundle_rel)
    File.cp_r!(source_bundle, dest_bundle)

    tmp_dir = Path.join(dest_bundle, "tmp")
    File.rm_rf!(tmp_dir)
    File.mkdir_p!(tmp_dir)

    lease_path = Path.join(dest_root, bundle_rel <> ".lease")
    if File.exists?(lease_path), do: File.rm!(lease_path)

    ProdRelease.write_host_toml!(dest_root, dest_port, web_ui: false)

    container_name = "vialkeeper-restore-#{System.unique_integer([:positive])}"
    on_exit(fn -> ContainerEngine.stop(container_name) end)

    restore_started = System.monotonic_time(:millisecond)

    _container_id =
      ContainerEngine.run!(
        name: container_name,
        release_dir: release_dir,
        data_root: dest_root
      )

    dest_base = ProdRelease.base_url(dest_port)

    Eventual.eventually(
      fn ->
        case Req.get(dest_base <> "/v1/databases", receive_timeout: 1_000) do
          {:ok, %{status: 200}} -> true
          _ -> false
        end
      end,
      timeout: 60_000,
      message: "restore container daemon did not become ready"
    )

    assert {:ok, %{status: 201, body: registered}} =
             Req.post(dest_base <> "/v1/registrations", json: %{"path" => bundle_rel})

    assert registered["data"]["database_uuid"] == uuid

    assert {:ok, %{status: 200, body: integrity_after}} =
             Req.post(dest_base <> "/v1/databases/#{uuid}/integrity-check", json: %{})

    assert integrity_after["data"]["ok"] == true

    assert {:ok, %{status: 200, body: got_doc}} =
             Req.post(dest_base <> "/v1/databases/#{uuid}/documents/get",
               json: %{"id" => "maint007-doc"}
             )

    assert got_doc["data"]["revision"] == revision
    assert got_doc["data"]["body"] == doc_body

    assert {:ok, %{status: 200, headers: headers, body: downloaded}} =
             Req.post(dest_base <> "/v1/databases/#{uuid}/attachments/get",
               json: %{"id" => "maint007-doc", "revision" => nil, "name" => "note.txt"},
               decode_body: false
             )

    assert downloaded == payload
    assert header(headers, "content-type") == "text/plain"

    assert {:ok, %{status: 200, body: rebuilt}} =
             Req.post(
               dest_base <> "/v1/databases/#{uuid}/indexes/#{fts_index_id}/rebuild",
               json: %{}
             )

    assert rebuilt["data"]["rebuilt"] == true

    assert {:ok, %{status: 200, body: search_body}} =
             Req.post(dest_base <> "/v1/databases/#{uuid}/query",
               json: %{
                 "search" => %{"index" => "kind-text", "text" => "restore-drill", "mode" => "all"},
                 "limit" => 10
               }
             )

    documents = search_body["data"]["documents"] || search_body["data"]["results"]
    assert Enum.any?(documents, &(&1["id"] == "maint007-doc"))

    assert {:ok, %{status: 200, body: jobs_body}} =
             Req.get(dest_base <> "/v1/databases/#{uuid}/replications")

    jobs = jobs_body["data"]
    assert is_list(jobs)

    job = Enum.find(jobs, &(&1["job_id"] == job_id))
    assert job
    assert job["enabled"] == false
    refute job["state"] in ["running", :running, "active", :active]

    restore_ms = System.monotonic_time(:millisecond) - restore_started
    assert restore_ms > 0
    IO.puts("MAINT-007 clean-host restore_ms=#{restore_ms}")

    {runtime_out, 0} =
      ContainerEngine.eval!(
        container_name,
        "IO.inspect(VialKeeper.Diagnostics.runtime(), label: :runtime)"
      )

    assert runtime_out =~ "app_version:"
    refute runtime_out =~ ~s(app_version: "")

    ContainerEngine.stop(container_name)
  end

  defp header(headers, name) do
    Enum.find_value(headers, fn {key, value} ->
      header_value(key, value, name)
    end)
  end

  defp header_value(key, value, name) do
    if String.downcase(key) == name do
      normalize_header_value(value)
    end
  end

  defp normalize_header_value([first | _]), do: first
  defp normalize_header_value(other), do: other
end
