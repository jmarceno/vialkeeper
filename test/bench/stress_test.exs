defmodule VialKeeper.Bench.StressTest do
  @moduledoc "Behavior-level coverage for the phase-separated SimpleWiki benchmark."

  use ExUnit.Case, async: false

  alias VialKeeper.Bench.{Marker, Registry, Root, SimpleWiki, Stress, Tmp}

  setup do
    parent = Tmp.dir("stress-approved")
    repo = Tmp.dir("stress-repo")
    root = Path.join(parent, "vialkeeper")

    env = [
      approved_parent: parent,
      repo_root: repo,
      available_bytes_fun: fn _ -> {:ok, 1_099_511_627_776} end
    ]

    {:ok, context} = Root.configure(root, env)
    {:ok, dataset} = Root.dataset_path(context, "simplewiki", "v1")
    File.mkdir_p!(dataset)
    prepare_smoke_fixture!(dataset)

    assert :ok =
             Marker.write(context, dataset, %{
               "dataset" => "simplewiki",
               "version" => "v1",
               "profile" => "smoke"
             })

    :ok = Application.stop(:vial_keeper)

    on_exit(fn ->
      {:ok, _} = Application.ensure_all_started(:vial_keeper)
      File.rm_rf(parent)
      File.rm_rf(repo)
    end)

    {:ok, context: context, env: env}
  end

  test "uses bulk seed/reference phases, precomputed queries, and preserves partial progress", %{
    context: context,
    env: env
  } do
    parent = self()

    observer = fn
      "bulk_document_ingest", progress ->
        send(parent, {:partial_written, progress})
        raise "interrupt after persisted progress"

      _phase, _progress ->
        :ok
    end

    assert_raise RuntimeError, "interrupt after persisted progress", fn ->
      Stress.run(env ++ [profile: :smoke, warmup: 0, iterations: 1, progress_observer: observer])
    end

    assert_receive {:partial_written, %{"processed" => 3, "total" => 3}}, 2_000
    report_path = Path.join(context.root, "reports/simplewiki-stress.json")
    partial = report_path |> File.read!() |> JSON.decode!()
    assert partial["status"] == "running"
    assert partial["current_phase"] == "bulk_document_ingest"
    assert partial["completed_phases"] == ["single_document_ingest"]
    assert partial["progress"]["processed"] == 3

    assert {:ok, ^report_path} =
             Stress.run(env ++ [profile: :smoke, warmup: 0, iterations: 1, max_concurrency: 1])

    report = report_path |> File.read!() |> JSON.decode!()
    assert report["status"] == "complete"

    assert report["completed_phases"] == [
             "single_document_ingest",
             "bulk_document_ingest",
             "attachment_physical_ingest",
             "attachment_reference_mutation",
             "fts_build",
             "fts_search",
             "attachment_read",
             "mixed"
           ]

    assert report["query_algorithm"] == "simplewiki-query-v2"
    assert report["results"]["single_document_ingest"]["documents"] == 3
    assert report["results"]["bulk_document_ingest"]["documents"] == 3
    assert report["results"]["bulk_document_ingest"]["transaction_count"] == 1
    assert report["results"]["attachment_physical_ingest"]["files"] == 1
    assert report["results"]["attachment_physical_ingest"]["metadata_transaction_count"] == 1
    assert report["results"]["attachment_reference_mutation"]["documents"] == 1
    assert report["results"]["attachment_reference_mutation"]["transaction_count"] == 1
  end

  defp prepare_smoke_fixture!(dataset) do
    source = Tmp.dir("stress-source")
    xml = Path.join(source, "pages.xml")
    archive = Path.join(source, "pages.xml.bz2")

    File.write!(xml, """
    <mediawiki>
      <page><title>One</title><ns>0</ns><id>1</id><revision><text>alpha beta common words</text></revision></page>
      <page><title>Two</title><ns>0</ns><id>2</id><revision><text>beta gamma common words</text></revision></page>
      <page><title>Three</title><ns>0</ns><id>3</id><revision><text>gamma delta common words</text></revision></page>
    </mediawiki>
    """)

    {compressed, 0} = System.cmd("bzip2", ["-c", xml])
    File.write!(archive, compressed)
    spec = Registry.fetch!("simplewiki")
    assert {:ok, manifest} = SimpleWiki.generate_fixture(archive, spec, :smoke, dataset)
    File.write!(Path.join(dataset, "manifest.json"), JSON.encode!(manifest) <> "\n")
    File.rm_rf(source)
  end
end
