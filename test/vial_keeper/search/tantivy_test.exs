defmodule VialKeeper.Search.TantivyTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Search.{Owner, Tantivy}

  @definition %{"index_id" => "idx_search_tantivy_test", "fields" => ["/title"]}

  setup do
    path =
      "/mnt/other/downloads/vialkeeper/work/fts/tantivy-adapter-test-" <>
        Integer.to_string(System.unique_integer([:positive]))

    File.rm_rf!(path)
    on_exit(fn -> File.rm_rf(path) end)

    assert {:ok, handle} = Tantivy.create(path, @definition)
    %{handle: handle, path: path}
  end

  test "built-in analysis supports term, any, all, phrase, and prefix queries", %{handle: handle} do
    :ok = Tantivy.add(handle, "both", %{"title" => "alpha beta"})
    :ok = Tantivy.add(handle, "one", %{"title" => "alpha only"})
    :ok = Tantivy.add(handle, "ordered", %{"title" => "alpha beta gamma"})
    :ok = Tantivy.add(handle, "reversed", %{"title" => "beta alpha"})
    {:ok, handle} = Tantivy.commit(handle)

    assert {:ok, all} = Tantivy.search(handle, "alpha beta", "all")
    assert Enum.map(all, & &1.id) |> Enum.sort() == ["both", "ordered", "reversed"]

    assert {:ok, any} = Tantivy.search(handle, "alpha beta", "any")
    assert Enum.map(any, & &1.id) |> Enum.sort() == ["both", "one", "ordered", "reversed"]

    assert {:ok, phrase} = Tantivy.search(handle, "alpha beta", "phrase")
    assert Enum.map(phrase, & &1.id) |> Enum.sort() == ["both", "ordered"]
    assert {:ok, prefix} = Tantivy.search(handle, "alpha bet", "prefix")
    assert Enum.map(prefix, & &1.id) |> Enum.sort() == ["both", "ordered", "reversed"]
  end

  test "stable IDs replace and delete documents without exposing uncommitted writes", %{
    handle: handle
  } do
    :ok = Tantivy.add(handle, "doc", %{"title" => "ancient oak"})
    {:ok, handle} = Tantivy.commit(handle)

    :ok = Tantivy.replace(handle, "doc", %{"title" => "modern pine"}, false)
    assert {:ok, [%{id: "doc"}]} = Tantivy.search(handle, "oak", "all")
    {:ok, handle} = Tantivy.commit(handle)
    assert {:ok, [%{id: "doc"}]} = Tantivy.search(handle, "pine", "all")

    :ok = Tantivy.replace(handle, "doc", %{"title" => "modern pine"}, true)
    {:ok, handle} = Tantivy.commit(handle)
    assert {:ok, []} = Tantivy.search(handle, "pine", "all")

    :ok = Tantivy.add(handle, "uncommitted", %{"title" => "not visible"})
    assert {:ok, []} = Tantivy.search(handle, "visible", "all")
    :ok = Tantivy.rollback(handle)
  end

  test "native batches remain unsearchable until one explicit commit", %{handle: handle} do
    assert :ok =
             Tantivy.add_batch(handle, [
               {"first", %{"title" => "batched alpha"}},
               {"second", %{"title" => "batched beta"}}
             ])

    assert {:error, %VialKeeper.Error{code: :index_not_found}} =
             Tantivy.search(handle, "batched", "all")

    assert {:ok, committed} = Tantivy.commit(handle)
    assert {:ok, hits} = Tantivy.search(committed, "batched", "all")
    assert Enum.map(hits, & &1.id) |> Enum.sort() == ["first", "second"]
  end

  test "owner reopens a committed generation from its manifest" do
    root =
      "/mnt/other/downloads/vialkeeper/work/fts/tantivy-owner-test-" <>
        Integer.to_string(System.unique_integer([:positive]))

    path = Path.join(root, "tmp")
    uuid = "tantivy-owner-test-#{System.unique_integer([:positive])}"
    File.rm_rf!(root)
    on_exit(fn -> File.rm_rf(root) end)

    {:ok, pid} = Owner.start_link({uuid, path})
    :ok = GenServer.call(pid, {:begin_rebuild, "idx", @definition})

    {:ok, 1} =
      GenServer.call(pid, {:rebuild_batch, "idx", [%{id: "doc", body: %{"title" => "reopen me"}}]})

    {:ok, 1} = GenServer.call(pid, {:finish_rebuild, "idx"})
    index_root = Path.join(path, "search/indexes/#{Base.url_encode64("idx", padding: false)}")
    {:ok, manifest} = File.read(Path.join(index_root, "manifest.json"))

    assert %{"backend" => "tantivy_ex", "schema_fingerprint" => fingerprint} =
             Jason.decode!(manifest)

    assert fingerprint == Tantivy.schema_fingerprint()
    :ok = GenServer.stop(pid)

    {:ok, pid} = Owner.start_link({uuid, path})
    assert {:ok, [%{id: "doc"}]} = GenServer.call(pid, {:search, "idx", "reopen", "all"})

    :ok =
      GenServer.call(
        pid,
        {:refresh_many,
         [
           {"doc", %{"title" => "first update"}, false},
           {"doc", %{"title" => "last update"}, false}
         ]}
      )

    assert {:ok, [%{id: "doc"}]} = GenServer.call(pid, {:search, "idx", "last", "all"})
    assert {:ok, []} = GenServer.call(pid, {:search, "idx", "first", "all"})

    :ok = GenServer.call(pid, {:begin_rebuild, "idx", @definition})

    {:ok, 1} =
      GenServer.call(
        pid,
        {:rebuild_batch, "idx", [%{id: "doc", body: %{"title" => "new generation"}}]}
      )

    {:ok, 1} = GenServer.call(pid, {:finish_rebuild, "idx"})

    assert ["generation-2"] =
             Path.wildcard(Path.join(index_root, "generation-*")) |> Enum.map(&Path.basename/1)

    :ok = GenServer.stop(pid)
  end

  test "old generation remains searchable until replacement publication" do
    root =
      "/mnt/other/downloads/vialkeeper/work/fts/tantivy-generation-test-" <>
        Integer.to_string(System.unique_integer([:positive]))

    path = Path.join(root, "tmp")
    uuid = "tantivy-generation-test-#{System.unique_integer([:positive])}"
    File.rm_rf!(root)
    on_exit(fn -> File.rm_rf(root) end)

    {:ok, pid} = Owner.start_link({uuid, path})
    :ok = GenServer.call(pid, {:begin_rebuild, "idx", @definition})

    {:ok, 1} =
      GenServer.call(pid, {:rebuild_batch, "idx", [%{id: "doc", body: %{"title" => "old"}}]})

    {:ok, 1} = GenServer.call(pid, {:finish_rebuild, "idx"})
    :ok = GenServer.call(pid, {:begin_rebuild, "idx", @definition})

    assert {:ok, [%{id: "doc"}]} = GenServer.call(pid, {:search, "idx", "old", "all"})

    {:ok, 1} =
      GenServer.call(pid, {:rebuild_batch, "idx", [%{id: "doc", body: %{"title" => "new"}}]})

    rebuilding = :sys.get_state(pid).rebuilds["idx"]
    assert rebuilding.handle.searcher == nil

    assert {:ok, [%{id: "doc"}]} = GenServer.call(pid, {:search, "idx", "old", "all"})
    assert {:ok, []} = GenServer.call(pid, {:search, "idx", "new", "all"})

    {:ok, 1} = GenServer.call(pid, {:finish_rebuild, "idx"})
    assert {:ok, [%{id: "doc"}]} = GenServer.call(pid, {:search, "idx", "new", "all"})
    assert {:ok, []} = GenServer.call(pid, {:search, "idx", "old", "all"})

    :ok = GenServer.stop(pid)
  end
end
