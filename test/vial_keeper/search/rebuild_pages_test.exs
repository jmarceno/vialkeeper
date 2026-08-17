defmodule VialKeeper.Search.RebuildPagesTest do
  @moduledoc """
  Full-text rebuild pages apply catch-up batches before the generation is
  published, so documents that arrive after winner paging are searchable.
  """

  use ExUnit.Case, async: false

  alias VialKeeper.Search
  alias VialKeeper.Search.Supervisor, as: SearchSupervisor
  alias VialKeeper.Storage.BackendContext

  @definition %{"index_id" => "idx_catchup", "fields" => ["/title"]}

  test "catch-up documents are indexed before the generation is published" do
    bundle_root =
      "/mnt/other/downloads/vialkeeper/work/fts/rebuild-pages-" <>
        Integer.to_string(System.unique_integer([:positive]))

    uuid = "rebuild-pages-#{System.unique_integer([:positive])}"
    File.mkdir_p!(Path.join(bundle_root, "tmp"))

    on_exit(fn ->
      SearchSupervisor.stop_owner(uuid)
      File.rm_rf(bundle_root)
    end)

    context =
      BackendContext.new(
        backend: VialKeeper.Storage.SQLite.Adapter,
        backend_ref: :unused,
        bundle_root: bundle_root,
        identity: %{database_uuid: uuid}
      )

    page_fun = fn
      nil ->
        {:ok, {[%{id: "seed", body: %{"title" => "seeded page"}}], :done, true}}

      _cursor ->
        {:ok, {[], nil, true}}
    end

    catch_up = fn _deadline ->
      case Search.rebuild_batch(context, "idx_catchup", [
             %{id: "late", body: %{"title" => "catch up term"}}
           ]) do
        {:ok, _count} -> :ok
        {:error, _} = error -> error
      end
    end

    assert {:ok, 2} =
             Search.rebuild_pages(context, "idx_catchup", @definition, nil, page_fun, catch_up)

    assert {:ok, [%{id: "seed"}]} = Search.search(context, "idx_catchup", "page", "all")
    assert {:ok, [%{id: "late"}]} = Search.search(context, "idx_catchup", "catch", "all")
  end
end
