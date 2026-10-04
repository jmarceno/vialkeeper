defmodule VialKeeper.Search.StagedRefreshTest do
  @moduledoc """
  Incremental winner updates are staged and published as a group, searches run
  outside the owner, and a generation that lost staged updates is not served.
  """

  use ExUnit.Case, async: false

  alias VialKeeper.Search
  alias VialKeeper.Search.Owner
  alias VialKeeper.Search.Supervisor, as: SearchSupervisor
  alias VialKeeper.Storage.BackendContext

  @index_id "idx_staged"
  @definition %{"index_id" => @index_id, "fields" => ["/title"]}
  @published VialKeeper.Search.Published

  setup do
    bundle_root =
      Path.join(
        System.tmp_dir!(),
        "vialkeeper-fts-staged-" <> Integer.to_string(System.unique_integer([:positive]))
      )

    uuid = "staged-refresh-#{System.unique_integer([:positive])}"
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

    assert :ok =
             Search.rebuild(context, @index_id, @definition, [
               %{id: "seed", body: %{"title" => "seeded"}}
             ])

    %{context: context, uuid: uuid}
  end

  test "staged writes are visible to the next search", %{context: context, uuid: uuid} do
    for id <- ["one", "two", "three"], do: :ok = write(context, id, "staged #{id}")

    assert :ets.member(@published, {uuid, :pending})
    assert {:ok, hits} = Search.search(context, @index_id, "staged", "all")
    assert hits |> Enum.map(& &1.id) |> Enum.sort() == ["one", "three", "two"]
    refute :ets.member(@published, {uuid, :pending})

    :ok = write(context, "two", "replaced")
    assert {:ok, [%{id: "two"}]} = Search.search(context, @index_id, "replaced", "all")
    assert {:ok, hits} = Search.search(context, @index_id, "staged", "all")
    assert hits |> Enum.map(& &1.id) |> Enum.sort() == ["one", "three"]
  end

  test "staged writes are published without a search", %{context: context, uuid: uuid} do
    :ok = write(context, "late", "timer")
    assert eventually(fn -> not :ets.member(@published, {uuid, :pending}) end)
  end

  test "searches do not wait for the owner", %{context: context, uuid: uuid} do
    pid = Owner.whereis(uuid)
    :ok = :sys.suspend(pid)

    try do
      assert {:ok, [%{id: "seed"}]} = Search.search(context, @index_id, "seeded", "all")
    after
      :ok = :sys.resume(pid)
    end
  end

  test "a generation that lost staged updates is not served after a crash",
       %{context: context, uuid: uuid} do
    :ok = write(context, "lost", "unpublished")
    pid = Owner.whereis(uuid)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    assert eventually(fn -> Owner.whereis(uuid) != pid end)

    assert {:error, %VialKeeper.Error{code: :index_not_found}} =
             Search.search(context, @index_id, "seeded", "all")
  end

  test "stopping the owner publishes staged writes", %{context: context, uuid: uuid} do
    :ok = write(context, "kept", "durable")
    :ok = SearchSupervisor.stop_owner(uuid)

    assert {:ok, [%{id: "kept"}]} = Search.search(context, @index_id, "durable", "all")
  end

  defp write(context, id, title) do
    :ok = Search.record_winner(id, %{body: %{"title" => title}, deleted: false})
    Search.flush_pending(context)
  end

  defp eventually(fun, attempts \\ 100) do
    cond do
      fun.() -> true
      attempts == 0 -> false
      true -> receive(after: (20 -> eventually(fun, attempts - 1)))
    end
  end
end
