defmodule VialKeeper.Storage.OpaqueHandleTest do
  @moduledoc "Covers the cost model of opaque backend handles on the storage hot path."
  # Suspends the host-wide handle server, so this module does not run async.
  use ExUnit.Case, async: false

  alias VialKeeper.Storage.OpaqueHandle.Server
  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.Adapter
  alias VialKeeper.Storage.Transaction

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)

    {:ok, _} =
      Adapter.apply_local_mutation(adapter, %{
        operation: :put,
        document_id: "doc",
        body: %{"n" => 1}
      })

    %{context: Adapter.to_context(adapter)}
  end

  test "storage reads and transactions do not call the handle server", %{context: context} do
    server = Process.whereis(Server)
    :ok = :sys.suspend(server)

    try do
      task =
        Task.async(fn ->
          {Services.get_document(context, %{document_id: "doc"}),
           Services.get_document(context, %{document_id: "doc", include_conflicts: true}),
           Transaction.run(context, fn tx -> Services.identity(tx) end)}
        end)

      assert {{:ok, %{body: %{"n" => 1}}}, {:ok, %{conflicts: []}}, {:ok, %{}}} =
               Task.await(task, 5_000)
    after
      :ok = :sys.resume(server)
    end
  end
end
