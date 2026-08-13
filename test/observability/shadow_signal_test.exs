defmodule ElixirDB.Observability.ShadowSignalTest do
  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Eventual
  alias ElixirDB.Shadow.{ReadRouter, RouteTable}
  alias ElixirDB.Storage.Results

  defmodule Endpoint do
    defstruct [:mode]

    def read_document(%__MODULE__{mode: :ok}, _request, _timeout, _opts),
      do:
        {:ok,
         %{
           "document" => %{
             "id" => "doc",
             "revision" => "rev",
             "deleted" => false,
             "body" => %{},
             "sequence" => 3,
             "attachments" => %{}
           },
           "source_watermark" => 3
         }}

    def read_document(%__MODULE__{mode: :error}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.database_unavailable("shadow unavailable")}
  end

  test "records served outcome and exact-route fallback without private fields" do
    source_uuid = ElixirDB.UUID.v4()
    on_exit(fn -> RouteTable.delete(source_uuid) end)

    assert :ok =
             RouteTable.put(source_uuid, %{
               endpoint: %Endpoint{mode: :ok},
               source_uuid: source_uuid,
               shadow_uuid: ElixirDB.UUID.v4(),
               generation: 1,
               operation_id: ElixirDB.UUID.v4()
             })

    assert {:ok, %Results.GetDocument{}, %{served_by: "shadow"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               primary: fn _ -> flunk("the ready shadow route should serve the read") end
             )

    assert [span] = TestExporter.spans_named("elixir_db.shadow.read")
    refute inspect(span) =~ "shadow unavailable"

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("elixir_db.shadow.read.count", %{
          :"db.uuid" => source_uuid,
          :outcome => :shadow
        }) == 1
      end,
      timeout: 2_000,
      message: "shadow read counter missing"
    )

    assert :ok =
             RouteTable.put(source_uuid, %{
               endpoint: %Endpoint{mode: :error},
               source_uuid: source_uuid,
               shadow_uuid: ElixirDB.UUID.v4(),
               generation: 2,
               operation_id: ElixirDB.UUID.v4()
             })

    assert {:ok, %Results.GetDocument{}, %{served_by: "primary"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               primary: fn _ ->
                 {:ok, Results.get_document(%{id: "doc", revision: "primary", body: %{}})}
               end
             )

    Eventual.eventually(
      fn ->
        TestMetricExporter.counter_sum("elixir_db.shadow.route.fallback.count", %{
          :"db.uuid" => source_uuid,
          :outcome => :fallback
        }) == 1
      end,
      timeout: 2_000,
      message: "shadow fallback counter missing"
    )
  end
end
