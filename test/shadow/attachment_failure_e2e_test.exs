defmodule ElixirDB.Shadow.AttachmentFailureE2ETest do
  use ExUnit.Case, async: false

  alias ElixirDB.Shadow.{ReadRouter, RouteTable}

  defmodule ProbeEndpoint do
    defstruct [:mode]

    def open_attachment_representation(%__MODULE__{mode: :miss}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.attachment_not_found("attachment is not present")}

    def open_attachment_representation(%__MODULE__{mode: :store}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.shadow_attachment_unavailable("cas missing")}
  end

  test "an attachment miss falls back and keeps the ready route" do
    {source_uuid, path} = ElixirDB.ShadowSource.open!("shadow-att")
    put_route(source_uuid, %ProbeEndpoint{mode: :miss})

    assert {:ok, %{body: body}, %{served_by: "source"}} =
             ReadRouter.open_attachment(source_uuid, %{id: "doc", name: "file.bin"},
               primary: fn _ -> {:ok, %{body: "source-bytes"}} end
             )

    assert body == "source-bytes"
    assert {:ok, _} = RouteTable.get(source_uuid)
    ElixirDB.ShadowSource.close!(source_uuid, path)
  end

  test "an attachment store failure falls back and compare-deletes the route" do
    {source_uuid, path} = ElixirDB.ShadowSource.open!("shadow-att")
    put_route(source_uuid, %ProbeEndpoint{mode: :store})

    assert {:ok, %{body: "source-bytes"}, %{served_by: "source"}} =
             ReadRouter.open_attachment(source_uuid, %{id: "doc", name: "file.bin"},
               primary: fn _ -> {:ok, %{body: "source-bytes"}} end
             )

    assert :not_found = RouteTable.get(source_uuid)
    ElixirDB.ShadowSource.close!(source_uuid, path)
  end

  defp put_route(source_uuid, endpoint) do
    assert :ok =
             RouteTable.put(source_uuid, %{
               endpoint: endpoint,
               source_uuid: source_uuid,
               shadow_uuid: ElixirDB.UUID.v4(),
               generation: 1,
               operation_id: ElixirDB.UUID.v4()
             })
  end
end
