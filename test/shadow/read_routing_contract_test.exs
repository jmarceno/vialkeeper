defmodule ElixirDB.Shadow.ReadRoutingContractTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Shadow.{ReadRouter, RouteTable}
  alias ElixirDB.Storage.Results

  defmodule ProbeEndpoint do
    defstruct [:mode, :counter]

    def read_document(%__MODULE__{mode: :ok}, _request, _timeout, _opts) do
      {:ok,
       %{
         "document" => %{
           "id" => "doc",
           "revision" => "rev",
           "deleted" => false,
           "body" => %{"served" => "shadow"},
           "sequence" => 9,
           "attachments" => %{}
         },
         "source_watermark" => 9
       }}
    end

    def read_document(%__MODULE__{mode: :error}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.database_unavailable("probe unavailable")}

    def read_document(%__MODULE__{mode: :miss}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.document_not_found("document not found")}

    def bulk_read_documents(%__MODULE__{mode: :ok}, _request, _timeout, _opts) do
      {:ok,
       %{
         "results" => [
           %{
             "ok" => %{
               "id" => "doc",
               "revision" => "rev",
               "deleted" => false,
               "body" => %{"served" => "shadow"},
               "sequence" => 9,
               "attachments" => %{}
             }
           }
         ],
         "source_watermark" => 9
       }}
    end

    def bulk_read_documents(%__MODULE__{mode: :miss}, _request, _timeout, _opts) do
      {:ok,
       %{
         "results" => [
           %{"error" => %{"code" => "document_not_found", "message" => "document not found"}}
         ],
         "source_watermark" => 9
       }}
    end

    def bulk_read_documents(%__MODULE__{mode: :error}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.database_unavailable("probe unavailable")}

    def open_attachment_representation(_endpoint, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.shadow_attachment_unavailable()}
  end

  setup do
    source_uuid = ElixirDB.UUID.v4()
    path = "shadow-route-#{System.unique_integer([:positive])}.elixirdb"
    assert {:ok, _} = DatabaseCatalog.create(path, %{database_uuid: source_uuid})
    assert {:ok, _} = DatabaseCatalog.open(source_uuid)

    on_exit(fn ->
      RouteTable.delete(source_uuid)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    {:ok, source_uuid: source_uuid}
  end

  test "eventual point reads use the ready route and preserve watermark", %{
    source_uuid: source_uuid
  } do
    endpoint = %ProbeEndpoint{mode: :ok}
    put_route(source_uuid, endpoint)

    assert {:ok, %Results.GetDocument{body: %{"served" => "shadow"}}, meta} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               read_consistency: :eventual,
               primary: fn _ -> flunk("eventual read should use the shadow") end
             )

    assert meta == %{served_by: "shadow", source_watermark: 9}
  end

  test "omitting consistency uses the eventual route", %{source_uuid: source_uuid} do
    put_route(source_uuid, %ProbeEndpoint{mode: :ok})

    assert {:ok, %Results.GetDocument{}, %{served_by: "shadow", source_watermark: 9}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               primary: fn _ -> flunk("omitted consistency should use the shadow") end
             )
  end

  test "any bulk shadow failure falls back as one primary batch and retires the exact route", %{
    source_uuid: source_uuid
  } do
    put_route(source_uuid, %ProbeEndpoint{mode: :error})
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    assert {:ok, [%{ok: %Results.GetDocument{body: %{}}}], %{served_by: "source"}} =
             ReadRouter.bulk_get(source_uuid, [%{id: "doc"}],
               read_consistency: :eventual,
               primary: fn requests ->
                 Agent.update(counter, &(&1 + 1))
                 assert requests == [%{id: "doc"}]
                 {:ok, [%{ok: Results.get_document(%{id: "doc", revision: "rev", body: %{}})}]}
               end
             )

    assert Agent.get(counter, & &1) == 1
    assert :not_found = RouteTable.get(source_uuid)
  end

  test "primary consistency never consults the route", %{source_uuid: source_uuid} do
    put_route(source_uuid, %ProbeEndpoint{mode: :ok})

    assert {:ok, %Results.GetDocument{body: %{served: :primary}}, %{served_by: "source"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               read_consistency: :primary,
               primary: fn _ ->
                 {:ok,
                  Results.get_document(%{id: "doc", revision: "rev", body: %{served: :primary}})}
               end
             )
  end

  test "a document miss falls back to source and keeps the ready route", %{
    source_uuid: source_uuid
  } do
    put_route(source_uuid, %ProbeEndpoint{mode: :miss})

    assert {:ok, %Results.GetDocument{body: %{"served" => "source"}}, %{served_by: "source"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               read_consistency: :eventual,
               primary: fn _ ->
                 {:ok,
                  Results.get_document(%{
                    id: "doc",
                    revision: "rev",
                    body: %{"served" => "source"}
                  })}
               end
             )

    assert {:ok, %{endpoint: %ProbeEndpoint{mode: :miss}}} = RouteTable.get(source_uuid)
  end

  test "a bulk miss falls back the original batch and keeps the ready route", %{
    source_uuid: source_uuid
  } do
    put_route(source_uuid, %ProbeEndpoint{mode: :miss})

    assert {:ok, [%{ok: %Results.GetDocument{}}], %{served_by: "source"}} =
             ReadRouter.bulk_get(source_uuid, [%{id: "doc"}],
               read_consistency: :eventual,
               primary: fn requests ->
                 assert requests == [%{id: "doc"}]
                 {:ok, [%{ok: Results.get_document(%{id: "doc", revision: "rev", body: %{}})}]}
               end
             )

    assert {:ok, %{endpoint: %ProbeEndpoint{mode: :miss}}} = RouteTable.get(source_uuid)
  end

  test "a closed source is never served from a ready shadow route", %{source_uuid: source_uuid} do
    put_route(source_uuid, %ProbeEndpoint{mode: :ok})
    assert :ok = DatabaseCatalog.close(source_uuid)

    assert {:ok, %Results.GetDocument{body: %{served: :source}}, %{served_by: "source"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               read_consistency: :eventual,
               primary: fn _ ->
                 {:ok,
                  Results.get_document(%{id: "doc", revision: "rev", body: %{served: :source}})}
               end
             )
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
