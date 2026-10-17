defmodule VialKeeper.Shadow.ReadRoutingContractTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Attachments.Compression
  alias VialKeeper.Replication.BlobRepresentationStream
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.{ReadRouter, RouteTable}
  alias VialKeeper.Storage.Results

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
      do: {:error, VialKeeper.Error.database_unavailable("probe unavailable")}

    def read_document(%__MODULE__{mode: :miss}, _request, _timeout, _opts),
      do: {:error, VialKeeper.Error.document_not_found("document not found")}

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
      do: {:error, VialKeeper.Error.database_unavailable("probe unavailable")}

    def open_attachment_representation(_endpoint, _request, _timeout, _opts),
      do: {:error, VialKeeper.Error.shadow_attachment_unavailable()}
  end

  defmodule AttachmentEndpoint do
    defstruct [:representation]

    def open_attachment_representation(
          %__MODULE__{representation: stream},
          _request,
          _timeout,
          _opts
        ),
        do: {:ok, %{"stream" => stream, "attachment" => %{}, "source_watermark" => 9}}
  end

  setup do
    source_uuid = VialKeeper.UUID.v4()
    path = "shadow-route-#{System.unique_integer([:positive])}.vialkeeper"
    assert {:ok, _} = DatabaseCatalog.create(path, %{database_uuid: source_uuid})
    assert {:ok, _} = DatabaseCatalog.open(source_uuid)

    on_exit(fn ->
      RouteTable.delete(source_uuid)
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      VialKeeper.TempDatabase.cleanup(Path.join(VialKeeper.Config.database_root(), path))
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
               shadow_uuid: VialKeeper.UUID.v4(),
               generation: 1,
               operation_id: VialKeeper.UUID.v4()
             })
  end

  describe "shadow-served attachment bytes are verified against the route digest" do
    test "honest zstd representations stream intact", %{source_uuid: source_uuid} do
      logical = String.duplicate("honest-bytes-", 1_000)
      put_route(source_uuid, attachment_endpoint(logical, logical))

      assert collect_attachment(source_uuid, logical) == {:ok, logical}
    end

    test "forged content under an honest digest halts the stream", %{source_uuid: source_uuid} do
      honest = String.duplicate("h", 64)
      forged = String.duplicate("f", 64)
      put_route(source_uuid, attachment_endpoint(honest, forged))

      assert {:error, %VialKeeper.Error{code: :integrity_violation}} =
               collect_attachment(source_uuid, honest)
    end

    test "decoded output beyond the logical length is cut off", %{source_uuid: source_uuid} do
      declared = "x"
      bomb = :binary.copy("x", 8 * 1024 * 1024)
      put_route(source_uuid, attachment_endpoint(declared, bomb))

      assert {:error, %VialKeeper.Error{code: :integrity_violation}, emitted} =
               collect_attachment_with_bytes(source_uuid, declared)

      assert emitted <= byte_size(declared)
    end
  end

  defp attachment_endpoint(declared_logical, actual_logical) do
    {:ok, ctx} = Compression.new_compression_context()
    :ok = Compression.set_level(ctx)
    {:ok, payload} = Compression.compress_chunks(ctx, [actual_logical], true)

    stream =
      BlobRepresentationStream.new!(%{
        encoding: :zstd,
        logical_digest: sha256(declared_logical),
        logical_length: byte_size(declared_logical),
        payload_length: byte_size(payload),
        payload_sha256: sha256(payload),
        body: [payload]
      })

    %AttachmentEndpoint{representation: stream}
  end

  defp collect_attachment(source_uuid, declared) do
    case collect_attachment_with_bytes(source_uuid, declared) do
      {:ok, body} -> {:ok, body}
      {:error, error, _emitted} -> {:error, error}
    end
  end

  defp collect_attachment_with_bytes(source_uuid, declared) do
    assert {:ok, logical, %{served_by: "shadow"}} =
             ReadRouter.open_attachment(
               source_uuid,
               %{id: "doc", name: "file", digest: sha256(declared)},
               primary: fn _ -> flunk("eventual attachment read should use the shadow") end
             )

    Enum.reduce_while(logical.body, {:ok, ""}, fn
      {:error, error}, {:ok, acc} -> {:halt, {:error, error, byte_size(acc)}}
      chunk, {:ok, acc} -> {:cont, {:ok, acc <> chunk}}
    end)
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
