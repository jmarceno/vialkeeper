defmodule ElixirDB.Shadow.EndpointTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Shadow.{LocalEndpoint, Protocol, RemoteEndpoint}

  defmodule Probe do
    def capabilities(_opts), do: {:ok, Protocol.response("00000000-0000-4000-8000-000000000001")}
    def provision(request, _opts), do: {:ok, Map.put(request, "state", "bootstrapping")}
    def inspect(request, _opts), do: {:ok, Map.put(request, "state", "bootstrapping")}
    def destroy(_request, _opts), do: {:ok, %{"state" => "absent"}}
    def read_document(request, _read_opts, _opts), do: {:ok, request}
    def bulk_read_documents(request, _read_opts, _opts), do: {:ok, request}

    def open_attachment_representation(_request, _read_opts, _opts),
      do: {:error, ElixirDB.Error.shadow_attachment_unavailable()}
  end

  test "local endpoint invokes worker services directly" do
    assert {:ok, endpoint} = LocalEndpoint.new(worker: Probe, worker_options: [tag: :local])
    assert {:ok, %{"protocol_major" => 1}} = LocalEndpoint.capabilities(endpoint, 100)

    assert {:ok, %{"id" => "doc", "state" => "bootstrapping"}} =
             LocalEndpoint.provision(endpoint, %{"id" => "doc"}, 100)
  end

  test "remote endpoint requires explicit bounded control and read timeouts" do
    assert {:error, %{code: :invalid_request}} =
             RemoteEndpoint.new(%{
               base_url: "http://127.0.0.1:4000",
               auth_token: "token",
               control_timeout_ms: 10,
               read_timeout_ms: nil
             })

    assert {:ok, endpoint} =
             RemoteEndpoint.new(%{
               base_url: "http://127.0.0.1:4000",
               auth_token: "token",
               control_timeout_ms: 10,
               read_timeout_ms: 20
             })

    assert endpoint.control_timeout_ms == 10
    assert endpoint.read_timeout_ms == 20
  end
end
