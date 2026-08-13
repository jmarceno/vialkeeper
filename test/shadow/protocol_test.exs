defmodule ElixirDB.Shadow.ProtocolTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Shadow.Protocol

  test "capability mismatch fails closed" do
    response = Protocol.response(ElixirDB.UUID.v4())
    assert :ok = Protocol.ensure_compatible(response)

    assert {:error, %{code: :shadow_incompatible}} =
             Protocol.ensure_compatible(Map.put(response, "protocol_major", 2))
  end

  test "generation requests reject unknown fields" do
    request = %{
      "source_uuid" => ElixirDB.UUID.v4(),
      "shadow_uuid" => ElixirDB.UUID.v4(),
      "generation" => 1,
      "operation_id" => ElixirDB.UUID.v4(),
      "unexpected" => true
    }

    assert {:error, %{code: :invalid_request}} =
             Protocol.generation_request(request, Map.keys(request) -- ["unexpected"])
  end
end
