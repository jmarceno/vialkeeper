defmodule ElixirDB.Shadow.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.Error
  alias ElixirDB.Shadow.Protocol
  alias ElixirDB.TestSupport.GarbageGenerators

  @allowed_fields ~w(source_uuid shadow_uuid generation operation_id)
  @uuid "550e8400-e29b-41d4-a716-446655440000"

  test "capability mismatch fails closed" do
    response = Protocol.response(ElixirDB.UUID.v4())
    assert :ok = Protocol.ensure_compatible(response)

    assert {:error, %{code: :shadow_incompatible}} =
             Protocol.ensure_compatible(Map.put(response, "protocol_major", 2))
  end

  test "compatible? rejects non-maps and incomplete capability responses" do
    for value <- [nil, [], "response"] do
      refute Protocol.compatible?(value)
    end

    response = Protocol.response(ElixirDB.UUID.v4())

    refute response |> Map.delete("capabilities") |> Protocol.compatible?()
    refute response |> Map.put("capabilities", "all") |> Protocol.compatible?()

    refute response
           |> Map.update!("capabilities", &List.delete(&1, List.first(&1)))
           |> Protocol.compatible?()
  end

  test "compatible? accepts capability supersets and atom keys" do
    response = Protocol.response(ElixirDB.UUID.v4())

    assert response
           |> Map.update!("capabilities", &["future_capability_v1" | &1])
           |> Protocol.compatible?()

    atom_keyed = %{
      node_id: response["node_id"],
      protocol_major: response["protocol_major"],
      capabilities: response["capabilities"]
    }

    assert Protocol.compatible?(atom_keyed)
  end

  test "compatible? rejects a string protocol major" do
    response =
      Protocol.response(ElixirDB.UUID.v4())
      |> Map.put("protocol_major", "1")

    refute Protocol.compatible?(response)
  end

  test "generation requests reject non-map input" do
    for value <- [nil, [], "request", 1] do
      assert {:error, %Error{code: :invalid_request}} =
               Protocol.generation_request(value, @allowed_fields)
    end
  end

  test "generation requests reject invalid UUID fields and name the field" do
    for field <- ~w(source_uuid shadow_uuid operation_id),
        invalid <- [nil, 42, "not-a-uuid", wrong_uuid_version(), wrong_uuid_variant()] do
      assert {:error, %Error{code: :invalid_request, message: message}} =
               valid_generation_request()
               |> Map.put(field, invalid)
               |> Protocol.generation_request(@allowed_fields)

      assert message =~ field
    end
  end

  test "generation requests accept uppercase UUIDs and normalize them to lowercase" do
    uppercase_uuid = String.upcase(@uuid)

    request =
      valid_generation_request()
      |> Map.put("source_uuid", uppercase_uuid)
      |> Map.put("shadow_uuid", uppercase_uuid)
      |> Map.put("operation_id", uppercase_uuid)

    assert {:ok, normalized} = Protocol.generation_request(request, @allowed_fields)
    assert normalized["source_uuid"] == @uuid
    assert normalized["shadow_uuid"] == @uuid
    assert normalized["operation_id"] == @uuid
  end

  test "generation requests reject invalid generations" do
    for generation <- [-1, "1", 1.5, Protocol.max_generation() + 1] do
      assert {:error, %Error{code: :invalid_request}} =
               valid_generation_request()
               |> Map.put("generation", generation)
               |> Protocol.generation_request(@allowed_fields)
    end
  end

  test "generation requests reject unsupported map key types without raising" do
    request = Map.put(valid_generation_request(), {:unsupported, 1}, true)

    assert {:error, %Error{code: :invalid_request}} =
             Protocol.generation_request(request, @allowed_fields)
  end

  test "generation requests reject atom and string key collisions" do
    request = Map.put(valid_generation_request(), :source_uuid, @uuid)

    assert {:error, %Error{code: :invalid_request}} =
             Protocol.generation_request(request, @allowed_fields)
  end

  test "generation requests reject struct maps without raising" do
    assert {:error, %Error{code: :invalid_request}} =
             Protocol.generation_request(MapSet.new([1]), @allowed_fields)

    refute Protocol.compatible?(MapSet.new([1]))
  end

  test "generation requests reject unknown fields" do
    request = Map.put(valid_generation_request(), "unexpected", true)

    assert {:error, %{code: :invalid_request}} =
             Protocol.generation_request(request, @allowed_fields)
  end

  @tag :slow
  property "generation requests return typed results for arbitrary maps" do
    check all(junk <- GarbageGenerators.junk_map(), max_runs: 400) do
      result = Protocol.generation_request(junk, @allowed_fields)

      assert match?({:ok, %{}}, result) or match?({:error, %Error{}}, result)
    end
  end

  defp valid_generation_request do
    %{
      "source_uuid" => @uuid,
      "shadow_uuid" => @uuid,
      "generation" => 1,
      "operation_id" => @uuid
    }
  end

  defp wrong_uuid_version, do: replace_byte(@uuid, 14, "0")
  defp wrong_uuid_variant, do: replace_byte(@uuid, 19, "7")

  defp replace_byte(value, offset, replacement) do
    <<prefix::binary-size(^offset), _byte, suffix::binary>> = value
    prefix <> replacement <> suffix
  end
end
