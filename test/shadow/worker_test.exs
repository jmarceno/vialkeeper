defmodule VialKeeper.Shadow.WorkerTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Shadow.Worker

  test "provision is idempotent and exact-generation destroy is fenced" do
    root =
      Path.join(
        VialKeeper.Config.database_root(),
        "shadow-worker-#{System.unique_integer([:positive])}"
      )

    external_root = Path.join(root, "external")
    attachment_location = Path.join(external_root, "blobs")
    File.mkdir_p!(attachment_location)

    source_uuid = VialKeeper.UUID.v4()
    request = request(source_uuid, VialKeeper.UUID.v4(), 1, attachment_location)
    opts = [root: Path.join(root, "managed"), allowed_attachment_roots: [external_root]]

    identity_request =
      Map.take(request, ["source_uuid", "shadow_uuid", "generation", "operation_id"])

    on_exit(fn ->
      _ = Worker.destroy(identity_request, opts)
      VialKeeper.TempDatabase.cleanup(root)
    end)

    assert {:ok, %{"state" => "bootstrapping", "idempotent" => false}} =
             Worker.provision(request, opts)

    assert {:ok, %{"state" => "bootstrapping", "idempotent" => true}} =
             Worker.provision(request, opts)

    replacement = request(source_uuid, VialKeeper.UUID.v4(), 1, attachment_location)
    assert {:error, %{code: :shadow_generation_conflict}} = Worker.provision(replacement, opts)

    wrong_identity =
      request
      |> Map.take(["source_uuid", "shadow_uuid", "generation", "operation_id"])
      |> Map.put("shadow_uuid", VialKeeper.UUID.v4())

    assert {:ok, %{"state" => "absent"}} = Worker.destroy(wrong_identity, opts)
    assert {:ok, %{"state" => "absent"}} = Worker.destroy(identity_request, opts)
  end

  defp request(source_uuid, shadow_uuid, generation, attachment_location) do
    %{
      "source_uuid" => source_uuid,
      "shadow_uuid" => shadow_uuid,
      "generation" => generation,
      "operation_id" => VialKeeper.UUID.v4(),
      "attachment_store_type" => "external_cas",
      "attachment_location" => attachment_location,
      "specification_digest" => String.duplicate("b", 64)
    }
  end
end
