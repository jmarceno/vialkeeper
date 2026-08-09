defmodule ElixirDB.ConfigTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Config

  test "wave 1 replication defaults are present" do
    replication = Config.defaults()["replication"]

    assert replication["max_concurrent_chain_fetches"] == 4
    assert replication["max_concurrent_blob_transfers"] == 4
    assert replication["max_transfer_bytes_in_flight"] == 1_073_741_824
  end

  test "database chain fetch concurrency cannot exceed the host ceiling" do
    assert_resource_limit(%{
      "replication" => %{"max_concurrent_chain_fetches" => 33}
    })
  end

  test "database blob transfer concurrency cannot exceed the host ceiling" do
    assert_resource_limit(%{
      "replication" => %{"max_concurrent_blob_transfers" => 33}
    })
  end

  test "database transfer bytes cannot exceed the host ceiling" do
    assert_resource_limit(%{
      "replication" => %{"max_transfer_bytes_in_flight" => 4_294_967_297}
    })
  end

  test "transfer bytes must be at least attachment max bytes" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Config.merge_and_bound(%{
               "attachments" => %{"max_attachment_bytes" => 2_000_000_000},
               "replication" => %{"max_transfer_bytes_in_flight" => 1_073_741_824}
             })
  end

  test "raising attachment max requires matching transfer budget" do
    assert {:ok, config} =
             Config.merge_and_bound(%{
               "attachments" => %{"max_attachment_bytes" => 2_000_000_000},
               "replication" => %{"max_transfer_bytes_in_flight" => 2_000_000_000}
             })

    assert config["attachments"]["max_attachment_bytes"] == 2_000_000_000
    assert config["replication"]["max_transfer_bytes_in_flight"] == 2_000_000_000
  end

  defp assert_resource_limit(config) do
    assert {:error, %ElixirDB.Error{code: :resource_limit}} = Config.merge_and_bound(config)
  end
end
