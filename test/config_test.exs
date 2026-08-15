defmodule VialKeeper.ConfigTest do
  @moduledoc "Configuration validation tests."
  use ExUnit.Case, async: true

  alias VialKeeper.Config

  test "replication defaults are present" do
    replication = Config.defaults()["replication"]

    assert replication["max_concurrent_chain_fetches"] == 4
    assert replication["max_concurrent_blob_transfers"] == 4
    assert replication["max_transfer_bytes_in_flight"] == 1_073_741_824
  end

  test "subscription defaults are present" do
    subscriptions = Config.defaults()["subscriptions"]

    assert subscriptions["max_active"] == 128
    assert subscriptions["max_members"] == 500
    assert subscriptions["max_buffered_events"] == 256
    assert subscriptions["default_heartbeat_ms"] == 15_000
    assert subscriptions["max_heartbeat_ms"] == 60_000
  end

  test "subscription max_active cannot exceed the host ceiling" do
    assert_resource_limit(%{"subscriptions" => %{"max_active" => 5_000}})
  end

  test "subscription max_members cannot exceed the host ceiling" do
    assert_resource_limit(%{"subscriptions" => %{"max_members" => 20_000}})
  end

  test "subscription heartbeat order is validated" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Config.merge_and_bound(%{
               "subscriptions" => %{
                 "default_heartbeat_ms" => 90_000,
                 "max_heartbeat_ms" => 60_000
               }
             })
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
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
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

  test "view defaults are present" do
    views = Config.defaults()["views"]

    assert views["max_definitions"] == 32
    assert views["batch_changes"] == 100
    assert views["consistent_wait_ms"] == 5000
  end

  test "view max_definitions cannot exceed the host ceiling" do
    assert_resource_limit(%{"views" => %{"max_definitions" => 512}})
  end

  test "view batch_changes cannot exceed the host ceiling" do
    assert_resource_limit(%{"views" => %{"batch_changes" => 1000}})
  end

  test "view consistent_wait_ms cannot exceed the host ceiling" do
    assert_resource_limit(%{"views" => %{"consistent_wait_ms" => 60_000}})
  end

  defp assert_resource_limit(config) do
    assert {:error, %VialKeeper.Error{code: :resource_limit}} = Config.merge_and_bound(config)
  end
end
