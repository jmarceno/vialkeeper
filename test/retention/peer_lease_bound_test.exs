defmodule VialKeeper.Retention.PeerLeaseBoundTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Domain.PeerPosition
  alias VialKeeper.Retention.Service

  @now_ms 1_800_000_000_000
  @day_ms 86_400_000
  @skew_ms 300_000

  test "a far-future lease is clamped to the configured peer expiry" do
    peer = peer("9999-12-31T23:59:59Z")
    config = %{"retention" => %{"peer_expiry_ms" => @day_ms}}

    bounded = Service.bound_peer_lease(peer, config, @now_ms)

    assert lease_ms(bounded) == @now_ms + @day_ms + @skew_ms
  end

  test "a lease within the ceiling is kept" do
    lease = iso(@now_ms + 3_600_000)
    config = %{"retention" => %{"peer_expiry_ms" => @day_ms}}

    assert Service.bound_peer_lease(peer(lease), config, @now_ms).lease_expires_at == lease
  end

  test "a missing retention policy falls back to the default ceiling" do
    bounded = Service.bound_peer_lease(peer("9999-12-31T23:59:59Z"), %{}, @now_ms)

    assert lease_ms(bounded) == @now_ms + @day_ms + @skew_ms
  end

  defp peer(lease_expires_at) do
    %PeerPosition{
      peer_database_uuid: "peer",
      peer_history_epoch: "peer-epoch",
      source_database_uuid: "source",
      source_history_epoch: "source-epoch",
      safe_source_sequence: 0,
      installed_source_compaction_epoch: 0,
      last_seen_at: iso(@now_ms),
      lease_expires_at: lease_expires_at,
      status: :active
    }
  end

  defp iso(ms), do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  defp lease_ms(%PeerPosition{lease_expires_at: lease}) do
    {:ok, datetime, 0} = DateTime.from_iso8601(lease)
    DateTime.to_unix(datetime, :millisecond)
  end
end
