defmodule VialKeeper.Retention.FrontierTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Domain.{PeerPosition, RetentionState}
  alias VialKeeper.Retention.Frontier

  setup do
    now = DateTime.utc_now()
    future = now |> DateTime.add(60, :second) |> DateTime.to_iso8601()
    past = now |> DateTime.add(-60, :second) |> DateTime.to_iso8601()

    base_opts = %{
      source_database_uuid: "db-1",
      source_history_epoch: "epoch-1",
      current_sequence: 100,
      current_floor: 10,
      current_compaction_epoch: 2,
      mode: :stable_frontier,
      peers: [],
      now: now
    }

    {:ok, base_opts: base_opts, future: future, past: past}
  end

  test "empty peers yields current sequence when mode is stable_frontier", %{base_opts: base_opts} do
    result = Frontier.compute(base_opts)

    assert result.candidate_floor == 100
    assert result.floor_advanced
    assert result.active_peer_count == 0
    assert result.blocking_peer_count == 0
    refute result.noop?
  end

  test "unknown peer contributes zero via empty admitted set", %{
    base_opts: base_opts,
    future: future
  } do
    peer =
      peer_fixture(%{
        safe_source_sequence: 0,
        lease_expires_at: future,
        status: :bootstrap_required
      })

    result = Frontier.compute(Map.put(base_opts, :peers, [peer]))

    assert result.candidate_floor == 100
    assert result.bootstrap_required_count == 1
    assert result.blocking_peer_count == 0
  end

  test "expired peers are excluded from frontier minimum", %{base_opts: base_opts, past: past} do
    peer =
      peer_fixture(%{
        safe_source_sequence: 20,
        lease_expires_at: past,
        status: :active
      })

    result = Frontier.compute(Map.put(base_opts, :peers, [peer]))

    assert result.expired_peer_count == 1
    assert result.candidate_floor == 100
    assert result.blocking_peer_count == 0
  end

  test "bootstrap and quarantined peers are not admitted", %{base_opts: base_opts, future: future} do
    bootstrap =
      peer_fixture(%{
        safe_source_sequence: 15,
        lease_expires_at: future,
        status: :bootstrap_required
      })

    quarantined =
      peer_fixture(%{
        peer_database_uuid: "peer-2",
        safe_source_sequence: 15,
        lease_expires_at: future,
        status: :quarantined
      })

    result = Frontier.compute(Map.put(base_opts, :peers, [bootstrap, quarantined]))

    assert result.bootstrap_required_count == 1
    assert result.active_peer_count == 0
    assert result.candidate_floor == 100
  end

  test "safe positions cannot exceed current sequence", %{base_opts: base_opts, future: future} do
    peer =
      peer_fixture(%{
        safe_source_sequence: 500,
        lease_expires_at: future,
        status: :active
      })

    result = Frontier.compute(Map.put(base_opts, :peers, [peer]))

    assert result.candidate_floor == 100
    assert result.blocking_peer_count == 1
  end

  test "frontier is minimum across admitted peers", %{base_opts: base_opts, future: future} do
    peers = [
      peer_fixture(%{
        peer_database_uuid: "peer-1",
        safe_source_sequence: 80,
        lease_expires_at: future,
        status: :active
      }),
      peer_fixture(%{
        peer_database_uuid: "peer-2",
        safe_source_sequence: 25,
        lease_expires_at: future,
        status: :active
      })
    ]

    result = Frontier.compute(Map.put(base_opts, :peers, peers))

    assert result.candidate_floor == 25
    assert result.active_peer_count == 2
    assert result.blocking_peer_count == 2
    assert result.floor_advanced
  end

  test "floor never moves backwards", %{base_opts: base_opts, future: future} do
    peer =
      peer_fixture(%{
        safe_source_sequence: 5,
        lease_expires_at: future,
        status: :active
      })

    result =
      base_opts
      |> Map.put(:current_floor, 40)
      |> Map.put(:peers, [peer])
      |> Frontier.compute()

    assert result.candidate_floor == 40
    refute result.floor_advanced
    assert result.noop?
  end

  test "epoch mismatch increments bootstrap_required_count", %{base_opts: base_opts, future: future} do
    peer =
      peer_fixture(%{
        source_history_epoch: "epoch-old",
        safe_source_sequence: 30,
        lease_expires_at: future,
        status: :active
      })

    result = Frontier.compute(Map.put(base_opts, :peers, [peer]))

    assert result.bootstrap_required_count == 1
    assert result.active_peer_count == 0
    assert result.candidate_floor == 100
  end

  test "disabled mode is a no-op", %{base_opts: base_opts, future: future} do
    peer =
      peer_fixture(%{
        safe_source_sequence: 20,
        lease_expires_at: future,
        status: :active
      })

    result =
      base_opts
      |> Map.put(:mode, :disabled)
      |> Map.put(:peers, [peer])
      |> Frontier.compute()

    assert result.candidate_floor == 10
    refute result.floor_advanced
    assert result.noop?
    assert result.active_peer_count == 0
    assert result.blocking_peer_count == 0
  end

  test "floor monotonicity holds across repeated compute calls", %{
    base_opts: base_opts,
    future: future
  } do
    peer =
      peer_fixture(%{
        safe_source_sequence: 55,
        lease_expires_at: future,
        status: :active
      })

    first =
      base_opts
      |> Map.put(:peers, [peer])
      |> Frontier.compute()

    second =
      base_opts
      |> Map.put(:current_floor, first.candidate_floor)
      |> Map.put(:peers, [peer])
      |> Frontier.compute()

    assert RetentionState.floor_can_advance?(first.candidate_floor, second.candidate_floor)
    assert second.candidate_floor >= first.candidate_floor
  end

  defp peer_fixture(overrides) do
    defaults = %{
      peer_database_uuid: "peer-1",
      peer_history_epoch: "peer-epoch-1",
      source_database_uuid: "db-1",
      source_history_epoch: "epoch-1",
      safe_source_sequence: 0,
      installed_source_compaction_epoch: 0,
      last_seen_at: "2026-01-01T00:00:00Z",
      lease_expires_at: "2099-01-01T00:00:00Z",
      status: :active
    }

    {:ok, peer} = PeerPosition.new(Map.merge(defaults, overrides))
    peer
  end
end
