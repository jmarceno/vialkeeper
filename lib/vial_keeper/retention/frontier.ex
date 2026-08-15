defmodule VialKeeper.Retention.Frontier do
  @moduledoc "Pure stable-frontier calculation from durable peer reports."

  alias VialKeeper.Domain.PeerPosition

  @type compute_result :: %{
          candidate_floor: non_neg_integer(),
          floor_advanced: boolean(),
          active_peer_count: non_neg_integer(),
          expired_peer_count: non_neg_integer(),
          bootstrap_required_count: non_neg_integer(),
          blocking_peer_count: non_neg_integer(),
          noop?: boolean()
        }

  @type opts :: %{
          required(:source_database_uuid) => binary(),
          required(:source_history_epoch) => binary(),
          required(:current_sequence) => non_neg_integer(),
          required(:current_floor) => non_neg_integer(),
          required(:mode) => :disabled | :stable_frontier,
          required(:now) => DateTime.t(),
          optional(:peers) => [PeerPosition.t()]
        }

  @spec compute(opts()) :: compute_result()
  def compute(opts) when is_map(opts) do
    source_database_uuid = Map.fetch!(opts, :source_database_uuid)
    source_history_epoch = Map.fetch!(opts, :source_history_epoch)
    current_sequence = Map.fetch!(opts, :current_sequence)
    current_floor = Map.fetch!(opts, :current_floor)
    mode = Map.fetch!(opts, :mode)
    peers = Map.get(opts, :peers, [])
    now = Map.fetch!(opts, :now)
    now_ms = DateTime.to_unix(now, :millisecond)

    if mode == :disabled do
      %{
        candidate_floor: current_floor,
        floor_advanced: false,
        active_peer_count: 0,
        expired_peer_count: count_expired(peers, now_ms),
        bootstrap_required_count: count_bootstrap_required(peers, source_history_epoch, now_ms),
        blocking_peer_count: 0,
        noop?: true
      }
    else
      {expired_peer_count, bootstrap_required_count, admitted_safes, active_peer_count} =
        Enum.reduce(peers, {0, 0, [], 0}, fn peer, counts ->
          accumulate_peer(
            peer,
            source_database_uuid,
            source_history_epoch,
            current_sequence,
            now_ms,
            counts
          )
        end)

      frontier =
        case admitted_safes do
          [] -> current_sequence
          safes -> Enum.min(safes)
        end

      candidate_floor = max(current_floor, frontier)
      floor_advanced = candidate_floor > current_floor

      %{
        candidate_floor: candidate_floor,
        floor_advanced: floor_advanced,
        active_peer_count: active_peer_count,
        expired_peer_count: expired_peer_count,
        bootstrap_required_count: bootstrap_required_count,
        blocking_peer_count: active_peer_count,
        noop?: not floor_advanced
      }
    end
  end

  defp accumulate_peer(
         peer,
         source_database_uuid,
         source_history_epoch,
         current_sequence,
         now_ms,
         {expired, bootstrap, safes, active}
       ) do
    case classify_peer(peer, source_database_uuid, source_history_epoch, current_sequence, now_ms) do
      :expired -> {expired + 1, bootstrap, safes, active}
      :bootstrap -> {expired, bootstrap + 1, safes, active}
      :inactive -> {expired, bootstrap, safes, active}
      {:active, safe} -> {expired, bootstrap, [safe | safes], active + 1}
    end
  end

  defp classify_peer(peer, source_database_uuid, source_history_epoch, current_sequence, now_ms) do
    cond do
      peer.source_database_uuid != source_database_uuid -> :inactive
      PeerPosition.expired?(peer, now_ms) -> :expired
      peer.source_history_epoch != source_history_epoch -> :bootstrap
      peer.status == :bootstrap_required -> :bootstrap
      peer.status != :active -> :inactive
      true -> {:active, bounded_safe(peer.safe_source_sequence, current_sequence)}
    end
  end

  defp count_expired(peers, now_ms) do
    Enum.count(peers, &PeerPosition.expired?(&1, now_ms))
  end

  defp count_bootstrap_required(peers, source_history_epoch, now_ms) do
    Enum.count(peers, fn peer ->
      not PeerPosition.expired?(peer, now_ms) and
        (peer.source_history_epoch != source_history_epoch or peer.status == :bootstrap_required)
    end)
  end

  defp bounded_safe(safe, current_sequence) when is_integer(safe), do: min(safe, current_sequence)
end
