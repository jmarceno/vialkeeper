defmodule ElixirDB.Domain.PeerPosition do
  @moduledoc "Validated peer-ledger position for stable-frontier retention."

  alias ElixirDB.Domain.ValidatedStruct

  @enforce_keys [
    :peer_database_uuid,
    :peer_history_epoch,
    :source_database_uuid,
    :source_history_epoch,
    :safe_source_sequence,
    :installed_source_compaction_epoch,
    :last_seen_at,
    :lease_expires_at,
    :status
  ]
  defstruct [
    :peer_database_uuid,
    :peer_history_epoch,
    :source_database_uuid,
    :source_history_epoch,
    :safe_source_sequence,
    :installed_source_compaction_epoch,
    :last_seen_at,
    :lease_expires_at,
    :status
  ]

  @type status :: :active | :expired | :bootstrap_required | :quarantined

  @type t :: %__MODULE__{
          peer_database_uuid: binary(),
          peer_history_epoch: binary(),
          source_database_uuid: binary(),
          source_history_epoch: binary(),
          safe_source_sequence: non_neg_integer(),
          installed_source_compaction_epoch: non_neg_integer(),
          last_seen_at: binary(),
          lease_expires_at: binary(),
          status: status()
        }

  @known [
    :peer_database_uuid,
    :peer_history_epoch,
    :source_database_uuid,
    :source_history_epoch,
    :safe_source_sequence,
    :installed_source_compaction_epoch,
    :last_seen_at,
    :lease_expires_at,
    :status
  ]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown peer position field")},
      else: ValidatedStruct.build(__MODULE__, attrs, &peer_position_validation_error/1)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("peer position must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "peer_database_uuid",
      "peer_history_epoch",
      "source_database_uuid",
      "source_history_epoch",
      "safe_source_sequence",
      "installed_source_compaction_epoch",
      "last_seen_at",
      "lease_expires_at",
      "status"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, ElixirDB.Error.invalid_request("unknown peer position field")}
    else
      with {:ok, status} <- decode_status(attrs["status"]) do
        new(%{
          peer_database_uuid: attrs["peer_database_uuid"],
          peer_history_epoch: attrs["peer_history_epoch"],
          source_database_uuid: attrs["source_database_uuid"],
          source_history_epoch: attrs["source_history_epoch"],
          safe_source_sequence: attrs["safe_source_sequence"],
          installed_source_compaction_epoch: attrs["installed_source_compaction_epoch"],
          last_seen_at: attrs["last_seen_at"],
          lease_expires_at: attrs["lease_expires_at"],
          status: status
        })
      end
    end
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("peer position must be an object")}

  @spec expired?(t(), non_neg_integer()) :: boolean()
  def expired?(%__MODULE__{lease_expires_at: lease_expires_at}, now_ms)
      when is_integer(now_ms) and now_ms >= 0 do
    case DateTime.from_iso8601(lease_expires_at) do
      {:ok, expires_at, _offset} ->
        DateTime.compare(expires_at, DateTime.from_unix!(now_ms, :millisecond)) != :gt

      _ ->
        true
    end
  end

  @spec admits_to_frontier?(t(), binary(), non_neg_integer()) :: boolean()
  def admits_to_frontier?(
        %__MODULE__{} = peer,
        source_history_epoch,
        now_ms
      )
      when is_binary(source_history_epoch) and is_integer(now_ms) and now_ms >= 0 do
    not expired?(peer, now_ms) and peer.status == :active and
      peer.source_history_epoch == source_history_epoch and
      is_integer(peer.safe_source_sequence) and peer.safe_source_sequence >= 0
  end

  @spec epoch_changed?(t(), t()) :: boolean()
  def epoch_changed?(
        %__MODULE__{source_history_epoch: previous_epoch},
        %__MODULE__{source_history_epoch: incoming_epoch}
      ),
      do: previous_epoch != incoming_epoch

  @spec peer_history_changed?(t(), t()) :: boolean()
  def peer_history_changed?(
        %__MODULE__{peer_history_epoch: previous_epoch},
        %__MODULE__{peer_history_epoch: incoming_epoch}
      ),
      do: previous_epoch != incoming_epoch

  @spec history_changed?(t(), t()) :: boolean()
  def history_changed?(previous, incoming),
    do: epoch_changed?(previous, incoming) or peer_history_changed?(previous, incoming)

  @spec regresses?(t(), t()) :: boolean()
  def regresses?(
        %__MODULE__{
          source_history_epoch: previous_epoch,
          safe_source_sequence: previous_safe,
          installed_source_compaction_epoch: previous_compaction
        },
        %__MODULE__{
          source_history_epoch: incoming_epoch,
          safe_source_sequence: incoming_safe,
          installed_source_compaction_epoch: incoming_compaction
        }
      ) do
    previous_epoch == incoming_epoch and
      (incoming_safe < previous_safe or incoming_compaction < previous_compaction)
  end

  defp peer_position_validation_error(attrs) do
    with nil <- validate_peer_database_uuid(attrs),
         nil <- validate_peer_history_epoch(attrs),
         nil <- validate_source_database_uuid(attrs),
         nil <- validate_source_history_epoch(attrs),
         nil <- validate_safe_source_sequence(attrs),
         nil <- validate_installed_source_compaction_epoch(attrs),
         nil <- validate_last_seen_at(attrs),
         nil <- validate_lease_expires_at(attrs) do
      validate_status(attrs)
    end
  end

  defp validate_peer_database_uuid(%{peer_database_uuid: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_peer_database_uuid(_),
    do: ElixirDB.Error.invalid_request("peer position peer_database_uuid is required")

  defp validate_peer_history_epoch(%{peer_history_epoch: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_peer_history_epoch(_),
    do: ElixirDB.Error.invalid_request("peer position peer_history_epoch is required")

  defp validate_source_database_uuid(%{source_database_uuid: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_source_database_uuid(_),
    do: ElixirDB.Error.invalid_request("peer position source_database_uuid is required")

  defp validate_source_history_epoch(%{source_history_epoch: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_source_history_epoch(_),
    do: ElixirDB.Error.invalid_request("peer position source_history_epoch is required")

  defp validate_safe_source_sequence(%{safe_source_sequence: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_safe_source_sequence(_),
    do: ElixirDB.Error.invalid_request("peer position safe_source_sequence must be non-negative")

  defp validate_installed_source_compaction_epoch(%{installed_source_compaction_epoch: value})
       when is_integer(value) and value >= 0,
       do: nil

  defp validate_installed_source_compaction_epoch(_),
    do:
      ElixirDB.Error.invalid_request(
        "peer position installed_source_compaction_epoch must be non-negative"
      )

  defp validate_last_seen_at(%{last_seen_at: value}) when is_binary(value) and value != "",
    do: nil

  defp validate_last_seen_at(_),
    do: ElixirDB.Error.invalid_request("peer position last_seen_at is required")

  defp validate_lease_expires_at(%{lease_expires_at: value})
       when is_binary(value) and value != "",
       do: nil

  defp validate_lease_expires_at(_),
    do: ElixirDB.Error.invalid_request("peer position lease_expires_at is required")

  defp validate_status(%{status: value})
       when value in [:active, :expired, :bootstrap_required, :quarantined],
       do: nil

  defp validate_status(_),
    do:
      ElixirDB.Error.invalid_request(
        "peer position status must be active, expired, bootstrap_required, or quarantined"
      )

  defp decode_status("active"), do: {:ok, :active}
  defp decode_status("expired"), do: {:ok, :expired}
  defp decode_status("bootstrap_required"), do: {:ok, :bootstrap_required}
  defp decode_status("quarantined"), do: {:ok, :quarantined}

  defp decode_status(_),
    do:
      {:error,
       ElixirDB.Error.invalid_request(
         "peer position status must be active, expired, bootstrap_required, or quarantined"
       )}
end
