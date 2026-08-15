defmodule VialKeeper.Storage.RequestValidation do
  @moduledoc """
  Shared request validation helpers used by storage adapters.
  """

  alias VialKeeper.Attachments.FilesystemStore

  @uuid_re ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  @doc "Validates a UUID string."
  @spec validate_uuid(term()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_uuid(uuid) when is_binary(uuid) do
    if Regex.match?(@uuid_re, uuid) do
      :ok
    else
      {:error, VialKeeper.Error.invalid_request("database UUID must be a UUID")}
    end
  end

  def validate_uuid(_),
    do: {:error, VialKeeper.Error.invalid_request("database UUID must be a UUID")}

  @doc "Validates a non-negative integer field."
  @spec validate_non_negative_integer(term(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_non_negative_integer(value, _label) when is_integer(value) and value >= 0, do: :ok

  def validate_non_negative_integer(_value, label),
    do: {:error, VialKeeper.Error.invalid_request("#{label} must be a non-negative integer")}

  @doc "Validates a positive integer field."
  @spec validate_positive_integer(term(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_positive_integer(value, _label) when is_integer(value) and value > 0, do: :ok

  def validate_positive_integer(_value, label),
    do: {:error, VialKeeper.Error.invalid_request("#{label} must be a positive integer")}

  @doc "Validates a changes-feed page limit against host and database caps."
  @spec validate_changes_limit(term(), term()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_changes_limit(limit, database_max) when is_integer(limit) and limit > 0 do
    max = min(VialKeeper.Config.host_limits()[:max_changes_batch] || 500, database_max || 500)

    if limit <= max,
      do: :ok,
      else: {:error, VialKeeper.Error.resource_limit("changes batch exceeds the host limit")}
  end

  def validate_changes_limit(_, _),
    do: {:error, VialKeeper.Error.invalid_request("changes limit must be a positive integer")}

  @doc "Rejects change-feed reads below the retention floor."
  @spec validate_changes_since_floor(integer(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_changes_since_floor(since, identity) when is_integer(since) and is_map(identity) do
    floor = Map.get(identity, :retention_floor_sequence, 0) || 0

    if since < floor do
      {:error,
       VialKeeper.Error.history_truncated("changes feed is below the retention floor", %{
         database_uuid: identity.database_uuid,
         history_epoch: identity.history_epoch,
         retention_floor: floor,
         compaction_epoch: Map.get(identity, :compaction_epoch, 0)
       })}
    else
      :ok
    end
  end

  @doc "Verifies one physical attachment digest under a bundle root."
  @spec verify_blob(binary(), binary(), non_neg_integer()) ::
          :ok | {:error, VialKeeper.Error.t()}
  def verify_blob(root, digest, logical_size)
      when is_binary(root) and is_binary(digest) and is_integer(logical_size) do
    case FilesystemStore.verify(root, digest, logical_size) do
      :ok ->
        :ok

      {:error, %VialKeeper.Error{code: :attachment_blob_not_found}} ->
        {:error,
         VialKeeper.Error.attachment_blob_not_found(
           "imported revision references a missing attachment blob",
           %{digest: digest}
         )}

      {:error, %VialKeeper.Error{} = error} ->
        {:error, error}
    end
  end
end
