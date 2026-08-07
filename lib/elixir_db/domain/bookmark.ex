defmodule ElixirDB.Domain.Bookmark do
  @moduledoc "Validated bookmark state for paginated queries."

  @enforce_keys [:version, :protocol_major, :query_fingerprint, :sequence, :last_id, :checksum]
  defstruct [
    :version,
    :protocol_major,
    :query_fingerprint,
    :index_id,
    :index_digest,
    :sequence,
    :sort_direction,
    :ordering_key,
    :last_id,
    :checksum
  ]

  @type t :: %__MODULE__{
          version: pos_integer(),
          protocol_major: pos_integer(),
          query_fingerprint: binary(),
          index_id: binary() | nil,
          index_digest: binary() | nil,
          sequence: non_neg_integer(),
          sort_direction: binary() | nil,
          ordering_key: term(),
          last_id: binary(),
          checksum: binary()
        }

  @known [
    :version,
    :protocol_major,
    :query_fingerprint,
    :index_id,
    :index_digest,
    :sequence,
    :sort_direction,
    :ordering_key,
    :last_id,
    :checksum
  ]

  @wire_keys [
    "version",
    "protocol_major",
    "query_fingerprint",
    "index_id",
    "index_digest",
    "sequence",
    "sort_direction",
    "ordering_key",
    "last_id",
    "checksum"
  ]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown bookmark field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("bookmark must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @wire_keys)) do
      {:error, ElixirDB.Error.invalid_request("unknown bookmark field")}
    else
      new(%{
        version: attrs["version"],
        protocol_major: attrs["protocol_major"],
        query_fingerprint: attrs["query_fingerprint"],
        index_id: attrs["index_id"],
        index_digest: attrs["index_digest"],
        sequence: attrs["sequence"],
        sort_direction: attrs["sort_direction"],
        ordering_key: attrs["ordering_key"],
        last_id: attrs["last_id"],
        checksum: attrs["checksum"]
      })
    end
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("bookmark must be an object")}

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_version/1,
      &validate_protocol_major/1,
      &validate_query_fingerprint/1,
      &validate_sequence/1,
      &validate_last_id/1,
      &validate_checksum/1,
      &validate_index_id/1,
      &validate_index_digest/1,
      &validate_sort_direction/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_version(%{version: 1}), do: nil
  defp validate_version(_), do: ElixirDB.Error.invalid_bookmark("unsupported bookmark version")

  defp validate_protocol_major(%{protocol_major: value})
       when is_integer(value) and value >= 1,
       do: nil

  defp validate_protocol_major(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark protocol_major is invalid")

  defp validate_query_fingerprint(%{query_fingerprint: value}) when is_binary(value), do: nil

  defp validate_query_fingerprint(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark query_fingerprint is required")

  defp validate_sequence(%{sequence: value}) when is_integer(value) and value >= 0, do: nil

  defp validate_sequence(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark sequence must be non-negative")

  defp validate_last_id(%{last_id: value}) when is_binary(value), do: nil
  defp validate_last_id(_), do: ElixirDB.Error.invalid_bookmark("bookmark last_id is required")

  defp validate_checksum(%{checksum: value}) when is_binary(value), do: nil
  defp validate_checksum(_), do: ElixirDB.Error.invalid_bookmark("bookmark checksum is required")

  defp validate_index_id(%{index_id: value}) when is_nil(value) or is_binary(value), do: nil

  defp validate_index_id(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark index_id must be a string")

  defp validate_index_digest(%{index_digest: value}) when is_nil(value) or is_binary(value),
    do: nil

  defp validate_index_digest(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark index_digest must be a string")

  defp validate_sort_direction(%{sort_direction: value})
       when is_nil(value) or value in ["asc", "desc"],
       do: nil

  defp validate_sort_direction(_),
    do: ElixirDB.Error.invalid_bookmark("bookmark sort_direction is invalid")
end
