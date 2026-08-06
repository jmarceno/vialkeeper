defmodule ElixirDB.Domain.Bookmark do
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
    cond do
      attrs[:version] != 1 ->
        {:error, ElixirDB.Error.invalid_bookmark("unsupported bookmark version")}

      not is_integer(attrs[:protocol_major]) or attrs[:protocol_major] < 1 ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark protocol_major is invalid")}

      not is_binary(attrs[:query_fingerprint]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark query_fingerprint is required")}

      not is_integer(attrs[:sequence]) or attrs[:sequence] < 0 ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark sequence must be non-negative")}

      not is_binary(attrs[:last_id]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark last_id is required")}

      not is_binary(attrs[:checksum]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark checksum is required")}

      not is_nil(attrs[:index_id]) and not is_binary(attrs[:index_id]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark index_id must be a string")}

      not is_nil(attrs[:index_digest]) and not is_binary(attrs[:index_digest]) ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark index_digest must be a string")}

      not is_nil(attrs[:sort_direction]) and attrs[:sort_direction] not in ["asc", "desc"] ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark sort_direction is invalid")}

      true ->
        {:ok, struct(__MODULE__, attrs)}
    end
  end
end
