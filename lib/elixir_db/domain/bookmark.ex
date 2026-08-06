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
end
