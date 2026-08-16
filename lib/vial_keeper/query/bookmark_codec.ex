defmodule VialKeeper.Query.BookmarkCodec do
  @moduledoc "Encodes and validates opaque query pagination bookmarks."
  alias VialKeeper.Domain.Bookmark
  alias VialKeeper.JSON.{Canonical, SignedPayload, StrictDecoder}

  @spec encode(Bookmark.t() | map()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def encode(%Bookmark{} = bookmark) do
    encode(%{
      "query_fingerprint" => bookmark.query_fingerprint,
      "plan_digest" => bookmark.plan_digest,
      "index_bindings" => bookmark.index_bindings,
      "sequence" => bookmark.sequence,
      "sort_direction" => bookmark.sort_direction,
      "ordering_key" => bookmark.ordering_key,
      "last_id" => bookmark.last_id
    })
  end

  def encode(payload) when is_map(payload) do
    payload
    |> Map.put("version", 1)
    |> Map.put("protocol_major", VialKeeper.protocol_major())
    |> SignedPayload.encode()
  end

  @spec decode(term()) :: {:ok, Bookmark.t()} | {:error, VialKeeper.Error.t()}
  @spec decode(term(), map()) :: {:ok, Bookmark.t()} | {:error, VialKeeper.Error.t()}
  def decode(bookmark, expected \\ %{})

  def decode(bookmark, expected) when is_binary(bookmark) do
    with {:ok, encoded} <- Base.url_decode64(bookmark, padding: false),
         {:ok, value} when is_map(value) <- StrictDecoder.decode(encoded),
         {:ok, checksum} <- Map.fetch(value, "checksum"),
         true <- is_binary(checksum),
         unsigned <- Map.delete(value, "checksum"),
         {:ok, canonical} <- Canonical.encode(unsigned),
         true <- checksum == :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower),
         true <- unsigned["version"] == 1,
         true <- unsigned["protocol_major"] == VialKeeper.protocol_major(),
         true <- is_binary(unsigned["query_fingerprint"]),
         true <-
           is_binary(unsigned["plan_digest"]) and
             valid_digest?(unsigned["plan_digest"]),
         true <- is_list(unsigned["index_bindings"]),
         true <- is_integer(unsigned["sequence"]) and unsigned["sequence"] >= 0,
         true <- Map.has_key?(unsigned, "ordering_key"),
         true <- is_binary(unsigned["last_id"]),
         :ok <- validate_expected(unsigned, expected),
         {:ok, struct} <- Bookmark.from_wire(value) do
      {:ok, struct}
    else
      _ ->
        {:error, VialKeeper.Error.invalid_bookmark("bookmark is invalid or bound to another query")}
    end
  end

  def decode(_, _),
    do: {:error, VialKeeper.Error.invalid_bookmark("bookmark must be an opaque string")}

  defp validate_expected(value, expected) do
    Enum.reduce_while(expected, :ok, fn {key, expected_value}, :ok ->
      if Map.get(value, key) == expected_value, do: {:cont, :ok}, else: {:halt, {:error, :mismatch}}
    end)
  end

  defp valid_digest?(value) do
    Regex.match?(~r/^[0-9a-f]{64}$/, value) and value != String.duplicate("0", 64)
  end
end
