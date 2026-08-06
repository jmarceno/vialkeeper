defmodule ElixirDB.Query.BookmarkCodec do
  @moduledoc false
  alias ElixirDB.JSON.{Canonical, StrictDecoder}

  def encode(payload) when is_map(payload) do
    payload = Map.put(payload, "version", 1) |> Map.put("protocol_major", ElixirDB.protocol_major())

    with {:ok, unsigned} <- Canonical.encode(payload),
         value <-
           Map.put(
             payload,
             "checksum",
             :crypto.hash(:sha256, unsigned) |> Base.encode16(case: :lower)
           ),
         {:ok, encoded} <- Canonical.encode(value) do
      {:ok, Base.url_encode64(encoded, padding: false)}
    end
  end

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
         true <- unsigned["protocol_major"] == ElixirDB.protocol_major(),
         true <- is_binary(unsigned["query_fingerprint"]),
         true <- is_integer(unsigned["sequence"]) and unsigned["sequence"] >= 0,
         true <- is_binary(unsigned["last_id"]),
         :ok <- validate_expected(unsigned, expected) do
      {:ok, value}
    else
      _ ->
        {:error, ElixirDB.Error.invalid_bookmark("bookmark is invalid or bound to another query")}
    end
  end

  def decode(_, _),
    do: {:error, ElixirDB.Error.invalid_bookmark("bookmark must be an opaque string")}

  defp validate_expected(value, expected) do
    Enum.reduce_while(expected, :ok, fn {key, expected_value}, :ok ->
      if Map.get(value, key) == expected_value, do: {:cont, :ok}, else: {:halt, {:error, :mismatch}}
    end)
  end
end
