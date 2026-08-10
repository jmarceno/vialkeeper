defmodule ElixirDB.View.BookmarkCodec do
  @moduledoc """
  Self-contained view query bookmarks bound to definition digest and indexed-through.
  """

  alias ElixirDB.JSON.{Canonical, SignedPayload, StrictDecoder}

  @spec encode(map()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def encode(payload) when is_map(payload) do
    SignedPayload.encode(Map.put(payload, "version", 1))
  end

  @spec decode(binary(), map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def decode(bookmark, expected \\ %{})

  def decode(bookmark, expected) when is_binary(bookmark) do
    with {:ok, encoded} <- Base.url_decode64(bookmark, padding: false),
         {:ok, value} when is_map(value) <- StrictDecoder.decode(encoded),
         {:ok, checksum} <- Map.fetch(value, "checksum"),
         true <- is_binary(checksum),
         unsigned <- Map.delete(value, "checksum"),
         {:ok, canonical} <- Canonical.encode(unsigned),
         true <- checksum == :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower),
         true <- value["version"] == 1,
         true <- is_binary(value["definition_digest"]),
         true <- is_integer(value["indexed_through"]) and value["indexed_through"] >= 0,
         true <- is_binary(value["key_sort"]),
         true <- is_binary(value["document_id"]),
         :ok <- validate_expected(value, expected) do
      {:ok, value}
    else
      {:error, _} = error ->
        error

      _ ->
        {:error,
         ElixirDB.Error.invalid_bookmark("view bookmark is invalid or bound to another view")}
    end
  end

  def decode(_, _),
    do: {:error, ElixirDB.Error.invalid_bookmark("view bookmark must be an opaque string")}

  defp validate_expected(value, expected) do
    Enum.reduce_while(expected, :ok, fn {key, expected_value}, :ok ->
      if Map.get(value, key) == expected_value, do: {:cont, :ok}, else: {:halt, {:error, :mismatch}}
    end)
    |> case do
      :ok -> :ok
      {:error, :mismatch} -> {:error, ElixirDB.Error.invalid_bookmark("view bookmark is stale")}
    end
  end
end
