defmodule VialKeeper.JSON.SignedPayload do
  @moduledoc "Encodes canonical JSON payloads with an integrity checksum."

  alias VialKeeper.JSON.Canonical

  @doc "Encodes a map as unpadded URL-safe base64 with a SHA-256 checksum."
  @spec encode(map()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def encode(payload) when is_map(payload) do
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
end
