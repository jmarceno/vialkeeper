defmodule ElixirDB.JSON.SignedPayload do
  @moduledoc false

  alias ElixirDB.JSON.Canonical

  @spec encode(map()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
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
