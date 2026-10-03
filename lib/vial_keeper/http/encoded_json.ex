defmodule VialKeeper.HTTP.EncodedJSON do
  @moduledoc """
  A JSON value already encoded to iodata, embedded verbatim when a response
  envelope is encoded.

  Lets a response that repeats one value under several keys (query results
  appear as both `documents` and `results`) encode that value once.
  """

  @enforce_keys [:iodata]
  defstruct [:iodata]

  @type t :: %__MODULE__{iodata: iodata()}

  @doc "Encodes `value` once for embedding."
  @spec encode(term()) :: t()
  def encode(value), do: %__MODULE__{iodata: JSON.encode_to_iodata!(value)}

  defimpl JSON.Encoder do
    def encode(%{iodata: iodata}, _encoder), do: iodata
  end
end
