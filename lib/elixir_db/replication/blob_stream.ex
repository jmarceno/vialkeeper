defmodule ElixirDB.Replication.BlobStream do
  @moduledoc """
  Replication attachment byte stream envelope.

  `body` is an enumerable of original uncompressed byte chunks.
  """

  alias ElixirDB.Attachments.Manifest

  @enforce_keys [:digest, :length, :body]
  defstruct [:digest, :length, :body]

  @type t :: %__MODULE__{
          digest: binary(),
          length: non_neg_integer(),
          body: Enumerable.t()
        }

  @spec new(binary(), non_neg_integer(), Enumerable.t()) ::
          {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(digest, length, body)
      when is_integer(length) and length >= 0 and not is_nil(body) do
    with {:ok, validated_digest} <- Manifest.validate_digest(digest) do
      {:ok, %__MODULE__{digest: validated_digest, length: length, body: body}}
    end
  end

  def new(_, _, _), do: {:error, ElixirDB.Error.invalid_request("invalid blob stream")}
end
