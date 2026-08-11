defmodule ElixirDB.Storage.Memory.Peers do
  @moduledoc """
  Decodes in-memory peer ledger maps into `PeerPosition` structs.
  """

  alias ElixirDB.Domain.PeerPosition

  @doc "Decodes store peer entries keyed by UUID into a peer list."
  @spec decode(map()) :: {:ok, [PeerPosition.t()]} | {:error, ElixirDB.Error.t()}
  def decode(peers) when is_map(peers) do
    Enum.reduce_while(peers, {:ok, []}, fn {_uuid, %{value: value}}, {:ok, acc} ->
      case PeerPosition.from_wire(value) do
        {:ok, peer} -> {:cont, {:ok, [peer | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end
end
