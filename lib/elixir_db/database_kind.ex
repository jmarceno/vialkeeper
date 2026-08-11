defmodule ElixirDB.DatabaseKind do
  @moduledoc "Canonical database-kind values stored in backend metadata."

  @type t :: :ordinary | :derived

  @spec normalize(term()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def normalize(:ordinary), do: {:ok, :ordinary}
  def normalize(:derived), do: {:ok, :derived}
  def normalize("ordinary"), do: {:ok, :ordinary}
  def normalize("derived"), do: {:ok, :derived}

  def normalize(_),
    do: {:error, ElixirDB.Error.invalid_request("database kind must be ordinary or derived")}

  @spec storage(t()) :: binary()
  def storage(:ordinary), do: "ordinary"
  def storage(:derived), do: "derived"

  @spec ordinary() :: t()
  def ordinary, do: :ordinary

  @spec derived() :: t()
  def derived, do: :derived

  @spec from_storage(term()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_storage(value), do: normalize(value)
end
