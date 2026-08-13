defmodule ElixirDB.DatabaseKind do
  @moduledoc "Canonical database-kind values stored in backend metadata."

  @type t :: :ordinary | :derived | :shadow

  @spec normalize(term()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def normalize(:ordinary), do: {:ok, :ordinary}
  def normalize(:derived), do: {:ok, :derived}
  def normalize(:shadow), do: {:ok, :shadow}
  def normalize("ordinary"), do: {:ok, :ordinary}
  def normalize("derived"), do: {:ok, :derived}
  def normalize("shadow"), do: {:ok, :shadow}

  def normalize(_),
    do:
      {:error, ElixirDB.Error.invalid_request("database kind must be ordinary, derived, or shadow")}

  @spec storage(t()) :: binary()
  def storage(:ordinary), do: "ordinary"
  def storage(:derived), do: "derived"
  def storage(:shadow), do: "shadow"

  @spec ordinary() :: t()
  def ordinary, do: :ordinary

  @spec derived() :: t()
  def derived, do: :derived

  @spec shadow() :: t()
  def shadow, do: :shadow

  @spec from_storage(term()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_storage(value), do: normalize(value)
end
