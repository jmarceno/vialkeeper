defmodule ElixirDB.Domain.Checkpoint do
  @enforce_keys [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]
  defstruct [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]

  @type t :: %__MODULE__{
          version: pos_integer(),
          replication_id: binary(),
          checkpoint_version: non_neg_integer(),
          session_id: binary(),
          source_sequence: non_neg_integer(),
          history: list()
        }

  @known [
    :version,
    :replication_id,
    :checkpoint_version,
    :session_id,
    :source_sequence,
    :history
  ]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown checkpoint field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("checkpoint must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "version",
      "replication_id",
      "checkpoint_version",
      "session_id",
      "source_sequence",
      "history"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, ElixirDB.Error.invalid_request("unknown checkpoint field")}
    else
      new(%{
        version: attrs["version"],
        replication_id: attrs["replication_id"],
        checkpoint_version: attrs["checkpoint_version"],
        session_id: attrs["session_id"],
        source_sequence: attrs["source_sequence"],
        history: attrs["history"]
      })
    end
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("checkpoint must be an object")}

  defp build(attrs) do
    cond do
      attrs[:version] != 1 ->
        {:error, ElixirDB.Error.invalid_request("unsupported checkpoint version")}

      not is_binary(attrs[:replication_id]) or attrs[:replication_id] == "" ->
        {:error, ElixirDB.Error.invalid_request("checkpoint replication_id is required")}

      not is_integer(attrs[:checkpoint_version]) or attrs[:checkpoint_version] < 0 ->
        {:error, ElixirDB.Error.invalid_request("checkpoint_version must be non-negative")}

      not is_binary(attrs[:session_id]) or attrs[:session_id] == "" ->
        {:error, ElixirDB.Error.invalid_request("checkpoint session_id is required")}

      not is_integer(attrs[:source_sequence]) or attrs[:source_sequence] < 0 ->
        {:error, ElixirDB.Error.invalid_request("source_sequence must be non-negative")}

      not is_list(attrs[:history]) ->
        {:error, ElixirDB.Error.invalid_request("checkpoint history must be an array")}

      true ->
        {:ok, struct(__MODULE__, attrs)}
    end
  end
end
