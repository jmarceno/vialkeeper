defmodule VialKeeper.Shadow.Observation do
  @moduledoc "Observed lifecycle and health state for a managed shadow."

  alias VialKeeper.MapAccess

  @states ~w(absent provisioning bootstrapping ready unhealthy destroying orphaned)a
  @enforce_keys [:state, :applied_source_sequence, :updated_at]
  defstruct state: :absent,
            worker_node_id: nil,
            applied_source_sequence: 0,
            last_error_code: nil,
            updated_at: nil

  @type t :: %__MODULE__{
          state: atom(),
          worker_node_id: String.t() | nil,
          applied_source_sequence: non_neg_integer(),
          last_error_code: atom() | nil,
          updated_at: String.t()
        }

  @spec states() :: [atom()]
  def states, do: @states

  @spec new(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def new(attrs \\ %{})

  def new(attrs) when is_map(attrs) do
    with {:ok, attrs} <- normalize_keys(attrs),
         state <- attrs["state"] || :absent,
         sequence <- attrs["applied_source_sequence"] || 0,
         {:ok, state} <- normalize_state(state),
         :ok <- validate_sequence(sequence) do
      {:ok,
       %__MODULE__{
         state: state,
         worker_node_id: attrs["worker_node_id"],
         applied_source_sequence: sequence,
         last_error_code: attrs["last_error_code"],
         updated_at: attrs["updated_at"] || DateTime.utc_now() |> DateTime.to_iso8601()
       }}
    end
  end

  def new(_), do: {:error, VialKeeper.Error.invalid_request("shadow observation must be an object")}

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = observation) do
    %{
      "state" => Atom.to_string(observation.state),
      "worker_node_id" => observation.worker_node_id,
      "applied_source_sequence" => observation.applied_source_sequence,
      "last_error_code" =>
        if(observation.last_error_code, do: Atom.to_string(observation.last_error_code), else: nil),
      "updated_at" => observation.updated_at
    }
  end

  defp normalize_state(value) when is_atom(value) and value in @states, do: {:ok, value}

  defp normalize_state(value) when is_binary(value) do
    atom = String.to_existing_atom(value)

    if atom in @states,
      do: {:ok, atom},
      else: {:error, VialKeeper.Error.invalid_request("unknown shadow lifecycle state")}
  rescue
    ArgumentError -> {:error, VialKeeper.Error.invalid_request("unknown shadow lifecycle state")}
  end

  defp normalize_state(_),
    do: {:error, VialKeeper.Error.invalid_request("unknown shadow lifecycle state")}

  defp validate_sequence(value) when is_integer(value) and value >= 0, do: :ok

  defp validate_sequence(_),
    do:
      {:error, VialKeeper.Error.invalid_request("shadow watermark must be a non-negative integer")}

  defp normalize_keys(map) do
    case MapAccess.string_keys(map) do
      {:ok, normalized} ->
        {:ok, normalized}

      :key_collision ->
        {:error, VialKeeper.Error.invalid_request("shadow fields contain duplicate keys")}
    end
  end
end
