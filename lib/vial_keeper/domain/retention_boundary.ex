defmodule VialKeeper.Domain.RetentionBoundary do
  @moduledoc "Validated compact-retention history boundary entry."

  @enforce_keys [
    :document_id,
    :history_id,
    :minimum_retained_generation,
    :retired,
    :retired_branch_roots
  ]
  defstruct [
    :document_id,
    :history_id,
    :minimum_retained_generation,
    :retired,
    :retired_branch_roots
  ]

  @type t :: %__MODULE__{
          document_id: binary(),
          history_id: binary(),
          minimum_retained_generation: pos_integer() | nil,
          retired: boolean(),
          retired_branch_roots: [binary()]
        }

  @known [
    :document_id,
    :history_id,
    :minimum_retained_generation,
    :retired,
    :retired_branch_roots
  ]

  @spec new(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, VialKeeper.Error.invalid_request("unknown retention boundary field")},
      else: build(attrs)
  end

  def new(_), do: {:error, VialKeeper.Error.invalid_request("retention boundary must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, VialKeeper.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "document_id",
      "history_id",
      "minimum_retained_generation",
      "retired",
      "retired_branch_roots"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, VialKeeper.Error.invalid_request("unknown retention boundary field")}
    else
      new(wire_attrs(attrs))
    end
  end

  def from_wire(_),
    do: {:error, VialKeeper.Error.invalid_request("retention boundary must be an object")}

  @spec active(binary(), binary(), pos_integer(), [binary()]) :: t()
  def active(document_id, history_id, minimum_retained_generation, retired_branch_roots) do
    {:ok, boundary} =
      new(
        boundary_attrs(
          document_id,
          history_id,
          minimum_retained_generation,
          false,
          retired_branch_roots
        )
      )

    boundary
  end

  @spec retired(binary(), binary(), [binary()]) :: t()
  def retired(document_id, history_id, retired_branch_roots) do
    {:ok, boundary} =
      new(boundary_attrs(document_id, history_id, nil, true, retired_branch_roots))

    boundary
  end

  defp wire_attrs(attrs) do
    boundary_attrs(
      attrs["document_id"],
      attrs["history_id"],
      attrs["minimum_retained_generation"],
      attrs["retired"],
      attrs["retired_branch_roots"]
    )
  end

  defp boundary_attrs(
         document_id,
         history_id,
         minimum_retained_generation,
         retired,
         retired_branch_roots
       ) do
    %{
      document_id: document_id,
      history_id: history_id,
      minimum_retained_generation: minimum_retained_generation,
      retired: retired,
      retired_branch_roots: retired_branch_roots
    }
  end

  defp build(attrs) do
    case validation_error(attrs) do
      nil -> {:ok, struct(__MODULE__, attrs)}
      error -> {:error, error}
    end
  end

  defp validation_error(attrs) do
    validators = [
      &validate_document_id/1,
      &validate_history_id/1,
      &validate_retired_branch_roots/1,
      &validate_retired_state/1
    ]

    Enum.find_value(validators, & &1.(attrs))
  end

  defp validate_document_id(%{document_id: value}) when is_binary(value) and value != "",
    do: nil

  defp validate_document_id(_),
    do: VialKeeper.Error.invalid_request("retention boundary document_id is required")

  defp validate_history_id(%{history_id: value}) when is_binary(value) and value != "", do: nil

  defp validate_history_id(_),
    do: VialKeeper.Error.invalid_request("retention boundary history_id is required")

  defp validate_retired_branch_roots(%{retired_branch_roots: value}) when is_list(value) do
    if Enum.all?(value, &(is_binary(&1) and &1 != "")),
      do: nil,
      else:
        VialKeeper.Error.invalid_request("retention boundary retired_branch_roots must be binaries")
  end

  defp validate_retired_branch_roots(_),
    do: VialKeeper.Error.invalid_request("retention boundary retired_branch_roots must be an array")

  defp validate_retired_state(%{retired: true, minimum_retained_generation: nil}), do: nil

  defp validate_retired_state(%{retired: false, minimum_retained_generation: value})
       when is_integer(value) and value > 0,
       do: nil

  defp validate_retired_state(%{retired: retired}) when not is_boolean(retired),
    do: VialKeeper.Error.invalid_request("retention boundary retired must be boolean")

  defp validate_retired_state(%{retired: true}),
    do:
      VialKeeper.Error.invalid_request(
        "retention boundary minimum_retained_generation must be null when retired"
      )

  defp validate_retired_state(%{retired: false}),
    do:
      VialKeeper.Error.invalid_request(
        "retention boundary minimum_retained_generation must be a positive integer when not retired"
      )
end
