defmodule ElixirDB.Domain.BoundaryPage do
  @moduledoc "Validated compact-retention boundary page for wire transfer."

  alias ElixirDB.Domain.RetentionBoundary
  alias ElixirDB.JSON.Canonical

  @enforce_keys [
    :source_history_epoch,
    :compaction_epoch,
    :boundary_digest,
    :next_page,
    :boundaries
  ]
  defstruct [
    :source_history_epoch,
    :compaction_epoch,
    :boundary_digest,
    :next_page,
    :boundaries
  ]

  @type t :: %__MODULE__{
          source_history_epoch: binary(),
          compaction_epoch: non_neg_integer(),
          boundary_digest: binary(),
          next_page: binary() | nil,
          boundaries: [RetentionBoundary.t()]
        }

  @known [:source_history_epoch, :compaction_epoch, :boundary_digest, :next_page, :boundaries]

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    if Enum.any?(Map.keys(attrs), &(&1 not in @known)),
      do: {:error, ElixirDB.Error.invalid_request("unknown boundary page field")},
      else: build(attrs)
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("boundary page must be an object")}

  @spec from_wire(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_wire(attrs) when is_map(attrs) do
    allowed = [
      "source_history_epoch",
      "compaction_epoch",
      "boundary_digest",
      "next_page",
      "boundaries"
    ]

    if Enum.any?(Map.keys(attrs), &(&1 not in allowed)) do
      {:error, ElixirDB.Error.invalid_request("unknown boundary page field")}
    else
      with {:ok, boundaries} <- normalize_boundaries(attrs["boundaries"]) do
        new(wire_page_attrs(attrs, boundaries))
      end
    end
  end

  def from_wire(_), do: {:error, ElixirDB.Error.invalid_request("boundary page must be an object")}

  @spec digest_for([RetentionBoundary.t()]) :: binary()
  def digest_for(boundaries) when is_list(boundaries) do
    boundaries
    |> Enum.sort_by(&{&1.document_id, &1.history_id})
    |> Enum.map(&boundary_digest_entry/1)
    |> then(&Canonical.encode!/1)
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp build(attrs) do
    with {:ok, boundaries} <- normalize_boundaries(attrs[:boundaries]),
         :ok <- validate_page_fields(attrs) do
      {:ok, struct(__MODULE__, Map.put(attrs, :boundaries, boundaries))}
    end
  end

  @spec page(
          binary(),
          non_neg_integer(),
          binary(),
          binary() | nil,
          [RetentionBoundary.t()]
        ) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def page(source_history_epoch, compaction_epoch, boundary_digest, next_page, boundaries) do
    new(
      page_attrs(
        %{
          source_history_epoch: source_history_epoch,
          compaction_epoch: compaction_epoch,
          boundary_digest: boundary_digest,
          next_page: next_page
        },
        boundaries
      )
    )
  end

  defp page_attrs(attrs, boundaries) do
    %{
      source_history_epoch: attrs[:source_history_epoch],
      compaction_epoch: attrs[:compaction_epoch],
      boundary_digest: attrs[:boundary_digest],
      next_page: attrs[:next_page],
      boundaries: boundaries
    }
  end

  defp wire_page_attrs(attrs, boundaries) do
    %{
      source_history_epoch: attrs["source_history_epoch"],
      compaction_epoch: attrs["compaction_epoch"],
      boundary_digest: attrs["boundary_digest"],
      next_page: attrs["next_page"],
      boundaries: boundaries
    }
  end

  defp normalize_boundaries(boundaries) when is_list(boundaries) do
    Enum.reduce_while(boundaries, {:ok, []}, fn boundary, {:ok, acc} ->
      case normalize_boundary(boundary) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp normalize_boundaries(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page boundaries must be an array")}

  defp normalize_boundary(%RetentionBoundary{} = value), do: {:ok, value}

  defp normalize_boundary(%{} = value) do
    if Enum.any?(Map.keys(value), &is_atom/1),
      do: RetentionBoundary.new(value),
      else: RetentionBoundary.from_wire(value)
  end

  defp normalize_boundary(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page boundaries entries must be objects")}

  defp validate_page_fields(attrs) do
    validators = [
      fn -> validate_source_history_epoch(attrs[:source_history_epoch]) end,
      fn -> validate_compaction_epoch(attrs[:compaction_epoch]) end,
      fn -> validate_boundary_digest(attrs[:boundary_digest]) end,
      fn -> validate_next_page(attrs[:next_page]) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> :ok
      error -> error
    end
  end

  defp validate_source_history_epoch(epoch)
       when is_binary(epoch) and epoch != "",
       do: nil

  defp validate_source_history_epoch(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page source_history_epoch is required")}

  defp validate_compaction_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: nil

  defp validate_compaction_epoch(_),
    do:
      {:error,
       ElixirDB.Error.invalid_request("boundary page compaction_epoch must be non-negative")}

  defp validate_boundary_digest(digest) when is_binary(digest) and digest != "", do: nil

  defp validate_boundary_digest(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page boundary_digest is required")}

  defp validate_next_page(nil), do: nil
  defp validate_next_page(page) when is_binary(page) and page != "", do: nil

  defp validate_next_page(_),
    do: {:error, ElixirDB.Error.invalid_request("boundary page next_page must be a binary or null")}

  defp boundary_digest_entry(%RetentionBoundary{} = boundary) do
    %{
      "document_id" => boundary.document_id,
      "history_id" => boundary.history_id,
      "minimum_retained_generation" => boundary.minimum_retained_generation,
      "retired" => boundary.retired,
      "retired_branch_roots" => Enum.sort(boundary.retired_branch_roots)
    }
  end
end
