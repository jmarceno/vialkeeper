defmodule VialKeeper.Runtime.CommandContext do
  @moduledoc "Carries the narrow authority used when routing internal database commands."

  @enforce_keys [:class]
  defstruct [
    :class,
    :source_database_uuid,
    :shadow_database_uuid,
    :generation,
    :operation_id
  ]

  @type class :: :public | :shadow_read | :shadow_replication | :shadow_control
  @type t :: %__MODULE__{
          class: class(),
          source_database_uuid: binary() | nil,
          shadow_database_uuid: binary() | nil,
          generation: non_neg_integer() | nil,
          operation_id: binary() | nil
        }

  @doc "Creates the context used by public callers."
  @spec public() :: t()
  def public, do: %__MODULE__{class: :public}

  @doc "Creates a read-only shadow context."
  @spec shadow_read(keyword()) :: t()
  def shadow_read(opts \\ []), do: build(:shadow_read, opts)

  @doc "Creates the context used by the shadow replication worker."
  @spec shadow_replication(keyword()) :: t()
  def shadow_replication(opts \\ []), do: build(:shadow_replication, opts)

  @doc "Creates the context used by shadow control-plane operations."
  @spec shadow_control(keyword()) :: t()
  def shadow_control(opts \\ []), do: build(:shadow_control, opts)

  @spec shadow?(t()) :: boolean()
  def shadow?(%__MODULE__{class: class}), do: class != :public

  defp build(class, opts) when is_atom(class) and is_list(opts) do
    %__MODULE__{
      class: class,
      source_database_uuid: Keyword.get(opts, :source_database_uuid),
      shadow_database_uuid: Keyword.get(opts, :shadow_database_uuid),
      generation: Keyword.get(opts, :generation),
      operation_id: Keyword.get(opts, :operation_id)
    }
  end
end
