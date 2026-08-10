defmodule ElixirDB.Runtime.AdmissionPolicy do
  @moduledoc false

  alias ElixirDB.Runtime.ServiceClass

  @enforce_keys [
    :foreground_weight,
    :subscription_weight,
    :replication_weight,
    :maintenance_weight,
    :foreground_reserved_slots,
    :subscription_reserved_slots,
    :replication_reserved_slots,
    :maintenance_reserved_slots
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          foreground_weight: pos_integer(),
          subscription_weight: pos_integer(),
          replication_weight: pos_integer(),
          maintenance_weight: pos_integer(),
          foreground_reserved_slots: non_neg_integer(),
          subscription_reserved_slots: non_neg_integer(),
          replication_reserved_slots: non_neg_integer(),
          maintenance_reserved_slots: non_neg_integer()
        }

  @weight_keys [
    :foreground_weight,
    :subscription_weight,
    :replication_weight,
    :maintenance_weight
  ]

  @reserved_keys [
    :foreground_reserved_slots,
    :subscription_reserved_slots,
    :replication_reserved_slots,
    :maintenance_reserved_slots
  ]

  @allowed_keys @weight_keys ++ @reserved_keys

  @min_weight 1
  @max_weight 64

  @spec allowed_keys() :: [atom()]
  def allowed_keys, do: @allowed_keys

  @spec default_keyword() :: keyword()
  def default_keyword do
    [
      foreground_weight: 8,
      subscription_weight: 4,
      replication_weight: 2,
      maintenance_weight: 1,
      foreground_reserved_slots: 1,
      subscription_reserved_slots: 1,
      replication_reserved_slots: 1,
      maintenance_reserved_slots: 1
    ]
  end

  @spec default_toml_map() :: %{String.t() => integer()}
  def default_toml_map do
    Map.new(default_keyword(), fn {key, value} -> {atom_to_toml_key(key), value} end)
  end

  @spec from_keyword(keyword(), pos_integer()) :: {:ok, t()} | {:error, String.t()}
  def from_keyword(keyword, admission_limit)
      when is_list(keyword) and is_integer(admission_limit) and admission_limit > 0 do
    with :ok <- validate_unique_keys(keyword),
         :ok <- validate_keys(keyword),
         :ok <- validate_weights(keyword),
         :ok <- validate_reserved_slots(keyword, admission_limit) do
      {:ok, struct!(__MODULE__, Map.new(keyword))}
    end
  end

  @spec validate_reserved_slots(keyword(), pos_integer()) :: :ok | {:error, String.t()}
  def validate_reserved_slots(keyword, admission_limit)
      when is_list(keyword) and is_integer(admission_limit) and admission_limit > 0 do
    with :ok <- validate_reserved_ranges(keyword, admission_limit),
         do: validate_reserved_sum(keyword, admission_limit)
  end

  @weight_fields %{
    foreground: :foreground_weight,
    subscription: :subscription_weight,
    replication: :replication_weight,
    maintenance: :maintenance_weight
  }

  @reserved_fields %{
    foreground: :foreground_reserved_slots,
    subscription: :subscription_reserved_slots,
    replication: :replication_reserved_slots,
    maintenance: :maintenance_reserved_slots
  }

  @spec weights(t()) :: %{ServiceClass.t() => pos_integer()}
  def weights(%__MODULE__{} = policy) do
    class_field_map(policy, @weight_fields)
  end

  @spec reserved_slots(t()) :: %{ServiceClass.t() => non_neg_integer()}
  def reserved_slots(%__MODULE__{} = policy) do
    class_field_map(policy, @reserved_fields)
  end

  defp class_field_map(%__MODULE__{} = policy, field_by_class) do
    Map.new(field_by_class, fn {class, field} -> {class, Map.fetch!(policy, field)} end)
  end

  defp validate_unique_keys(keyword) do
    keys = Keyword.keys(keyword)

    case keys -- Enum.uniq(keys) do
      [] ->
        :ok

      [duplicate | _] ->
        {:error, "admission policy contains duplicate key #{inspect(duplicate)}"}
    end
  end

  defp validate_keys(keyword) do
    keys = Keyword.keys(keyword)

    case keys -- @allowed_keys do
      [] ->
        missing = @allowed_keys -- keys

        if missing == [] do
          :ok
        else
          {:error, "admission policy is missing required keys: #{inspect(missing)}"}
        end

      [unknown | _] ->
        {:error, "admission policy contains unknown key #{inspect(unknown)}"}
    end
  end

  defp validate_weights(keyword) do
    Enum.reduce_while(@weight_keys, :ok, fn key, :ok ->
      case Keyword.fetch(keyword, key) do
        {:ok, value} when is_integer(value) and value in @min_weight..@max_weight ->
          {:cont, :ok}

        {:ok, _} ->
          {:halt,
           {:error,
            "host.toml: admission.#{atom_to_toml_key(key)} must be an integer #{@min_weight}..#{@max_weight}"}}

        :error ->
          {:halt, {:error, "admission policy is missing required keys: [#{inspect(key)}]"}}
      end
    end)
  end

  defp validate_reserved_ranges(keyword, admission_limit) do
    Enum.reduce_while(@reserved_keys, :ok, fn key, :ok ->
      case Keyword.fetch(keyword, key) do
        {:ok, value} when is_integer(value) and value >= 0 and value <= admission_limit ->
          {:cont, :ok}

        {:ok, _} ->
          {:halt,
           {:error,
            "host.toml: admission.#{atom_to_toml_key(key)} must be an integer 0..#{admission_limit}"}}

        :error ->
          {:halt, {:error, "admission policy is missing required keys: [#{inspect(key)}]"}}
      end
    end)
  end

  defp validate_reserved_sum(keyword, admission_limit) do
    sum =
      Enum.reduce(@reserved_keys, 0, fn key, acc ->
        acc + Keyword.fetch!(keyword, key)
      end)

    if sum <= admission_limit,
      do: :ok,
      else:
        {:error,
         "host.toml: sum of admission reserved slots (#{sum}) must be at most limits.admission_limit (#{admission_limit})"}
  end

  defp atom_to_toml_key(:foreground_weight), do: "foreground_weight"
  defp atom_to_toml_key(:subscription_weight), do: "subscription_weight"
  defp atom_to_toml_key(:replication_weight), do: "replication_weight"
  defp atom_to_toml_key(:maintenance_weight), do: "maintenance_weight"
  defp atom_to_toml_key(:foreground_reserved_slots), do: "foreground_reserved_slots"
  defp atom_to_toml_key(:subscription_reserved_slots), do: "subscription_reserved_slots"
  defp atom_to_toml_key(:replication_reserved_slots), do: "replication_reserved_slots"
  defp atom_to_toml_key(:maintenance_reserved_slots), do: "maintenance_reserved_slots"
end
