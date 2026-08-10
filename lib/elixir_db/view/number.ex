defmodule ElixirDB.View.Number do
  @moduledoc """
  Shared finite binary64 helpers for view key encoding and numeric reducers.
  """

  @spec normalize_zero(number()) :: number()
  def normalize_zero(value) when is_float(value) and value + 0.0 == 0.0, do: 0.0
  def normalize_zero(value), do: value

  @spec finite?(number()) :: boolean()
  def finite?(value) when is_integer(value), do: true

  def finite?(value) when is_float(value) do
    not (value !== value) and abs(value) <= Float.max_finite()
  end

  def finite?(_), do: false

  @doc """
  Converts an accepted number to a finite IEEE-754 binary64 value.

  Oversized integers and non-finite floats return `:overflow` instead of raising.
  """
  @spec to_binary64(number()) :: {:ok, float()} | :overflow
  def to_binary64(value) when is_float(value) do
    value = normalize_zero(value)

    if finite?(value), do: {:ok, value}, else: :overflow
  end

  def to_binary64(value) when is_integer(value) do
    float = value * 1.0

    if finite?(float), do: {:ok, normalize_zero(float)}, else: :overflow
  rescue
    ArithmeticError -> :overflow
  end

  def to_binary64(_), do: :overflow
end
