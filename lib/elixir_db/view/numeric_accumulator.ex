defmodule ElixirDB.View.NumericAccumulator do
  @moduledoc """
  Exact order-independent dyadic accumulator for view numeric reducers.

  Values are tracked in integer units at `2^-1074` for sums and `2^-2148` for
  sum-of-squares. Public aggregates round to the nearest finite binary64 with
  ties-to-even.
  """

  alias ElixirDB.View.Number

  @sum_scale 1074
  @fraction_bits 52
  @mantissa_bits 53

  defstruct sum_units: 0, sumsqr_units: 0

  @type t :: %__MODULE__{sum_units: integer(), sumsqr_units: integer()}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec add(t(), number() | nil) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def add(acc, nil), do: {:ok, acc}

  def add(acc, value) when is_number(value) do
    case Number.to_binary64(value) do
      {:ok, float} ->
        sum_unit = value_units(float, @sum_scale)

        {:ok,
         %__MODULE__{
           sum_units: acc.sum_units + sum_unit,
           sumsqr_units: acc.sumsqr_units + sum_unit * sum_unit
         }}

      :overflow ->
        {:error, ElixirDB.Error.resource_limit("numeric value is not representable")}
    end
  end

  @spec remove(t(), number() | nil) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def remove(acc, nil), do: {:ok, acc}

  def remove(acc, value) when is_number(value) do
    case Number.to_binary64(value) do
      {:ok, float} ->
        sum_unit = value_units(float, @sum_scale)

        {:ok,
         %__MODULE__{
           sum_units: acc.sum_units - sum_unit,
           sumsqr_units: acc.sumsqr_units - sum_unit * sum_unit
         }}

      :overflow ->
        {:error, ElixirDB.Error.resource_limit("numeric value is not representable")}
    end
  end

  @spec sum(t()) :: {:ok, float()} | {:error, ElixirDB.Error.t()}
  def sum(acc), do: units_to_binary64(acc.sum_units, @sum_scale)

  @spec sumsqr(t()) :: {:ok, float()} | {:error, ElixirDB.Error.t()}
  def sumsqr(acc), do: units_to_binary64(acc.sumsqr_units, @sum_scale * 2)

  defp value_units(value, scale) when is_float(value) do
    if value + 0.0 == 0.0 do
      0
    else
      {sign, significand, exponent} = decompose(value)
      magnitude = Bitwise.bsl(significand, exponent + scale - @fraction_bits)
      if sign == 0, do: magnitude, else: -magnitude
    end
  end

  defp decompose(value) when is_float(value) and value + 0.0 == 0.0, do: {0, 0, 0}

  defp decompose(value) when is_float(value) do
    <<bits::64>> = <<value::float>>
    sign = Bitwise.bsr(bits, 63)
    exponent_bits = Bitwise.band(Bitwise.bsr(bits, 52), 0x7FF)
    fraction = Bitwise.band(bits, Bitwise.bsl(1, 52) - 1)

    cond do
      exponent_bits == 0 and fraction == 0 ->
        {sign, 0, 0}

      exponent_bits == 0 ->
        {sign, fraction, -1022}

      exponent_bits == 0x7FF ->
        raise ArgumentError, "non-finite number"

      true ->
        {sign, fraction + Bitwise.bsl(1, 52), exponent_bits - 1023}
    end
  end

  defp units_to_binary64(0, _scale), do: {:ok, 0.0}

  defp units_to_binary64(units, scale) when is_integer(units) do
    negative = units < 0
    absolute = if negative, do: -units, else: units

    case round_units_to_binary64(absolute, scale, negative) do
      {:ok, float} ->
        {:ok, float}

      :resource_limit ->
        {:error, ElixirDB.Error.resource_limit("numeric aggregate is not representable")}
    end
  end

  defp round_units_to_binary64(absolute, scale, negative)
       when is_integer(absolute) and is_integer(scale) do
    bit_len = integer_bit_length(absolute)
    exponent = bit_len - 1 - scale

    if exponent > 1023 do
      :resource_limit
    else
      shift = bit_len - @mantissa_bits
      {mantissa, guard, sticky} = split_mantissa(absolute, shift)
      rounded = round_mantissa(mantissa, guard, sticky)
      apply_rounding(rounded, exponent, negative)
    end
  end

  defp split_mantissa(absolute, shift)
       when is_integer(absolute) and is_integer(shift) and shift > 0 do
    dropped = Bitwise.band(absolute, Bitwise.bsl(1, shift) - 1)
    mantissa = Bitwise.bsr(absolute, shift)
    guard = Bitwise.band(Bitwise.bsr(absolute, shift - 1), 1) == 1
    sticky = Bitwise.band(dropped, Bitwise.bsl(1, shift - 1) - 1) != 0
    {mantissa, guard, sticky}
  end

  defp split_mantissa(absolute, shift)
       when is_integer(absolute) and is_integer(shift) and shift <= 0 do
    {Bitwise.bsl(absolute, -shift), false, false}
  end

  defp round_mantissa(mantissa, guard, sticky) do
    lsb = Bitwise.band(mantissa, 1) == 1

    if guard and (sticky or lsb), do: mantissa + 1, else: mantissa
  end

  defp apply_rounding(0, _exponent, _negative), do: {:ok, 0.0}

  defp apply_rounding(mantissa, exponent, negative) when is_integer(mantissa) do
    bit_len = integer_bit_length(mantissa)

    if bit_len > @mantissa_bits do
      apply_rounding(Bitwise.bsr(mantissa, 1), exponent + 1, negative)
    else
      encode_binary64(mantissa, exponent, negative)
    end
  end

  defp integer_bit_length(0), do: 1

  defp integer_bit_length(n) when is_integer(n) and n > 0 do
    do_integer_bit_length(n, 0) |> max(1)
  end

  defp do_integer_bit_length(0, acc), do: acc

  defp do_integer_bit_length(n, acc) when n > 0,
    do: do_integer_bit_length(Bitwise.bsr(n, 1), acc + 1)

  defp encode_binary64(mantissa, exponent, negative) do
    cond do
      exponent < -1074 ->
        :resource_limit

      exponent < -1022 ->
        encode_denormal(mantissa, exponent, negative)

      exponent > 1023 ->
        :resource_limit

      true ->
        encode_normal(mantissa, exponent, negative)
    end
  end

  defp encode_normal(mantissa, exponent, negative) do
    fraction = mantissa - Bitwise.bsl(1, 52)
    exponent_bits = exponent + 1023
    sign = if negative, do: 1, else: 0
    bits = Bitwise.bor(Bitwise.bor(Bitwise.bsl(sign, 63), Bitwise.bsl(exponent_bits, 52)), fraction)
    <<float::float>> = <<bits::64>>
    if Number.finite?(float), do: {:ok, float}, else: :resource_limit
  end

  defp encode_denormal(mantissa, exponent, negative) do
    shift = -1022 - exponent
    fraction = Bitwise.bsr(mantissa, shift)

    if fraction == 0 do
      :resource_limit
    else
      sign = if negative, do: 1, else: 0
      bits = Bitwise.bor(Bitwise.bsl(sign, 63), fraction)
      <<float::float>> = <<bits::64>>
      if Number.finite?(float), do: {:ok, float}, else: :resource_limit
    end
  end
end
