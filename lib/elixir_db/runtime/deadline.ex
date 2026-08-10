defmodule ElixirDB.Runtime.Deadline do
  @moduledoc "Monotonic deadline and remaining-time helpers for database operations."

  @type t :: integer() | :infinity
  @type deadline_timeout :: non_neg_integer() | :infinity

  @spec from_timeout(deadline_timeout()) :: t()
  def from_timeout(:infinity), do: :infinity

  def from_timeout(timeout) when is_integer(timeout) and timeout >= 0 do
    System.monotonic_time(:millisecond) + timeout
  end

  @spec remaining(t()) :: non_neg_integer() | :infinity
  def remaining(:infinity), do: :infinity

  def remaining(deadline_ms) when is_integer(deadline_ms) do
    max(deadline_ms - System.monotonic_time(:millisecond), 0)
  end

  @spec exhausted?(t()) :: boolean()
  def exhausted?(:infinity), do: false

  def exhausted?(deadline_ms) when is_integer(deadline_ms) do
    remaining(deadline_ms) <= 0
  end

  @spec call_timeout(t()) :: deadline_timeout()
  def call_timeout(:infinity), do: :infinity
  def call_timeout(deadline_ms) when is_integer(deadline_ms), do: remaining(deadline_ms)

  @spec genserver_call_timeout?(term()) :: boolean()
  def genserver_call_timeout?(:timeout), do: true
  def genserver_call_timeout?({:timeout, {GenServer, :call, _}}), do: true
  def genserver_call_timeout?(_), do: false
end
