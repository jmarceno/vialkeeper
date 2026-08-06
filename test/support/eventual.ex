defmodule ElixirDB.Eventual do
  @moduledoc """
  Deterministic wait/poll helpers for asynchronous test conditions.

  Prefer these over fixed `Process.sleep/1` calls: polls until a condition
  succeeds or a deadline elapses, keeping failure messages actionable.
  """

  import ExUnit.Assertions

  @type condition :: (-> boolean() | {:ok, term()} | {:error, term()} | term())

  @doc """
  Polls `fun` until it returns a truthy value, `{:ok, value}`, or the timeout elapses.

  Options:
  * `:timeout` — milliseconds before failure (default `5_000`)
  * `:interval` — milliseconds between polls (default `10`)
  * `:message` — assertion message on timeout
  """
  @spec eventually(condition(), keyword()) :: term()
  def eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    interval = Keyword.get(opts, :interval, 10)
    message = Keyword.get(opts, :message, "condition was not met before timeout")
    deadline = System.monotonic_time(:millisecond) + timeout

    case poll(fun, interval, deadline) do
      {:ok, value} ->
        value

      :timeout ->
        flunk("#{message} (timeout=#{timeout}ms)")
    end
  end

  @doc """
  Polls until `fun` returns a truthy value without asserting; returns `:ok` or `:timeout`.
  """
  @spec await(condition(), keyword()) :: :ok | {:ok, term()} | :timeout
  def await(fun, opts \\ []) when is_function(fun, 0) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    interval = Keyword.get(opts, :interval, 10)
    deadline = System.monotonic_time(:millisecond) + timeout

    case poll(fun, interval, deadline) do
      {:ok, true} -> :ok
      {:ok, value} -> {:ok, value}
      :timeout -> :timeout
    end
  end

  @doc """
  Polls until `fun.()` equals `expected`.
  """
  @spec eventually_equal(term(), (-> term()), keyword()) :: term()
  def eventually_equal(expected, fun, opts \\ []) when is_function(fun, 0) do
    message =
      Keyword.get(opts, :message, "expected value #{inspect(expected)} was not observed in time")

    eventually(
      fn ->
        case fun.() do
          ^expected -> {:ok, expected}
          _other -> false
        end
      end,
      Keyword.put(opts, :message, message)
    )
  end

  defp poll(fun, interval, deadline) do
    case normalize(fun.()) do
      {:done, value} ->
        {:ok, value}

      :continue ->
        if System.monotonic_time(:millisecond) >= deadline do
          :timeout
        else
          Process.sleep(interval)
          poll(fun, interval, deadline)
        end
    end
  end

  defp normalize(true), do: {:done, true}
  defp normalize({:ok, value}), do: {:done, value}
  defp normalize(false), do: :continue
  defp normalize(nil), do: :continue
  defp normalize({:error, _}), do: :continue
  defp normalize(other), do: {:done, other}
end
