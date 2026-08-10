defmodule ElixirDB.Runtime.DeadlineTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Runtime.Deadline

  test "from_timeout maps :infinity to :infinity" do
    assert Deadline.from_timeout(:infinity) == :infinity
  end

  test "from_timeout maps finite timeout to monotonic deadline" do
    before = System.monotonic_time(:millisecond)
    deadline = Deadline.from_timeout(1_000)
    after_ms = System.monotonic_time(:millisecond)

    assert deadline >= before + 1_000
    assert deadline <= after_ms + 1_000
  end

  test "remaining is :infinity for infinite deadlines" do
    assert Deadline.remaining(:infinity) == :infinity
    refute Deadline.exhausted?(:infinity)
    assert Deadline.call_timeout(:infinity) == :infinity
  end

  test "remaining and exhaustion for finite deadlines" do
    deadline = System.monotonic_time(:millisecond) + 5_000

    assert Deadline.remaining(deadline) > 0
    refute Deadline.exhausted?(deadline)
    assert Deadline.call_timeout(deadline) == Deadline.remaining(deadline)

    past = System.monotonic_time(:millisecond) - 1
    assert Deadline.remaining(past) == 0
    assert Deadline.exhausted?(past)
  end
end
