defmodule ElixirDB.TestSupport.AdmissionClassProbe do
  @moduledoc "Test helper for observing the service class of admitted commands."
  import ExUnit.Assertions

  @type grant :: {atom(), atom() | nil}

  @doc """
  Installs a probe that records each granted admission as
  `{ref, :admission_grant, class, op}` messages to `pid`.

  `op` is the owner command atom when the grant came from a catalog command path.
  """
  @spec install(pid()) :: reference()
  def install(pid \\ self()) do
    ref = make_ref()
    Application.put_env(:elixir_db, :admission_class_probe, {pid, ref})
    Application.put_env(:elixir_db, :read_pool_probe, {pid, ref})
    ref
  end

  @spec uninstall() :: :ok
  def uninstall do
    Application.delete_env(:elixir_db, :admission_class_probe)
    Application.delete_env(:elixir_db, :read_pool_probe)
    :ok
  end

  @doc """
  Runs `fun`, drains probe messages, and returns `{result, grants}`.
  """
  @spec collect(reference(), (-> term())) :: {term(), [grant()]}
  def collect(ref, fun) do
    result = fun.()
    {result, drain(ref)}
  end

  @spec drain(reference(), timeout()) :: [grant()]
  def drain(ref, timeout \\ 100) do
    drain(ref, [], timeout)
  end

  @doc """
  Drains probe messages until a grant matches `expected_class` / `expected_op` or
  `timeout` elapses.
  """
  @spec await_grant(reference(), atom(), atom(), timeout()) :: [grant()]
  def await_grant(ref, expected_class, expected_op, timeout) do
    await_grant(ref, expected_class, expected_op, [], timeout)
  end

  defp await_grant(_ref, _expected_class, _expected_op, acc, remaining) when remaining <= 0 do
    acc
  end

  defp await_grant(ref, expected_class, expected_op, acc, remaining) do
    grants = drain(ref, 0)
    acc = acc ++ grants

    if Enum.any?(acc, fn {class, op} -> class == expected_class and op == expected_op end) do
      acc ++ drain(ref, 100)
    else
      if remaining > 0 do
        Process.sleep(min(remaining, 50))
        await_grant(ref, expected_class, expected_op, acc, remaining - 50)
      else
        acc
      end
    end
  end

  defp drain(ref, acc, timeout) do
    drain_loop(ref, acc, timeout)
  end

  defp drain_loop(_ref, acc, 0), do: Enum.reverse(acc)

  defp drain_loop(ref, acc, remaining) do
    receive do
      {^ref, :admission_grant, class, op} -> drain_loop(ref, [{class, op} | acc], remaining)
      {^ref, :read_pool_grant, class, op} -> drain_loop(ref, [{class, op} | acc], remaining)
    after
      0 ->
        if remaining <= 0 do
          Enum.reverse(acc)
        else
          Process.sleep(min(remaining, 10))
          drain_loop(ref, acc, remaining - 10)
        end
    end
  end

  @doc """
  Asserts every grant during `fun` uses `expected` and none use a `forbidden` class.
  """
  @spec assert_only!(reference(), atom(), [atom()], (-> term())) :: term()
  def assert_only!(ref, expected, forbidden \\ [], fun) when is_function(fun, 0) do
    {result, grants} = collect(ref, fun)
    assert_grants_match!(grants, expected, forbidden, nil, true)
    result
  end

  @doc """
  Asserts every grant during `fun` uses `expected_class` with `expected_op`, and none
  use a `forbidden` class.
  """
  @spec assert_op_only!(reference(), atom(), atom(), [atom()], (-> term())) :: term()
  def assert_op_only!(ref, expected_class, expected_op, forbidden \\ [], fun)
      when is_function(fun, 0) do
    {result, grants} = collect(ref, fun)
    assert_grants_match!(grants, expected_class, forbidden, expected_op, true)
    result
  end

  @doc """
  Asserts at least one grant matches `expected_class` and `expected_op`, every grant
  avoids `forbidden` classes, and when `only: true` every grant uses `expected_class`.
  """
  @spec assert_includes_op!(
          reference(),
          atom(),
          atom(),
          [atom()],
          (-> term()),
          keyword()
        ) :: term()
  def assert_includes_op!(ref, expected_class, expected_op, forbidden, fun, opts \\ [])
      when is_function(fun, 0) do
    only? = Keyword.get(opts, :only, false)
    {result, grants} = collect(ref, fun)
    assert_grants_match!(grants, expected_class, forbidden, expected_op, only?)
    result
  end

  @doc """
  Asserts a collected grant list matches the expected class/op constraints.
  """
  @spec assert_grants!([grant()], atom(), [atom()], atom() | nil, boolean()) :: :ok
  def assert_grants!(grants, expected_class, forbidden, expected_op, only?) do
    assert_grants_match!(grants, expected_class, forbidden, expected_op, only?)
  end

  @doc """
  Asserts every grant whose op is in `trusted_ops` uses `expected_class` and none use
  `forbidden_classes`. At least one trusted op grant must appear.
  """
  @spec assert_trusted_ops!([grant()], [atom()], atom(), [atom()]) :: :ok
  def assert_trusted_ops!(grants, trusted_ops, expected_class, forbidden_classes) do
    trusted_grants = Enum.filter(grants, fn {_class, op} -> op in trusted_ops end)

    if trusted_grants == [] do
      flunk("expected grants for ops #{inspect(trusted_ops)} in #{inspect(grants)}")
    end

    for {class, op} <- trusted_grants do
      if class in forbidden_classes do
        flunk(
          "forbidden admission class #{inspect(class)} for op #{inspect(op)} in #{inspect(grants)}"
        )
      end

      if class != expected_class do
        flunk(
          "admission class #{inspect(class)} for op #{inspect(op)} does not match expected #{inspect(expected_class)}; got #{inspect(grants)}"
        )
      end
    end

    :ok
  end

  defp assert_grants_match!(grants, expected_class, forbidden, expected_op, only?) do
    assert_no_forbidden_classes!(grants, expected_class, forbidden)
    if only?, do: assert_all_same_class!(grants, expected_class)
    assert_expected_grant!(grants, expected_class, expected_op)
  end

  defp assert_no_forbidden_classes!(grants, expected_class, forbidden) do
    Enum.each(grants, fn {class, _op} ->
      if class in forbidden do
        flunk(
          "forbidden admission class #{inspect(class)} in #{inspect(grants)}; expected #{inspect(expected_class)} without #{inspect(forbidden)}"
        )
      end
    end)
  end

  defp assert_all_same_class!(grants, expected_class) do
    Enum.each(grants, fn {class, op} ->
      if class != expected_class do
        flunk(
          "admission class #{inspect(class)} (op #{inspect(op)}) does not match expected #{inspect(expected_class)}; got #{inspect(grants)}"
        )
      end
    end)
  end

  defp assert_expected_grant!(grants, expected_class, nil) do
    if grants == [] do
      flunk("expected at least one admission grant for #{inspect(expected_class)}")
    end
  end

  defp assert_expected_grant!(grants, expected_class, expected_op) do
    if Enum.any?(grants, fn {class, op} -> class == expected_class and op == expected_op end) do
      :ok
    else
      flunk(
        "expected grant #{inspect(expected_class)} / #{inspect(expected_op)} in #{inspect(grants)}"
      )
    end
  end
end
