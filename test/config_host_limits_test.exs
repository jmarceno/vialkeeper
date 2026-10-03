defmodule VialKeeper.ConfigHostLimitsTest do
  @moduledoc "Covers reading host limits from the application environment."
  # Changes the global host limits, so this module does not run async.
  use ExUnit.Case, async: false

  alias VialKeeper.Config

  setup do
    previous = Application.fetch_env(:vial_keeper, :host_limits)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:vial_keeper, :host_limits, value)
        :error -> Application.delete_env(:vial_keeper, :host_limits)
      end
    end)
  end

  test "host limits follow every change to the environment" do
    base = Application.get_env(:vial_keeper, :host_limits, [])

    Application.put_env(:vial_keeper, :host_limits, Keyword.put(base, :max_bulk_operations, 7))
    assert Config.host_limits()[:max_bulk_operations] == 7
    assert Config.host_limits()[:max_bulk_operations] == 7

    Application.put_env(:vial_keeper, :host_limits, Keyword.put(base, :max_bulk_operations, 9))
    assert Config.host_limits()[:max_bulk_operations] == 9

    Application.delete_env(:vial_keeper, :host_limits)
    assert Config.host_limits() == %{}

    Application.put_env(:vial_keeper, :host_limits, base)
    assert Config.host_limits() == Map.new(base)
  end
end
