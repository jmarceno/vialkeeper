defmodule VialKeeper.Quality.ReachSmells.NoSleepInOtpCallbackTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Quality.ReachSmellCase
  alias VialKeeper.Quality.ReachSmells.NoSleepInOtpCallback

  test "flags sleep inside handle_call and allows sleep in ordinary functions" do
    bad =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def handle_call(:wait, _from, state) do
          Process.sleep(10)
          {:reply, :ok, state}
        end
      end
      """)

    timer =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def handle_info(:wait, state) do
          :timer.sleep(10)
          {:noreply, state}
        end
      end
      """)

    good =
      ReachSmellCase.parse!("""
      defmodule Sample do
        def wait, do: Process.sleep(10)
      end
      """)

    assert [%{kind: :vial_keeper_sleep_in_otp_callback}] =
             NoSleepInOtpCallback.findings(bad, "lib/sample.ex")

    assert [%{kind: :vial_keeper_sleep_in_otp_callback}] =
             NoSleepInOtpCallback.findings(timer, "lib/sample.ex")

    assert NoSleepInOtpCallback.findings(good, "lib/sample.ex") == []
  end
end
