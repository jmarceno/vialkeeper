defmodule VialKeeper.Search.RebuildTimeoutTest do
  @moduledoc """
  Full-text rebuild duration is a host ceiling (`max_search_rebuild_ms`), not
  the 5s `GenServer.call/3` default and not `max_query_execution_ms`.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Config

  setup do
    previous = Application.get_env(:vial_keeper, :host_limits, [])

    on_exit(fn ->
      Application.put_env(:vial_keeper, :host_limits, previous)
    end)

    %{previous: previous}
  end

  test "search_rebuild_timeout_ms reads max_search_rebuild_ms", %{previous: previous} do
    Application.put_env(
      :vial_keeper,
      :host_limits,
      Keyword.put(previous, :max_search_rebuild_ms, 12_345)
    )

    assert Config.search_rebuild_timeout_ms() == 12_345
  end

  test "search_rebuild_timeout_ms uses the compiled floor when the key is absent", %{
    previous: previous
  } do
    Application.put_env(
      :vial_keeper,
      :host_limits,
      Keyword.delete(previous, :max_search_rebuild_ms)
    )

    assert Config.search_rebuild_timeout_ms() == 300_000
  end
end
