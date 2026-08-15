defmodule VialKeeper.Bench.Tmp do
  @moduledoc "Fresh temporary directories for data-backed benchmark tests."

  @spec dir(binary()) :: Path.t()
  def dir(prefix) when is_binary(prefix) do
    path =
      Path.join(
        System.tmp_dir!(),
        "vk-bench-#{prefix}-#{System.unique_integer([:positive])}-#{System.os_time(:nanosecond)}"
      )

    File.mkdir_p!(path)
    path
  end
end
