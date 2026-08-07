defmodule ElixirDB.Runtime.ChildSpec do
  @moduledoc "Common child-spec maps for runtime supervisors and workers."

  @spec worker(term(), tuple(), atom()) :: map()
  def worker(id, start, restart), do: base(id, start, restart, :worker)

  @spec supervisor(term(), tuple(), atom()) :: map()
  def supervisor(id, start, restart), do: base(id, start, restart, :supervisor)

  defp base(id, start, restart, type), do: %{id: id, start: start, restart: restart, type: type}
end
