defmodule ElixirDB.Revisions.Winner do
  @moduledoc "Deterministic winner and conflict selection for revision leaves."

  alias ElixirDB.Domain.Revision

  @spec select([Revision.t()]) :: {:ok, Revision.t()} | {:error, ElixirDB.Error.t()}
  def select([]), do: {:error, ElixirDB.Error.document_not_found("document has no revision leaves")}

  def select(leaves) when is_list(leaves) do
    {:ok,
     Enum.max_by(leaves, fn %Revision{revision_id: id, generation: generation, deleted: deleted} ->
       {if(deleted, do: 0, else: 1), generation, digest(id)}
     end)}
  end

  def live_leaves(leaves), do: Enum.reject(leaves, & &1.deleted)

  def conflicts(leaves, winner) do
    leaves
    |> live_leaves()
    |> Enum.reject(&(&1.revision_id == winner.revision_id))
    |> Enum.map(& &1.revision_id)
    |> Enum.sort()
  end

  defp digest(id), do: id |> String.split("-", parts: 2) |> List.last()
end
