defmodule ElixirDB.Revisions.Tree do
  alias ElixirDB.Domain.Revision
  alias ElixirDB.Revisions.Winner

  @spec leaves([Revision.t()]) :: [Revision.t()]
  def leaves(revisions) do
    parents = revisions |> Enum.map(& &1.parent_revision) |> Enum.reject(&is_nil/1) |> MapSet.new()
    Enum.filter(revisions, &(!MapSet.member?(parents, &1.revision_id)))
  end

  @spec add([Revision.t()], Revision.t()) :: {:ok, [Revision.t()]} | {:error, ElixirDB.Error.t()}
  def add(revisions, %Revision{} = revision) do
    case Enum.find(revisions, &(&1.revision_id == revision.revision_id)) do
      nil -> {:ok, [revision | revisions]}
      ^revision -> {:ok, revisions}
      _ -> {:error, ElixirDB.Error.integrity_violation("revision id has different content")}
    end
  end

  @spec projection([Revision.t()]) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def projection(revisions) do
    with leaves when leaves != [] <- leaves(revisions), {:ok, winner} <- Winner.select(leaves) do
      {:ok,
       %{
         winner: winner,
         leaves: leaves,
         live_leaves: Winner.live_leaves(leaves),
         conflicts: Winner.conflicts(leaves, winner)
       }}
    else
      _ -> {:error, ElixirDB.Error.document_not_found("revision tree is empty")}
    end
  end
end
