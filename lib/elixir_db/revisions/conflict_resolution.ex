defmodule ElixirDB.Revisions.ConflictResolution do
  alias ElixirDB.Domain.Revision

  @spec validate_leaf_set([Revision.t()], [binary()]) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_leaf_set(current, expected) do
    actual = current |> Enum.reject(& &1.deleted) |> Enum.map(& &1.revision_id) |> Enum.sort()

    if actual == Enum.sort(expected),
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.revision_conflict("current live revision set differs", %{
           expected: expected,
           observed: actual
         })}
  end
end
