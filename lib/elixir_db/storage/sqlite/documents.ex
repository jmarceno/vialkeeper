defmodule ElixirDB.Storage.SQLite.Documents do
  @moduledoc false
  def get(adapter, request), do: ElixirDB.Storage.SQLite.Adapter.get_document(adapter, request)

  def put(adapter, request),
    do:
      ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(
        adapter,
        Map.put(request, :operation, :put)
      )

  def delete(adapter, request),
    do:
      ElixirDB.Storage.SQLite.Adapter.apply_local_mutation(
        adapter,
        Map.put(request, :operation, :delete)
      )
end
