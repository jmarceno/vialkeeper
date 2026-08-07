defmodule ElixirDB.OperationFixtures do
  @moduledoc "Shared operation-map constructors for model and adapter fixtures."

  @spec put(binary(), map(), binary() | atom() | nil) :: map()
  def put(document_id, body, if_revision),
    do: %{op: :put, document_id: document_id, body: body, if_revision: if_revision}
end
