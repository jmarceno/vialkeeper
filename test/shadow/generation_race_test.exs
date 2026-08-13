defmodule ElixirDB.Shadow.GenerationRaceTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Shadow.{ReadRouter, RouteTable}
  alias ElixirDB.Storage.Results

  defmodule ProbeEndpoint do
    defstruct [:mode]

    def read_document(%__MODULE__{mode: :error}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.database_unavailable("stale generation")}

    def read_document(%__MODULE__{mode: :miss}, _request, _timeout, _opts),
      do: {:error, ElixirDB.Error.document_not_found("document not found")}
  end

  test "an old failed request cannot delete a newer route" do
    {source_uuid, path} = ElixirDB.ShadowSource.open!("shadow-race")
    old = snapshot(source_uuid, 1, %ProbeEndpoint{mode: :error})
    new = snapshot(source_uuid, 2, %ProbeEndpoint{mode: :miss})
    assert :ok = RouteTable.put(source_uuid, new)
    assert :stale = RouteTable.compare_delete(source_uuid, old)
    assert {:ok, ^new} = RouteTable.get(source_uuid)
    ElixirDB.ShadowSource.close!(source_uuid, path)
  end

  test "a transport failure compare-deletes only that generation" do
    {source_uuid, path} = ElixirDB.ShadowSource.open!("shadow-race")
    snapshot = snapshot(source_uuid, 1, %ProbeEndpoint{mode: :error})
    assert :ok = RouteTable.put(source_uuid, snapshot)

    assert {:ok, %Results.GetDocument{}, %{served_by: "source"}} =
             ReadRouter.get(source_uuid, %{id: "doc"},
               primary: fn _ ->
                 {:ok, Results.get_document(%{id: "doc", revision: "rev", body: %{}})}
               end
             )

    assert :not_found = RouteTable.get(source_uuid)
    ElixirDB.ShadowSource.close!(source_uuid, path)
  end

  defp snapshot(source_uuid, generation, endpoint) do
    %{
      endpoint: endpoint,
      source_uuid: source_uuid,
      shadow_uuid: ElixirDB.UUID.v4(),
      generation: generation,
      operation_id: ElixirDB.UUID.v4()
    }
  end
end
