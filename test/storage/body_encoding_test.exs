defmodule VialKeeper.Storage.BodyEncodingTest do
  @moduledoc "Covers how often a write encodes a document body as canonical JSON."
  # Uses VM-global call tracing, so this module does not run async.
  use ExUnit.Case, async: false

  alias VialKeeper.JSON.Canonical
  alias VialKeeper.Storage.Services
  alias VialKeeper.Storage.SQLite.Adapter

  setup do
    {:ok, adapter} = Adapter.create(":memory:", %{storage_mode: :memory})
    on_exit(fn -> Adapter.close(adapter) end)
    %{context: Adapter.to_context(adapter)}
  end

  test "a bulk write without body JSON encodes each body exactly once", %{context: context} do
    bodies = for i <- 1..5, do: %{"n" => i, "title" => "Document #{i}", "tags" => ["a", "b"]}

    operations =
      bodies
      |> Enum.with_index(1)
      |> Enum.map(fn {body, i} -> %{operation: :put, document_id: "doc-#{i}", body: body} end)

    {result, calls} =
      VialKeeper.CallTrace.run([{Canonical, :encode, 1}], fn ->
        Services.apply_bulk_mutation(context, %{operations: operations})
      end)

    assert {:ok, [_, _, _, _, _]} = result
    assert body_encodes(calls, bodies) == Map.new(bodies, &{&1, 1})
  end

  test "a write that supplies its body JSON never re-encodes the body", %{context: context} do
    body = %{"n" => 1, "nested" => %{"x" => [1, 2, 3]}}
    {:ok, body_json} = Canonical.encode(body)

    {result, calls} =
      VialKeeper.CallTrace.run([{Canonical, :encode, 1}], fn ->
        Services.apply_bulk_mutation(context, %{
          operations: [%{operation: :put, document_id: "doc", body: body, body_json: body_json}]
        })
      end)

    assert {:ok, [_]} = result
    assert body_encodes(calls, [body]) == %{}

    assert {:ok, %{body: ^body}} = Services.get_document(context, %{document_id: "doc"})
  end

  # Counts encode calls whose argument is, or embeds, one of `bodies`.
  defp body_encodes(calls, bodies) do
    for {Canonical, :encode, [value]} <- calls,
        body <- bodies,
        contains?(value, body),
        reduce: %{} do
      acc -> Map.update(acc, body, 1, &(&1 + 1))
    end
  end

  defp contains?(value, value), do: true
  defp contains?(%Canonical.Fragment{}, _body), do: false

  defp contains?(value, body) when is_map(value),
    do: Enum.any?(value, fn {_k, v} -> contains?(v, body) end)

  defp contains?(value, body) when is_list(value), do: Enum.any?(value, &contains?(&1, body))
  defp contains?(_value, _body), do: false
end
