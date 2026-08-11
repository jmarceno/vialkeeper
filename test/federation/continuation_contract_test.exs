defmodule ElixirDB.Federation.ContinuationContractTest do
  @moduledoc "Covers the query inputs bound into federation continuations."

  use ExUnit.Case, async: true

  alias ElixirDB.Error
  alias ElixirDB.Federation.Executor

  @source "123e4567-e89b-12d3-a456-426614174000"
  @other_source "123e4567-e89b-12d3-a456-426614174001"

  test "changing any fingerprint-bound query input invalidates a continuation" do
    fetcher = fn _source_uuid, _request, _deadline ->
      {:ok,
       %{
         documents: [document("a", 1), document("b", 2)],
         sequence: 1,
         has_more: false
       }}
    end

    request = %{
      databases: [@source, @other_source],
      query: %{
        selector: %{"/kind" => "task"},
        fields: ["/value"],
        sort: [%{path: "/value", direction: "asc"}],
        limit: 1
      }
    }

    assert {:ok, first} = Executor.run(request, source_fetcher: fetcher, max_candidates: 4)
    assert is_binary(first.bookmark)

    variants = [
      Map.put(request, :databases, [@other_source]),
      put_in(request, [:query, :selector], %{"/kind" => "other"}),
      put_in(request, [:query, :fields], ["/other"]),
      put_in(request, [:query, :sort], [%{path: "/value", direction: "desc"}]),
      put_in(request, [:query, :limit], 2)
    ]

    for variant <- variants do
      continued = put_in(variant, [:query, :bookmark], first.bookmark)

      assert {:error, %Error{code: :invalid_bookmark}} =
               Executor.run(continued, source_fetcher: fetcher, max_candidates: 4)
    end
  end

  defp document(id, value),
    do: %{id: id, revision: "revision-" <> id, body: %{"kind" => "task", "value" => value}}
end
