defmodule VialKeeper.Query.NormalizerPropertiesTest do
  @moduledoc "Query normalization is stable for the same input and idempotent on its public fields."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Query.Normalizer

  property "normalizing the same request twice returns the same result" do
    check all(request <- query_request(), max_runs: 40) do
      assert {:ok, first} = Normalizer.normalize(request)
      assert {:ok, second} = Normalizer.normalize(request)
      assert first == second
    end
  end

  property "re-normalizing public fields preserves selector, sort, and fingerprint" do
    check all(request <- query_request(), max_runs: 40) do
      assert {:ok, first} = Normalizer.normalize(request)

      public = %{
        selector: first.selector,
        sort: first.sort,
        fields: first.fields,
        limit: first.limit,
        bookmark: first.bookmark,
        index: first.index,
        search: first.search
      }

      assert {:ok, second} = Normalizer.normalize(public)
      assert second.selector == first.selector
      assert second.sort == first.sort
      assert second.fingerprint == first.fingerprint
    end
  end

  defp query_request do
    StreamData.tuple({selector(), sort(), StreamData.integer(1..50)})
    |> StreamData.map(fn {selector, sort, limit} ->
      %{"selector" => selector, "sort" => sort, "limit" => limit}
    end)
  end

  defp selector do
    StreamData.map_of(json_pointer(), json_scalar(), max_length: 3)
  end

  defp sort do
    StreamData.list_of(
      StreamData.bind(json_pointer(), fn path ->
        StreamData.bind(StreamData.member_of(["asc", "desc"]), fn direction ->
          StreamData.constant(%{"path" => path, "direction" => direction})
        end)
      end),
      max_length: 2
    )
  end

  defp json_pointer do
    StreamData.map(StreamData.string([?a..?z], min_length: 1, max_length: 8), &("/" <> &1))
  end

  defp json_scalar do
    StreamData.one_of([
      StreamData.constant(nil),
      StreamData.boolean(),
      StreamData.integer(-1_000..1_000),
      StreamData.string(:alphanumeric, max_length: 16)
    ])
  end
end
