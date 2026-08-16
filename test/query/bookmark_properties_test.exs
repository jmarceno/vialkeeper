defmodule VialKeeper.Query.BookmarkPropertiesTest do
  @moduledoc "Query bookmark encode/decode preserves the represented cursor state."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Query.BookmarkCodec

  property "encode then decode restores fingerprint, bindings, sequence, and last_id" do
    check all(payload <- bookmark_payload(), max_runs: 40) do
      assert {:ok, encoded} = BookmarkCodec.encode(payload)

      assert {:ok, decoded} =
               BookmarkCodec.decode(encoded, %{
                 "query_fingerprint" => payload["query_fingerprint"]
               })

      assert decoded.query_fingerprint == payload["query_fingerprint"]
      assert decoded.plan_digest == payload["plan_digest"]
      assert decoded.index_bindings == payload["index_bindings"]
      assert decoded.sequence == payload["sequence"]
      assert decoded.sort_direction == payload["sort_direction"]
      assert decoded.last_id == payload["last_id"]
      assert decoded.ordering_key["id"] == payload["last_id"]
    end
  end

  defp bookmark_payload do
    StreamData.tuple({
      hex64(),
      index_bindings(),
      StreamData.string(:alphanumeric, min_length: 1, max_length: 24),
      StreamData.integer(0..10_000),
      StreamData.string(:alphanumeric, min_length: 1, max_length: 24),
      StreamData.member_of(["asc", "desc"])
    })
    |> StreamData.map(fn {plan_digest, bindings, fingerprint, sequence, last_id, direction} ->
      %{
        "query_fingerprint" => fingerprint,
        "plan_digest" => plan_digest,
        "index_bindings" => bindings,
        "sequence" => sequence,
        "sort_direction" => direction,
        "ordering_key" => %{"sort" => [], "id" => last_id},
        "last_id" => last_id
      }
    end)
  end

  defp index_bindings do
    StreamData.uniq_list_of(
      StreamData.bind(
        StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
        fn index_id ->
          StreamData.bind(hex64(), fn digest ->
            StreamData.constant(%{"index_id" => index_id, "definition_digest" => digest})
          end)
        end
      ),
      uniq_fun: & &1["index_id"],
      max_length: 3
    )
  end

  defp hex64 do
    StreamData.map(StreamData.binary(length: 32), fn bytes ->
      Base.encode16(bytes, case: :lower)
    end)
    |> StreamData.filter(&(&1 != String.duplicate("0", 64)))
  end
end
