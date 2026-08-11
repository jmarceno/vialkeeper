defmodule ElixirDB.Federation.BookmarkCodecTest do
  @moduledoc "Covers signed federation bookmark encoding and validation."

  use ExUnit.Case, async: true

  alias ElixirDB.Federation.BookmarkCodec
  alias ElixirDB.JSON.{Canonical, StrictDecoder}

  @source "123e4567-e89b-12d3-a456-426614174000"
  @other_source "123e4567-e89b-12d3-a456-426614174001"

  defp payload do
    %{
      "query_fingerprint" => String.duplicate("a", 64),
      "sources" => [
        %{"database_uuid" => @source, "sequence" => 0},
        %{"database_uuid" => @other_source, "sequence" => 1}
      ],
      "sort" => [%{"path" => "/name", "direction" => "asc"}],
      "ordering_key" => ["ready", "doc-1"],
      "last_source_uuid" => @other_source,
      "last_document_id" => "doc-1"
    }
  end

  test "round trips and validates expected fingerprint and source vector" do
    assert {:ok, encoded} = BookmarkCodec.encode(payload())

    assert {:ok, decoded} =
             BookmarkCodec.decode(encoded, %{"query_fingerprint" => payload()["query_fingerprint"]})

    assert decoded["sources"] == payload()["sources"]

    assert invalid(BookmarkCodec.decode(encoded, %{"query_fingerprint" => "other"}))

    assert invalid(
             BookmarkCodec.decode(encoded, %{"sources" => Enum.reverse(payload()["sources"])})
           )
  end

  test "rejects expected sort and ordering key mismatches" do
    assert {:ok, encoded} = BookmarkCodec.encode(payload())
    assert invalid(BookmarkCodec.decode(encoded, %{"sort" => []}))
    assert invalid(BookmarkCodec.decode(encoded, %{"ordering_key" => ["other", "doc-1"]}))
  end

  test "preserves arbitrary nonnegative source sequence order" do
    value =
      payload()
      |> Map.put("sources", [
        %{"database_uuid" => @source, "sequence" => 42},
        %{"database_uuid" => @other_source, "sequence" => 9}
      ])

    assert {:ok, encoded} = BookmarkCodec.encode(value)
    assert {:ok, decoded} = BookmarkCodec.decode(encoded)
    assert decoded["sources"] == value["sources"]
  end

  test "rejects checksum, version, and protocol tampering" do
    assert {:ok, encoded} = BookmarkCodec.encode(payload())
    {:ok, map} = StrictDecoder.decode(Base.url_decode64!(encoded, padding: false))

    tampered =
      Base.url_encode64(Canonical.encode!(Map.put(map, "checksum", "bad")),
        padding: false
      )

    assert invalid(BookmarkCodec.decode(tampered))

    for key <- ["version", "protocol_major"] do
      altered = Base.url_decode64!(encoded, padding: false) |> replace_json_field(key, 999)
      assert invalid(BookmarkCodec.decode(altered))
    end
  end

  test "rejects malformed source vectors and cursor shapes" do
    for change <- [
          %{"sources" => []},
          %{
            "sources" => [
              %{"database_uuid" => @source, "sequence" => 0, "extra" => true},
              %{"database_uuid" => @other_source, "sequence" => 1}
            ]
          },
          %{"sources" => [%{"database_uuid" => @source, "sequence" => -1}]},
          %{
            "sources" => [
              %{"database_uuid" => @source, "sequence" => 0},
              %{"database_uuid" => @source, "sequence" => 1}
            ]
          },
          %{"last_source_uuid" => "not-a-uuid"},
          %{"ordering_key" => %{"unexpected" => true}},
          %{"last_document_id" => ""}
        ] do
      assert {:ok, encoded} = BookmarkCodec.encode(Map.merge(payload(), change))
      assert invalid(BookmarkCodec.decode(encoded))
    end
  end

  defp invalid({:error, %ElixirDB.Error{code: :invalid_bookmark}}), do: true

  defp replace_json_field(bytes, key, value) do
    {:ok, map} = StrictDecoder.decode(bytes)

    {:ok, unsigned} =
      Canonical.encode(Map.put(Map.delete(map, "checksum"), key, value))

    checksum = :crypto.hash(:sha256, unsigned) |> Base.encode16(case: :lower)

    Base.url_encode64(
      Canonical.encode!(
        Map.put(Map.delete(map, "checksum"), key, value)
        |> Map.put("checksum", checksum)
      ),
      padding: false
    )
  end
end
