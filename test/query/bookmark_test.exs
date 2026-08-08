defmodule ElixirDB.Query.BookmarkTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Domain.Bookmark
  alias ElixirDB.Query.BookmarkCodec

  @plan_digest String.duplicate("a", 64)
  @first_digest String.duplicate("b", 64)
  @second_digest String.duplicate("c", 64)

  defp payload do
    %{
      "query_fingerprint" => "query-fingerprint",
      "plan_digest" => @plan_digest,
      "index_bindings" => [
        %{"index_id" => "idx-a", "definition_digest" => @first_digest},
        %{"index_id" => "idx-b", "definition_digest" => @second_digest}
      ],
      "sequence" => 42,
      "sort_direction" => "asc",
      "ordering_key" => %{"sort" => [], "id" => "doc-1"},
      "last_id" => "doc-1"
    }
  end

  test "encodes and decodes the plan-bound wire shape" do
    assert {:ok, encoded} = BookmarkCodec.encode(payload())

    assert {:ok, decoded} =
             BookmarkCodec.decode(encoded, %{"query_fingerprint" => "query-fingerprint"})

    assert decoded.plan_digest == @plan_digest

    assert decoded.index_bindings == [
             %{"index_id" => "idx-a", "definition_digest" => @first_digest},
             %{"index_id" => "idx-b", "definition_digest" => @second_digest}
           ]
  end

  test "rejects plan and binding shape violations" do
    assert {:ok, encoded} = BookmarkCodec.encode(Map.put(payload(), "plan_digest", "not-a-digest"))

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded)

    assert {:ok, encoded} =
             BookmarkCodec.encode(
               Map.put(payload(), "index_bindings", [
                 %{"index_id" => "idx-a", "definition_digest" => @first_digest},
                 %{"index_id" => "idx-a", "definition_digest" => @first_digest}
               ])
             )

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded)
  end

  test "rejects zero digests in the codec and domain value" do
    zero = String.duplicate("0", 64)

    assert {:ok, encoded} = BookmarkCodec.encode(Map.put(payload(), "plan_digest", zero))

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded)

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             Bookmark.new(%{
               version: 1,
               protocol_major: 1,
               query_fingerprint: "query-fingerprint",
               plan_digest: zero,
               index_bindings: [],
               sequence: 0,
               sort_direction: "asc",
               ordering_key: %{"sort" => [], "id" => "doc-1"},
               last_id: "doc-1",
               checksum: "checksum"
             })
  end

  test "requires 64 lowercase hexadecimal definition digests" do
    for digest <- ["", "abc", String.duplicate("A", 64), String.duplicate("0", 64)] do
      assert {:ok, encoded} =
               BookmarkCodec.encode(
                 put_in(payload(), ["index_bindings", Access.at(0), "definition_digest"], digest)
               )

      assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
               BookmarkCodec.decode(encoded)
    end
  end

  test "rejects checksum tampering, query mismatch, and legacy singular fields" do
    assert {:ok, encoded} = BookmarkCodec.encode(payload())
    last = String.last(encoded)
    replacement = if last == "A", do: "B", else: "A"
    tampered = String.slice(encoded, 0, byte_size(encoded) - 1) <> replacement

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} = BookmarkCodec.decode(tampered)

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded, %{"query_fingerprint" => "other"})

    assert {:ok, legacy} =
             BookmarkCodec.encode(%{
               "query_fingerprint" => "query-fingerprint",
               "index_id" => "idx-a",
               "index_digest" => @first_digest,
               "sequence" => 1,
               "last_id" => "doc-1"
             })

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} = BookmarkCodec.decode(legacy)
  end

  test "supports bounded-scan bookmarks with empty bindings" do
    bounded =
      payload()
      |> Map.put("plan_digest", String.duplicate("d", 64))
      |> Map.put("index_bindings", [])
      |> Map.put("ordering_key", ["ready", "doc-1"])

    assert {:ok, encoded} = BookmarkCodec.encode(bounded)
    assert {:ok, decoded} = BookmarkCodec.decode(encoded)
    assert decoded.index_bindings == []
    assert decoded.ordering_key == ["ready", "doc-1"]
  end

  test "supports rank in full-text ordering cursors" do
    ordering_key = %{"sort" => [], "rank" => -1.25, "id" => "doc-1"}

    assert {:ok, encoded} = BookmarkCodec.encode(Map.put(payload(), "ordering_key", ordering_key))
    assert {:ok, decoded} = BookmarkCodec.decode(encoded)
    assert decoded.ordering_key == ordering_key

    assert {:ok, encoded} =
             BookmarkCodec.encode(
               Map.put(payload(), "ordering_key", Map.put(ordering_key, "rank", "invalid"))
             )

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded)
  end

  test "rejects malformed ordering cursors" do
    assert {:ok, encoded} =
             BookmarkCodec.encode(Map.put(payload(), "ordering_key", %{"unexpected" => true}))

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(encoded)
  end

  test "preserves binding order in the authenticated wire payload" do
    reversed = Map.put(payload(), "index_bindings", Enum.reverse(payload()["index_bindings"]))
    assert {:ok, encoded} = BookmarkCodec.encode(reversed)
    assert {:ok, decoded} = BookmarkCodec.decode(encoded)
    assert hd(decoded.index_bindings)["index_id"] == "idx-b"
  end
end
