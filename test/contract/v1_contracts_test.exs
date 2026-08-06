defmodule ElixirDB.Contract.V1ContractsTest do
  use ExUnit.Case, async: true

  alias ElixirDB.JSON.{Canonical, Pointer, StrictDecoder}
  alias ElixirDB.Query.{BookmarkCodec, Normalizer}
  alias ElixirDB.Replication.Id

  test "strict JSON enforces duplicate keys, UTF-8, depth, and binary64 integers" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             StrictDecoder.decode(~s({"a":1,"a":2}))

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             StrictDecoder.decode(<<255>>)

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             StrictDecoder.decode("[[[0]]]", max_depth: 1)

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             StrictDecoder.decode("9007199254740992")

    assert {:ok, 0.000001} = StrictDecoder.decode("0.000001")
  end

  test "canonical JSON and JSON Pointer preserve the frozen identity rules" do
    assert {:ok, "{\"a\":1,\"b\":2}"} = Canonical.encode(%{"b" => 2, "a" => 1})
    assert {:ok, value} = Pointer.get(%{"a/b" => %{"~key" => 7}}, "/a~1b/~0key")
    assert value == 7
    assert {:error, %ElixirDB.Error{code: :invalid_request}} = Pointer.parse("/bad~2escape")
  end

  test "query normalization rejects unknown fields and unsupported operators" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{}, unexpected: true})

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{"/state" => %{"$where" => "x"}}})

    assert {:ok, normalized} =
             Normalizer.normalize(%{
               selector: %{"/state" => %{"$eq" => "ready"}},
               sort: [%{path: "/state", direction: "desc"}],
               limit: 10
             })

    assert is_binary(normalized.fingerprint)
  end

  test "bookmarks are checksummed and query-bound" do
    assert {:ok, bookmark} =
             BookmarkCodec.encode(%{
               "query_fingerprint" => "query",
               "sequence" => 4,
               "last_id" => "doc",
               "index_id" => "idx",
               "index_digest" => "digest",
               "ordering_key" => ["ready", "doc"]
             })

    assert {:ok, decoded} =
             BookmarkCodec.decode(bookmark, %{"query_fingerprint" => "query"})

    assert decoded.last_id == "doc"

    assert {:error, %ElixirDB.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(bookmark, %{"query_fingerprint" => "different"})
  end

  test "replication IDs include protocol identity and are deterministic" do
    assert {:ok, first} = Id.calculate("source", "target", "push", "one_shot")
    assert {:ok, second} = Id.calculate("source", "target", "push", "one_shot")
    assert first == second
    refute {:ok, first} == Id.calculate("source", "target", "pull", "one_shot")
  end

  test "endpoint URLs reject credentials, paths, and unknown fields" do
    uuid = ElixirDB.UUID.v4()

    assert {:ok, %{kind: :remote}} =
             ElixirDB.Domain.ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test"
             })

    for url <- ["https://user:pass@example.test", "https://example.test/db", "file:///tmp/db"] do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               ElixirDB.Domain.ReplicationEndpoint.new(%{
                 "kind" => "remote",
                 "database_uuid" => uuid,
                 "base_url" => url
               })
    end

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             ElixirDB.Domain.ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test",
               "headers" => %{}
             })
  end

  test "configuration and public errors retain their bounded stable contracts" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             ElixirDB.Config.merge_and_bound(%{"unknown" => true})

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             ElixirDB.Config.merge_and_bound(%{"queries" => %{"max_limit" => 501}})

    Enum.each(ElixirDB.Error.registry(), fn {code, {status, retryable}} ->
      error = ElixirDB.Error.new(code, "message")
      assert error.http_status == status
      assert error.retryable == retryable
      assert is_map(ElixirDB.Error.public(error))
    end)
  end

  test "unicode_words_v1 tokenization is deterministic" do
    assert ElixirDB.Query.FullText.tokens("École café 東京") == ["école", "cafe", "東京"]
    assert ElixirDB.Query.FullText.tokens("École café", :remove) == ["ecole", "cafe"]
  end
end
