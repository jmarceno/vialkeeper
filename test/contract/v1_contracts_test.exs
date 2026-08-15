defmodule VialKeeper.Contract.V1ContractsTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Domain.ReplicationEndpoint
  alias VialKeeper.JSON.{Canonical, Pointer, StrictDecoder}
  alias VialKeeper.Query.{BookmarkCodec, Normalizer}
  alias VialKeeper.Query.FullText
  alias VialKeeper.Replication.Id

  test "strict JSON enforces duplicate keys, UTF-8, depth, and binary64 integers" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             StrictDecoder.decode(~s({"a":1,"a":2}))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             StrictDecoder.decode(<<255>>)

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             StrictDecoder.decode("[[[0]]]", max_depth: 1)

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             StrictDecoder.decode("9007199254740992")

    assert {:ok, 0.000001} = StrictDecoder.decode("0.000001")
  end

  test "canonical JSON and JSON Pointer preserve the frozen identity rules" do
    assert {:ok, "{\"a\":1,\"b\":2}"} = Canonical.encode(%{"b" => 2, "a" => 1})
    assert {:ok, value} = Pointer.get(%{"a/b" => %{"~key" => 7}}, "/a~1b/~0key")
    assert value == 7
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Pointer.parse("/bad~2escape")
  end

  test "query normalization rejects unknown fields and unsupported operators" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             Normalizer.normalize(%{selector: %{}, unexpected: true})

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
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
               "plan_digest" => String.duplicate("a", 64),
               "index_bindings" => [
                 %{"index_id" => "idx", "definition_digest" => String.duplicate("b", 64)}
               ],
               "sequence" => 4,
               "last_id" => "doc",
               "ordering_key" => ["ready", "doc"]
             })

    assert {:ok, decoded} =
             BookmarkCodec.decode(bookmark, %{"query_fingerprint" => "query"})

    assert decoded.last_id == "doc"

    assert {:error, %VialKeeper.Error{code: :invalid_bookmark}} =
             BookmarkCodec.decode(bookmark, %{"query_fingerprint" => "different"})
  end

  test "replication IDs include protocol identity and are deterministic" do
    assert {:ok, first} = Id.calculate("source", "target", "push", "one_shot")
    assert {:ok, second} = Id.calculate("source", "target", "push", "one_shot")
    assert first == second
    refute {:ok, first} == Id.calculate("source", "target", "pull", "one_shot")
  end

  test "endpoint URLs reject credentials, paths, and unknown fields" do
    uuid = VialKeeper.UUID.v4()

    assert {:ok, %{kind: :remote}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test"
             })

    for url <- ["https://user:pass@example.test", "https://example.test/db", "file:///tmp/db"] do
      assert {:error, %VialKeeper.Error{code: :invalid_request}} =
               ReplicationEndpoint.new(%{
                 "kind" => "remote",
                 "database_uuid" => uuid,
                 "base_url" => url
               })
    end

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test",
               "headers" => %{}
             })
  end

  test "remote endpoints accept an auth_token; local endpoints reject it (AUTH-003)" do
    uuid = VialKeeper.UUID.v4()

    # Remote endpoint may carry an auth_token sibling of base_url.
    assert {:ok, %{kind: :remote, auth_token: "abc"}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test",
               "auth_token" => "abc"
             })

    # auth_token is optional on remote endpoints.
    assert {:ok, %{kind: :remote, auth_token: nil}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test"
             })

    # URL-embedded credentials remain rejected even with auth_token present.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://user:pass@example.test",
               "auth_token" => "abc"
             })

    # Local endpoints never accept credentials.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             ReplicationEndpoint.new(%{
               "kind" => "local",
               "database_uuid" => uuid,
               "auth_token" => "abc"
             })

    # Empty-string auth_token is rejected.
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             ReplicationEndpoint.new(%{
               "kind" => "remote",
               "database_uuid" => uuid,
               "base_url" => "https://example.test",
               "auth_token" => ""
             })
  end

  test "configuration and public errors retain their bounded stable contracts" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             VialKeeper.Config.merge_and_bound(%{"unknown" => true})

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             VialKeeper.Config.merge_and_bound(%{"queries" => %{"max_limit" => 501}})

    Enum.each(VialKeeper.Error.registry(), fn {code, {status, registry_retryable}} ->
      error = VialKeeper.Error.new(code, "message")

      expected_retryable =
        case registry_retryable do
          # API-016: internal_error retryability is "Depends on details"; absent an explicit
          # opt-in the default is the safer non-retryable outcome.
          :depends -> false
          boolean -> boolean
        end

      assert error.http_status == status
      assert error.retryable == expected_retryable
      assert is_boolean(error.retryable)
      assert is_map(VialKeeper.Error.public(error))
    end)

    # AUTH-004: unauthorized is 401, non-retryable, and uses a stable constant
    # message indistinguishable across missing/malformed/wrong-token failures.
    unauthorized = VialKeeper.Error.unauthorized()

    assert unauthorized.code == :unauthorized
    assert unauthorized.http_status == 401
    assert unauthorized.retryable == false
  end

  test "internal_error retryability depends on details (API-016)" do
    # API-016: internal_error is "Depends on details". The registry records a `:depends`
    # sentinel; the auto-generated constructor defaults to the safer non-retryable outcome,
    # and only an explicit opt-in marks a genuinely transient internal failure retryable.
    assert :depends == elem(VialKeeper.Error.registry()[:internal_error], 1)

    default = VialKeeper.Error.internal_error("unexpected SQLite failure")
    assert default.http_status == 500
    assert default.retryable == false

    transient =
      VialKeeper.Error.new(:internal_error, "transient remote failure", %{}, retryable: true)

    assert transient.retryable == true

    assert %{
             code: "internal_error",
             message: "internal error",
             retryable: false,
             details: %{}
           } = VialKeeper.Error.public(default)

    diagnostic =
      VialKeeper.Error.internal_error("SQLite statement failed: users@example.test", %{
        cause: "{:sqlite_error, 'disk I/O error'}",
        database_path: "/private/customer.vialkeeper"
      })

    refute inspect(VialKeeper.Error.public(diagnostic)) =~ "SQLite"
    refute inspect(VialKeeper.Error.public(diagnostic)) =~ "users@example.test"
    refute inspect(VialKeeper.Error.public(diagnostic)) =~ "/private/customer.vialkeeper"
  end

  test "public domain errors omit private causes while retaining stable details" do
    error =
      VialKeeper.Error.database_unavailable("database unavailable", %{
        cause: "{:file_error, '/private/customer.vialkeeper'}",
        database_uuid: "db-uuid",
        history_epoch: "epoch"
      })

    assert %{
             code: "database_unavailable",
             message: "database unavailable",
             retryable: true,
             details: %{database_uuid: "db-uuid", history_epoch: "epoch"}
           } = VialKeeper.Error.public(error)
  end

  test "unicode_words_v1 tokenization is deterministic" do
    assert FullText.tokens("École café 東京") == ["école", "cafe", "東京"]
    assert FullText.tokens("École café", :remove) == ["ecole", "cafe"]
  end
end
