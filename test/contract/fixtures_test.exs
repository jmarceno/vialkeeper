defmodule ElixirDB.Contract.FixturesTest do
  @moduledoc """
  Contract tests that load Phase 0 language-neutral fixtures from priv/fixtures.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.Domain.Checkpoint
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.FullText
  alias ElixirDB.Replication.{CheckpointReconciler, Id}
  alias ElixirDB.Revisions.Id, as: RevisionId
  alias ElixirDB.Storage.SQLite.Adapter

  @fixtures_root Application.app_dir(:elixir_db, "priv/fixtures")

  test "canonical JSON object fixtures match ElixirDB.JSON.Canonical" do
    fixtures = load_json!("canonical_json/objects.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      assert {:ok, actual} = Canonical.encode(fixture["input"])

      assert actual == fixture["expected"],
             "canonical mismatch for #{fixture["id"]}: #{inspect(actual)} != #{inspect(fixture["expected"])}"

      assert Map.has_key?(fixture, "matches_official_jcs")

      if fixture["matches_official_jcs"] == false and is_binary(fixture["official_expected"]) do
        assert actual != fixture["official_expected"],
               "#{fixture["id"]} flagged as intentional JCS divergence but matches official_expected"
      end
    end
  end

  test "RFC 8785 Appendix B number fixtures match ElixirDB.JSON.Canonical" do
    fixtures = load_json!("canonical_json/numbers.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      bits = String.to_integer(fixture["ieee754_hex"], 16)
      <<float::float>> = <<bits::64>>
      assert {:ok, actual} = Canonical.encode(float)

      assert actual == fixture["expected"],
             "number mismatch for #{fixture["id"]}: #{inspect(actual)} != #{inspect(fixture["expected"])}"

      assert Map.has_key?(fixture, "matches_official_jcs")

      if fixture["matches_official_jcs"] do
        assert actual == fixture["expected_es6"]
      else
        assert actual != fixture["expected_es6"],
               "#{fixture["id"]} flagged as intentional JCS divergence but matches expected_es6"
      end
    end
  end

  test "revision ID fixtures match ElixirDB.Revisions.Id (REV-002)" do
    fixtures = load_json!("revision_ids/vectors.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      case fixture do
        %{"expect_error" => code} ->
          assert {:error, %ElixirDB.Error{code: actual_code}} =
                   RevisionId.calculate(%{
                     document_id: fixture["document_id"],
                     history_id: fixture["history_id"],
                     parent_revision: fixture["parent_revision"],
                     deleted: fixture["deleted"],
                     body: fixture["body"]
                   })

          assert Atom.to_string(actual_code) == code

        %{"history_id" => history_id, "expected_revision_id" => expected} ->
          assert {:ok, actual} =
                   RevisionId.calculate(
                     fixture["document_id"],
                     history_id,
                     fixture["parent_revision"],
                     fixture["deleted"],
                     fixture["body"]
                   )

          assert actual == expected, "revision id mismatch for #{fixture["id"]}"
          assert String.match?(actual, ~r/^\d+-[0-9a-f]{64}$/)

          {:ok, canonical} =
            Canonical.encode(%{
              "version" => 1,
              "document_id" => fixture["document_id"],
              "history_id" => history_id,
              "parent_revision" => fixture["parent_revision"],
              "deleted" => fixture["deleted"],
              "body" => fixture["body"]
            })

          assert canonical == fixture["canonical_payload"],
                 "canonical payload mismatch for #{fixture["id"]}"
      end
    end
  end

  test "unicode_words_v1 tokenization fixtures match ElixirDB.Query.FullText (QUERY-015)" do
    fixtures = load_json!("tokenization/unicode_words_v1.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      diacritics =
        case fixture["diacritics"] do
          "remove" -> :remove
          _ -> :preserve
        end

      actual = FullText.tokens(fixture["input"], diacritics)

      assert actual == fixture["expected"],
             "tokenization mismatch for #{fixture["id"]}: #{inspect(actual)} != #{inspect(fixture["expected"])}"
    end
  end

  test "FTS5 unicode61 token multisets match FullText.tokens/2 for marked fixtures" do
    fixtures =
      "tokenization/unicode_words_v1.json"
      |> load_json!()
      |> Enum.filter(&(&1["check_fts5"] == true))

    assert [_ | _] = fixtures

    for fixture <- fixtures do
      diacritics = fixture["diacritics"] || "preserve"
      remove = if diacritics == "remove", do: "2", else: "0"
      expected = fixture["expected"]

      actual_elixir =
        FullText.tokens(fixture["input"], if(diacritics == "remove", do: :remove, else: :preserve))

      assert actual_elixir == expected

      fts_counts = fts5_token_counts(fixture["input"], remove)
      elixir_counts = Enum.frequencies(expected)

      assert fts_counts == elixir_counts,
             "FTS5 unicode61 mismatch for #{fixture["id"]}: fts=#{inspect(fts_counts)} elixir=#{inspect(elixir_counts)}"
    end
  end

  test "protocol fixtures exist with required wire shapes" do
    http = load_json!("protocol/http_envelopes.json")
    replication = load_json!("protocol/replication_wire.json")

    assert Enum.any?(http, &(&1["id"] == "success-envelope"))
    assert Enum.any?(http, &(&1["id"] == "error-envelope"))
    assert Enum.any?(replication, &(&1["id"] == "handshake-identity"))
    assert Enum.any?(replication, &(&1["id"] == "checkpoint"))
    assert Enum.any?(replication, &(&1["id"] == "transferred-revision"))
    assert Enum.any?(replication, &(&1["id"] == "boundary-page-response"))
    assert Enum.any?(replication, &(&1["id"] == "diff-revisions-response"))
  end

  test "replication ID fixtures match ElixirDB.Replication.Id (REPL-006)" do
    fixtures = load_json!("protocol/replication_ids.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      assert {:ok, actual} =
               Id.calculate(
                 fixture["source_database_uuid"],
                 fixture["target_database_uuid"],
                 fixture["direction"],
                 fixture["mode"],
                 fixture["filter"]
               )

      assert actual == fixture["expected_replication_id"],
             "replication id mismatch for #{fixture["id"]}"

      assert String.match?(actual, ~r/^[0-9a-f]{64}$/)
    end

    one_shot = Enum.find(fixtures, &(&1["id"] == "push-one-shot"))
    continuous = Enum.find(fixtures, &(&1["id"] == "push-continuous-same-endpoints"))
    assert one_shot && continuous
    assert one_shot["expected_replication_id"] != continuous["expected_replication_id"]
  end

  test "checkpoint reconcile fixtures match CheckpointReconciler (REPL-007)" do
    fixtures = load_json!("protocol/checkpoint_reconcile.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      if Map.has_key?(fixture, "expected_common_sequence") do
        actual =
          CheckpointReconciler.common_sequence(fixture["source"], fixture["target"])

        assert actual == fixture["expected_common_sequence"],
               "common_sequence mismatch for #{fixture["id"]}"
      end

      case Map.get(fixture, "expected_reconcile") do
        %{} = expected ->
          reconcile =
            CheckpointReconciler.reconcile(
              fixture["source"],
              fixture["target"],
              fixture["source_identity"]
            )

          assert reconcile.bootstrap_required == expected["bootstrap_required"],
                 "bootstrap_required mismatch for #{fixture["id"]}"

          assert reconcile.reason == String.to_existing_atom(expected["reason"]),
                 "reason mismatch for #{fixture["id"]}"

          assert reconcile.since == expected["since"],
                 "since mismatch for #{fixture["id"]}"

        _ ->
          :ok
      end
    end
  end

  test "checkpoint CAS and wire fixtures execute (REPL-007)" do
    fixtures = load_json!("protocol/checkpoint_cas.json")
    assert [_ | _] = fixtures

    for fixture <- fixtures do
      case fixture["op"] do
        "from_wire" ->
          execute_checkpoint_wire(fixture)

        nil ->
          execute_checkpoint_cas_scenario(fixture)
      end
    end
  end

  defp execute_checkpoint_wire(fixture) do
    case Checkpoint.from_wire(fixture["input"]) do
      {:ok, %Checkpoint{}} ->
        assert fixture["expect"]["ok"] == true, fixture["id"]

      {:error, %ElixirDB.Error{code: code}} ->
        assert fixture["expect"]["ok"] == false, fixture["id"]
        assert Atom.to_string(code) == fixture["expect"]["error_code"], fixture["id"]
    end
  end

  defp execute_checkpoint_cas_scenario(fixture) do
    {:ok, path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-fixture-cas")
    assert {:ok, adapter} = Adapter.create(path, %{})

    try do
      replication_id = fixture["replication_id"]

      for step <- fixture["steps"] do
        case step["op"] do
          "put" ->
            result =
              Adapter.put_local_record_cas(adapter, %{
                namespace: "checkpoints",
                key: replication_id,
                expected_version: step["expected_version"],
                value: step["value"]
              })

            expect = step["expect"]

            if expect["ok"] do
              assert {:ok, %{version: version, replayed: replayed}} = result

              assert version == expect["version"],
                     "#{fixture["id"]} put version"

              assert replayed == expect["replayed"],
                     "#{fixture["id"]} put replayed"
            else
              assert {:error, %ElixirDB.Error{code: code}} = result
              assert Atom.to_string(code) == expect["error_code"]
            end

          "get" ->
            assert {:ok, %{version: version, value: value}} =
                     Adapter.get_local_record(adapter, "checkpoints", replication_id)

            expect = step["expect"]
            assert version == expect["version"]
            assert value["source_sequence"] == expect["source_sequence"]
        end
      end
    after
      _ = Adapter.close(adapter)
      ElixirDB.TempDatabase.cleanup(path)
    end
  end

  defp fts5_token_counts(input, remove_diacritics) do
    path = Path.join(System.tmp_dir!(), "elixirdb-fts5-#{System.unique_integer([:positive])}.db")
    ElixirDB.TempDatabase.cleanup(path)
    {:ok, conn} = Exqlite.Sqlite3.open(path)

    try do
      ddl =
        "CREATE VIRTUAL TABLE docs USING fts5(content, tokenize = 'unicode61 remove_diacritics #{remove_diacritics}')"

      :ok = Exqlite.Sqlite3.execute(conn, ddl)
      {:ok, insert} = Exqlite.Sqlite3.prepare(conn, "INSERT INTO docs(content) VALUES (?)")
      :ok = Exqlite.Sqlite3.bind(insert, [input])
      :done = Exqlite.Sqlite3.step(conn, insert)
      :ok = Exqlite.Sqlite3.release(conn, insert)

      :ok =
        Exqlite.Sqlite3.execute(
          conn,
          "CREATE VIRTUAL TABLE docs_vocab USING fts5vocab(docs, 'row')"
        )

      {:ok, stmt} = Exqlite.Sqlite3.prepare(conn, "SELECT term, cnt FROM docs_vocab")

      rows =
        Stream.resource(
          fn -> stmt end,
          fn s ->
            case Exqlite.Sqlite3.step(conn, s) do
              {:row, [term, cnt]} -> {[{term, cnt}], s}
              :done -> {:halt, s}
              _ -> {:halt, s}
            end
          end,
          fn s -> Exqlite.Sqlite3.release(conn, s) end
        )
        |> Map.new()

      rows
    after
      Exqlite.Sqlite3.close(conn)
      ElixirDB.TempDatabase.cleanup(path)
    end
  end

  defp load_json!(relative) do
    path = Path.join(@fixtures_root, relative)
    assert File.exists?(path), "missing fixture file: #{path}"
    path |> File.read!() |> JSON.decode!()
  end
end
