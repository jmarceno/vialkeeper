defmodule ElixirDB.Contract.FixturesTest do
  @moduledoc """
  Contract tests that load Phase 0 language-neutral fixtures from priv/fixtures.
  """

  use ExUnit.Case, async: true

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.FullText
  alias ElixirDB.Revisions.Id

  @fixtures_root Path.expand("../../priv/fixtures", __DIR__)

  test "canonical JSON object fixtures match ElixirDB.JSON.Canonical" do
    fixtures = load_json!("canonical_json/objects.json")
    assert length(fixtures) >= 1

    for fixture <- fixtures do
      assert {:ok, actual} = Canonical.encode(fixture["input"])

      assert actual == fixture["expected"],
             "canonical mismatch for #{fixture["id"]}: #{inspect(actual)} != #{inspect(fixture["expected"])}"
    end
  end

  test "RFC 8785 Appendix B number fixtures match ElixirDB.JSON.Canonical" do
    fixtures = load_json!("canonical_json/numbers.json")
    assert length(fixtures) >= 1

    for fixture <- fixtures do
      bits = String.to_integer(fixture["ieee754_hex"], 16)
      <<float::float>> = <<bits::64>>
      assert {:ok, actual} = Canonical.encode(float)

      assert actual == fixture["expected"],
             "number mismatch for #{fixture["id"]}: #{inspect(actual)} != #{inspect(fixture["expected"])}"
    end
  end

  test "revision ID fixtures match ElixirDB.Revisions.Id (REV-002)" do
    fixtures = load_json!("revision_ids/vectors.json")
    assert length(fixtures) >= 1

    for fixture <- fixtures do
      assert {:ok, actual} =
               Id.calculate(
                 fixture["document_id"],
                 fixture["parent_revision"],
                 fixture["deleted"],
                 fixture["body"]
               )

      assert actual == fixture["expected_revision_id"],
             "revision id mismatch for #{fixture["id"]}"

      assert String.match?(actual, ~r/^\d+-[0-9a-f]{64}$/)
    end
  end

  test "unicode_words_v1 tokenization fixtures match ElixirDB.Query.FullText (QUERY-015)" do
    fixtures = load_json!("tokenization/unicode_words_v1.json")
    assert length(fixtures) >= 1

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

  test "protocol fixtures exist with required wire shapes" do
    http = load_json!("protocol/http_envelopes.json")
    replication = load_json!("protocol/replication_wire.json")

    assert Enum.any?(http, &(&1["id"] == "success-envelope"))
    assert Enum.any?(http, &(&1["id"] == "error-envelope"))
    assert Enum.any?(replication, &(&1["id"] == "handshake-identity"))
    assert Enum.any?(replication, &(&1["id"] == "checkpoint"))
    assert Enum.any?(replication, &(&1["id"] == "transferred-revision"))
  end

  defp load_json!(relative) do
    path = Path.join(@fixtures_root, relative)
    assert File.exists?(path), "missing fixture file: #{path}"
    path |> File.read!() |> JSON.decode!()
  end
end
