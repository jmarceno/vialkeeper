defmodule ElixirDB.Query.RegexTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Query.Regex, as: QueryRegex

  test "compiles with fixed Unicode semantics and matches strings" do
    assert {:ok, regex} = QueryRegex.compile("^café$")
    assert QueryRegex.source(regex) == "^café$"
    assert QueryRegex.match?(regex, "café") == {:ok, true}
    assert QueryRegex.match?(regex, "CAFÉ") == {:ok, false}
    assert QueryRegex.match?(regex, 42) == {:ok, false}
  end

  test "rejects invalid and oversized patterns" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} = QueryRegex.compile("(")

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             QueryRegex.compile(String.duplicate("a", 1_025))

    assert {:ok, _regex} = QueryRegex.compile(String.duplicate("a", 1_024))
  end

  test "does not accept invalid UTF-8 source" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} = QueryRegex.compile(<<255>>)
  end

  test "rejects inline option groups and keeps matching case-sensitive" do
    for source <- ["(?i)foo", "(?m)foo", "(?s)foo", "(?U)foo", "(?x)foo", "(?im-s:foo)"] do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} = QueryRegex.compile(source)
    end

    assert {:ok, regex} = QueryRegex.compile("foo")
    assert QueryRegex.match?(regex, "foo") == {:ok, true}
    assert QueryRegex.match?(regex, "FOO") == {:ok, false}
  end

  test "does not reject inline-looking text inside character classes" do
    assert {:ok, regex} = QueryRegex.compile("[(?i)]")
    assert QueryRegex.match?(regex, "(?i)") == {:ok, true}
  end

  test "returns resource_limit for bounded pathological matching" do
    assert {:ok, regex} = QueryRegex.compile("(a+)+$")

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             QueryRegex.match?(regex, String.duplicate("a", 1_000) <> "!")
  end
end
