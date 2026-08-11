defmodule ElixirDB.Contract.TermBlobTest do
  use ExUnit.Case, async: true

  @moduletag :sqlite_physical

  alias ElixirDB.Storage.SQLite.TermBlob

  test "round trips a JSON term and binds it as SQLite BLOB" do
    value = %{"active" => true, "items" => [1, 2.5, nil]}
    json = JSON.encode_to_iodata!(value) |> IO.iodata_to_binary()

    assert {:ok, blob} = TermBlob.encode(value, json)
    assert {:ok, ^value} = TermBlob.decode(blob, json)
    assert {:blob, ^blob} = TermBlob.bind(blob)
  end

  test "falls back when the canonical JSON changes" do
    value = %{"value" => 1}
    json = JSON.encode_to_iodata!(value) |> IO.iodata_to_binary()
    changed_json = JSON.encode_to_iodata!(%{"value" => 2}) |> IO.iodata_to_binary()

    assert {:ok, blob} = TermBlob.encode(value, json)
    assert {:fallback, :digest_mismatch} = TermBlob.decode(blob, changed_json)
  end

  test "rejects malformed and unsafe payloads" do
    json = "{}"
    digest = :crypto.hash(:sha256, json)

    malformed = <<"EXDBTERM", 1, digest::binary-size(32), 1, 2, 3>>
    unsafe = <<"EXDBTERM", 1, digest::binary-size(32), :erlang.term_to_binary(:not_json)::binary>>

    assert {:fallback, :invalid_term} = TermBlob.decode(malformed, json)
    assert {:fallback, :invalid_term} = TermBlob.decode(unsafe, json)
    assert {:fallback, :invalid_header} = TermBlob.decode(<<0, 1, 2>>, json)

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             TermBlob.decode_trusted(malformed)
  end

  test "trusted reads reject a digest that does not bind the decoded term" do
    value = %{"value" => 1}
    json = JSON.encode_to_iodata!(value) |> IO.iodata_to_binary()
    assert {:ok, blob} = TermBlob.encode(value, json)

    <<magic::binary-size(8), version, _digest::binary-size(32), payload::binary>> = blob
    forged_digest = :crypto.hash(:sha256, "{}")
    forged = <<magic::binary, version, forged_digest::binary, payload::binary>>

    assert {:error, %ElixirDB.Error{code: :integrity_violation}} =
             TermBlob.decode_trusted(forged)
  end

  test "trusted reads preserve the configured nesting limit" do
    value = %{"nested" => %{"value" => 1}}
    json = JSON.encode_to_iodata!(value) |> IO.iodata_to_binary()

    assert {:ok, blob} = TermBlob.encode(value, json)
    assert {:error, %ElixirDB.Error{code: :integrity_violation}} = TermBlob.decode_trusted(blob, 1)
    assert {:ok, ^value} = TermBlob.decode_trusted(blob, 2)
  end
end
