defmodule ElixirDB.HTTP.ReplicationWireCompressionTest do
  @moduledoc """
  Remote replication JSON is one bounded Zstandard frame on the wire.

  Public non-replication routes remain uncompressed.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  setup do
    path = "wire-json-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid
    server = TestServer.start_supervised!()

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, base_url: server.base_url}
  end

  test "identity GET advertises zstd, has no request body, and returns a compressed JSON envelope",
       %{uuid: uuid, base_url: base_url} do
    assert {:ok, response} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/identity",
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    assert response.status == 200
    assert json_content_type?(response)
    assert zstd_content_encoding?(response)
    assert uncompressed_length_header(response) > 0
    assert zstd_magic?(response.body)
    refute identity_encoding?(response)
  end

  test "bodyless replication GET rejects a request body with a compressed error", %{
    uuid: uuid,
    base_url: base_url
  } do
    assert {:ok, response} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/identity",
               body: "unexpected-body",
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    assert response.status == 400
    assert json_content_type?(response)
    assert zstd_content_encoding?(response)
    assert zstd_magic?(response.body)

    decoded = ElixirDB.TestReplicationWire.decode_response(response.headers, response.body)
    assert decoded["error"]["code"] == "invalid_request"
  end

  test "unauthorized replication JSON errors are compressed without disclosing the route", %{
    uuid: uuid,
    base_url: base_url
  } do
    previous = Application.get_env(:elixir_db, :auth)

    on_exit(fn ->
      restore_auth(previous)
    end)

    Application.put_env(:elixir_db, :auth,
      enabled: true,
      token_digests: [:crypto.hash(:sha256, "expected-token")]
    )

    assert {:ok, response} =
             Req.get(base_url <> "/v1/databases/#{uuid}/replication/identity",
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    assert response.status == 401
    assert json_content_type?(response)
    assert zstd_content_encoding?(response)
    assert uncompressed_length_header(response) > 0
    assert zstd_magic?(response.body)

    decoded = :ezstd.decompress(response.body)
    refute decoded =~ "replication/identity"
    refute decoded =~ uuid
  end

  test "malformed compressed JSON requests fail deterministically with compressed errors", %{
    uuid: uuid,
    base_url: base_url
  } do
    url = base_url <> "/v1/databases/#{uuid}/replication/revisions/diff"
    encoded = ElixirDB.TestReplicationWire.encode!(%{"revisions" => []})
    length_header = Integer.to_string(encoded.uncompressed_length)
    truncated = binary_part(encoded.body, 0, byte_size(encoded.body) - 1)
    concatenated = encoded.body <> encoded.body
    <<_magic::binary-size(4), frame_rest::binary>> = encoded.body
    corrupt_magic = <<0, 0, 0, 0>> <> frame_rest
    decoded_limit = ElixirDB.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216

    cases = [
      {"missing content-encoding",
       [{"content-type", "application/json"}, {"x-elixirdb-uncompressed-length", length_header}],
       encoded.body, 400, "invalid_request"},
      {"missing uncompressed length",
       [{"content-type", "application/json"}, {"content-encoding", "zstd"}], encoded.body, 400,
       "invalid_request"},
      {"non-canonical uncompressed length", wire_headers("0" <> length_header), encoded.body, 400,
       "invalid_request"},
      {"uncompressed length and frame content size disagree",
       wire_headers(Integer.to_string(encoded.uncompressed_length + 1)), encoded.body, 400,
       "invalid_request"},
      {"truncated frame", wire_headers(length_header), truncated, 400, "invalid_request"},
      {"concatenated frames", wire_headers(length_header), concatenated, 400, "invalid_request"},
      {"corrupt frame magic", wire_headers(length_header), corrupt_magic, 400, "invalid_request"},
      {"declared expansion over the limit", wire_headers(Integer.to_string(decoded_limit + 1)),
       encoded.body, 413, "payload_too_large"}
    ]

    for {label, headers, body, status, code} <- cases do
      assert {:ok, response} =
               Req.post(url,
                 body: body,
                 headers: [{"accept-encoding", "zstd"} | headers],
                 decode_body: false,
                 compressed: false
               )

      assert response.status == status,
             "#{label}: expected #{status}, got #{response.status}"

      assert zstd_content_encoding?(response), "#{label}: error response is not compressed"
      assert zstd_magic?(response.body), "#{label}: error body is not a Zstandard frame"

      decoded = ElixirDB.TestReplicationWire.decode_response(response.headers, response.body)
      assert decoded["error"]["code"] == code, "#{label}: #{inspect(decoded)}"
    end
  end

  test "public document routes remain uncompressed", %{uuid: uuid, base_url: base_url} do
    assert {:ok, response} =
             Req.post(base_url <> "/v1/databases/#{uuid}/documents/put",
               json: %{"id" => "public", "body" => %{"ok" => true}},
               headers: [{"accept-encoding", "zstd"}],
               decode_body: false,
               compressed: false
             )

    assert response.status in [200, 201]
    refute zstd_content_encoding?(response)
    assert json_content_type?(response)
    assert {:ok, _} = JSON.decode(response.body)
  end

  defp wire_headers(uncompressed_length) do
    [
      {"content-type", "application/json"},
      {"content-encoding", "zstd"},
      {"x-elixirdb-uncompressed-length", uncompressed_length}
    ]
  end

  defp json_content_type?(response) do
    content_type(response)
    |> String.starts_with?("application/json")
  end

  defp zstd_content_encoding?(response) do
    encoding = header(response, "content-encoding")
    is_binary(encoding) and String.contains?(encoding, "zstd")
  end

  defp identity_encoding?(response) do
    encoding = header(response, "content-encoding")
    encoding in [nil, "", "identity"]
  end

  defp uncompressed_length_header(response) do
    case Integer.parse(header(response, "x-elixirdb-uncompressed-length") || "") do
      {int, ""} -> int
      _ -> 0
    end
  end

  defp zstd_magic?(body) when is_binary(body),
    do: String.starts_with?(body, <<0x28, 0xB5, 0x2F, 0xFD>>)

  defp zstd_magic?(_), do: false

  defp content_type(response), do: header(response, "content-type") || ""

  defp header(%{headers: headers}, name) do
    Enum.find_value(headers, fn
      {key, value} ->
        if String.downcase(to_string(key)) == name, do: header_value(value)

      _ ->
        nil
    end)
  end

  defp header_value(value) when is_binary(value), do: value
  defp header_value([value | _]) when is_binary(value), do: value
  defp header_value(_), do: nil

  defp restore_auth(nil), do: Application.delete_env(:elixir_db, :auth)
  defp restore_auth(value), do: Application.put_env(:elixir_db, :auth, value)
end
