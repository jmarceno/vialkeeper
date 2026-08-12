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
