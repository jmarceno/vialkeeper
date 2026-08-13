defmodule ElixirDB.HTTP.AttachmentsTest do
  @moduledoc """
  HTTP attachment upload/download contract and §23 owner non-blocking proofs.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Attachments
  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.{AttachmentCoordinator, DatabaseCatalog}
  alias ElixirDB.TestServer

  setup do
    path = "attachments-http-#{System.unique_integer([:positive])}.elixirdb"
    absolute = Path.join(ElixirDB.Config.database_root(), path)
    ElixirDB.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(path)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      ElixirDB.TempDatabase.cleanup(absolute)
    end)

    server = TestServer.start_supervised!()
    {:ok, uuid: uuid, path: path, base_url: server.base_url}
  end

  test "sequential multi-chunk uploads on one keepalive connection stay in sync", %{
    base_url: base,
    uuid: uuid
  } do
    # Bodies larger than one read_body chunk force streamed reads; the server
    # must respond with the fully-consumed conn or the second request on the
    # same connection reads leftover body bytes.
    first_payload = :crypto.strong_rand_bytes(200_000)
    second_payload = :crypto.strong_rand_bytes(150_000)

    %URI{host: host, port: port} = URI.parse(base)

    {:ok, socket} =
      :gen_tcp.connect(String.to_charlist(host), port, [:binary, active: false, nodelay: true])

    try do
      first = keepalive_upload!(socket, host, port, uuid, first_payload)
      second = keepalive_upload!(socket, host, port, uuid, second_payload)

      assert first["data"]["blob"] == sha256_hex(first_payload)
      assert second["data"]["blob"] == sha256_hex(second_payload)
    after
      :gen_tcp.close(socket)
    end
  end

  test "JSON body is rejected on upload", %{base_url: base, uuid: uuid} do
    assert {:ok, %{status: status, body: body}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/upload",
               json: %{"not" => "bytes"}
             )

    assert status == 400
    assert body["error"]["code"] == "invalid_request"
  end

  test "missing octet-stream content type is rejected", %{uuid: uuid} do
    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/attachments/upload", "hello")
      |> Router.call([])

    assert conn.status == 400
    {:ok, body} = StrictDecoder.decode(conn.resp_body)
    assert body["error"]["code"] == "invalid_request"
  end

  test "oversize mid-stream returns payload_too_large", %{uuid: uuid} do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config, %{"attachments" => %{"max_attachment_bytes" => 8}}}
             )

    assert {:error, %ElixirDB.Error{code: :payload_too_large}} =
             Attachments.upload_stream(uuid, ["12345678", "9"])
  end

  test "attachment_overloaded is returned before body streaming", %{uuid: uuid} do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{"attachments" => %{"max_concurrent_attachment_writes" => 1}}}
             )

    assert {:ok, token, _} = AttachmentCoordinator.acquire_write(uuid)

    conn =
      Plug.Test.conn(:post, "/v1/databases/#{uuid}/attachments/upload", "bytes")
      |> Plug.Conn.put_req_header("content-type", "application/octet-stream")
      |> Router.call([])

    assert conn.status == 429
    {:ok, body} = StrictDecoder.decode(conn.resp_body)
    assert body["error"]["code"] == "attachment_overloaded"

    assert :ok = AttachmentCoordinator.release(uuid, token)
  end

  test "UpdateConfig refreshes attachment coordinator limits", %{uuid: uuid} do
    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :update_config,
                %{
                  "attachments" => %{
                    "max_concurrent_attachment_reads" => 2,
                    "max_concurrent_attachment_writes" => 1,
                    "max_attachment_bytes" => 1024
                  }
                }}
             )

    status = AttachmentCoordinator.status(uuid)
    assert status.read_limit == 2
    assert status.write_limit == 1
    assert status.max_attachment_bytes == 1024
  end

  test "slow upload does not block unrelated owner document put", %{uuid: uuid} do
    parent = self()
    barrier = make_ref()

    source = fn ->
      {:ok, "hello-",
       fn ->
         send(parent, {:upload_blocked, barrier})

         receive do
           {:release, ^barrier} -> {:ok, "world", fn -> :done end}
         after
           10_000 -> {:error, :timeout}
         end
       end}
    end

    upload = Task.async(fn -> Attachments.upload_stream(uuid, source) end)
    assert_receive {:upload_blocked, ^barrier}, 2_000

    assert {:ok, put} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put,
                %{document_id: "while-uploading", if_revision: nil, body: %{"ok" => true}}}
             )

    assert is_binary(MapAccess.get(put, :revision))

    send(upload.pid, {:release, barrier})

    assert {:ok, %{blob: blob, length: length, expires_at: expires_at}} =
             Task.await(upload, 10_000)

    assert is_binary(blob)
    assert length == byte_size("hello-world")
    assert is_binary(expires_at)
  end

  test "document put rejects client-authoritative attachment length", %{uuid: uuid} do
    digest = String.duplicate("a", 64)

    assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{},
               "attachments" => %{
                 "file.bin" => %{
                   "blob" => digest,
                   "content_type" => "application/octet-stream",
                   "length" => 12
                 }
               }
             })

    assert String.contains?(message, "length")
  end

  test "document put rejects unknown attachment reference fields", %{uuid: uuid} do
    digest = String.duplicate("b", 64)

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "doc",
               "body" => %{},
               "attachments" => %{
                 "file.bin" => %{
                   "blob" => digest,
                   "content_type" => "application/octet-stream",
                   "path" => "/tmp/x"
                 }
               }
             })
  end

  test "upload attach get download happy path", %{base_url: base, uuid: uuid} do
    payload = "attachment-bytes-#{System.unique_integer([:positive])}"

    assert {:ok, %{status: 201, body: upload_body}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/upload",
               body: payload,
               headers: [{"content-type", "application/octet-stream"}]
             )

    blob = upload_body["data"]["blob"]
    length = upload_body["data"]["length"]
    assert is_binary(blob)
    assert length == byte_size(payload)
    assert is_binary(upload_body["data"]["expires_at"])

    assert {:ok, %{status: 201, body: put_body}} =
             Req.post(base <> "/v1/databases/#{uuid}/documents/put",
               json: %{
                 "id" => "with-att",
                 "body" => %{"n" => 1},
                 "attachments" => %{
                   "note.txt" => %{"blob" => blob, "content_type" => "text/plain"}
                 }
               }
             )

    revision = put_body["data"]["revision"]
    assert is_binary(revision)

    assert {:ok, %{status: 200, body: get_doc}} =
             Req.post(base <> "/v1/databases/#{uuid}/documents/get",
               json: %{"id" => "with-att"}
             )

    attachments = get_doc["data"]["attachments"]
    assert is_map(attachments)
    assert attachments["note.txt"]["blob"] == blob
    assert attachments["note.txt"]["length"] == length

    assert {:ok, %{status: 200, headers: headers, body: downloaded}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/get",
               json: %{"id" => "with-att", "revision" => nil, "name" => "note.txt"},
               decode_body: false
             )

    assert downloaded == payload
    assert header(headers, "content-type") == "text/plain"
    assert header(headers, "content-length") == Integer.to_string(length)
    assert header(headers, "etag") == ~s("#{blob}")
  end

  test "attachment download can address a historical revision", %{base_url: base, uuid: uuid} do
    old_payload = "historical-old"
    new_payload = "historical-new"

    assert {:ok, %{status: 201, body: old_upload}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/upload",
               body: old_payload,
               headers: [{"content-type", "application/octet-stream"}]
             )

    old_blob = old_upload["data"]["blob"]

    assert {:ok, %{status: 201, body: old_put}} =
             Req.post(base <> "/v1/databases/#{uuid}/documents/put",
               json: %{
                 "id" => "history-att",
                 "body" => %{"version" => 1},
                 "attachments" => %{
                   "old.txt" => %{"blob" => old_blob, "content_type" => "text/plain"}
                 }
               }
             )

    old_revision = old_put["data"]["revision"]

    assert {:ok, %{status: 201, body: new_upload}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/upload",
               body: new_payload,
               headers: [{"content-type", "application/octet-stream"}]
             )

    assert {:ok, %{status: 201, body: _new_put}} =
             Req.post(base <> "/v1/databases/#{uuid}/documents/put",
               json: %{
                 "id" => "history-att",
                 "if_revision" => old_revision,
                 "body" => %{"version" => 2},
                 "attachments" => %{
                   "new.txt" => %{
                     "blob" => new_upload["data"]["blob"],
                     "content_type" => "text/plain"
                   }
                 }
               }
             )

    assert {:ok, %{status: 200, headers: headers, body: downloaded}} =
             Req.post(base <> "/v1/databases/#{uuid}/attachments/get",
               json: %{"id" => "history-att", "revision" => old_revision, "name" => "old.txt"},
               decode_body: false
             )

    assert downloaded == old_payload
    assert header(headers, "content-length") == Integer.to_string(byte_size(old_payload))
    assert header(headers, "etag") == ~s("#{old_blob}")
  end

  test "zero-length uploads can be referenced and downloaded", %{uuid: uuid} do
    assert {:ok, %{blob: blob, length: 0}} = Attachments.upload_stream(uuid, [])

    assert {:ok, %{revision: revision}} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "empty-attachment",
               "body" => %{},
               "attachments" => %{
                 "empty.bin" => %{
                   "blob" => blob,
                   "content_type" => "application/octet-stream"
                 }
               }
             })

    assert {:ok, %{attachments: %{"empty.bin" => %{length: 0}}}} =
             ElixirDB.Documents.get(uuid, %{id: "empty-attachment"})

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{
               "id" => "empty-attachment",
               "revision" => revision,
               "name" => "empty.bin"
             })

    assert Enum.into(stream.body, <<>>) == <<>>
    stream.close.()
  end

  test "slow download does not block unrelated owner document put", %{uuid: uuid} do
    payload = "download-block-#{System.unique_integer([:positive])}"
    assert {:ok, %{blob: blob}} = Attachments.upload_stream(uuid, [payload])

    assert {:ok, put} =
             ElixirDB.Documents.put(uuid, %{
               "id" => "dl-doc",
               "body" => %{},
               "attachments" => %{
                 "a.bin" => %{"blob" => blob, "content_type" => "application/octet-stream"}
               }
             })

    revision = MapAccess.get(put, :revision)
    parent = self()
    gate = make_ref()

    download =
      Task.async(fn ->
        assert {:ok, stream} =
                 Attachments.open_stream(uuid, %{
                   "id" => "dl-doc",
                   "revision" => revision,
                   "name" => "a.bin"
                 })

        {[first], rest} = Enum.split(stream.body, 1)
        send(parent, {:download_blocked, gate, first})

        receive do
          {:release, ^gate} -> Enum.into(rest, <<>>)
        after
          10_000 -> flunk("download was not released")
        end
      end)

    assert_receive {:download_blocked, ^gate, first_chunk}, 2_000
    assert is_binary(first_chunk)

    assert {:ok, _} =
             DatabaseCatalog.command(
               uuid,
               {:command, :put,
                %{document_id: "during-download", if_revision: nil, body: %{"ok" => true}}}
             )

    send(download.pid, {:release, gate})
    remainder = Task.await(download, 10_000)
    assert first_chunk <> remainder == payload
  end

  defp keepalive_upload!(socket, host, port, uuid, payload) do
    request = [
      "POST /v1/databases/#{uuid}/attachments/upload HTTP/1.1\r\n",
      "host: #{host}:#{port}\r\n",
      "content-type: application/octet-stream\r\n",
      "content-length: #{byte_size(payload)}\r\n",
      "\r\n",
      payload
    ]

    :ok = :gen_tcp.send(socket, request)
    {status, headers, body} = read_http_response(socket)
    assert status == 201, "upload failed with #{status}: #{inspect(body)}"
    refute String.downcase(Map.get(headers, "connection", "")) == "close"
    {:ok, decoded} = StrictDecoder.decode(body)
    decoded
  end

  defp read_http_response(socket) do
    {head, rest} = read_until_headers(socket, <<>>)
    [status_line | header_lines] = String.split(head, "\r\n", trim: true)
    [_http, status | _] = String.split(status_line, " ")

    headers =
      Map.new(header_lines, fn line ->
        [name, value] = String.split(line, ":", parts: 2)
        {String.downcase(name), String.trim(value)}
      end)

    content_length = String.to_integer(Map.fetch!(headers, "content-length"))
    body = read_exact(socket, rest, content_length)
    {String.to_integer(status), headers, body}
  end

  defp read_until_headers(socket, buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [head, rest] ->
        {head, rest}

      [_] ->
        {:ok, chunk} = :gen_tcp.recv(socket, 0, 10_000)
        read_until_headers(socket, buffer <> chunk)
    end
  end

  defp read_exact(_socket, buffer, length) when byte_size(buffer) >= length,
    do: binary_part(buffer, 0, length)

  defp read_exact(socket, buffer, length) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 10_000)
    read_exact(socket, buffer <> chunk, length)
  end

  defp sha256_hex(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp header(headers, name) do
    Enum.find_value(headers, &header_value(&1, name))
  end

  defp header_value({key, value}, name) do
    if String.downcase(to_string(key)) == name, do: header_string(value)
  end

  defp header_value(_, _), do: nil

  defp header_string([first | _]), do: to_string(first)
  defp header_string(other), do: to_string(other)
end
