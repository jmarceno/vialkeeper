defmodule ElixirDB.Replication.AttachmentRepresentationContractTest do
  @moduledoc """
  Attachment replication transfers the stored physical payload without expansion
  or a second compression decision at the target.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Replication.{BlobStream, LocalEndpoint, RemoteEndpoint}
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  setup do
    source_path = "repr-src-#{System.unique_integer([:positive])}.elixirdb"
    target_path = "repr-dst-#{System.unique_integer([:positive])}.elixirdb"
    root = ElixirDB.Config.database_root()
    source_abs = Path.join(root, source_path)
    target_abs = Path.join(root, target_path)
    ElixirDB.TempDatabase.cleanup(source_abs)
    ElixirDB.TempDatabase.cleanup(target_abs)

    assert {:ok, source} = DatabaseCatalog.create(source_path)
    assert {:ok, target} = DatabaseCatalog.create(target_path)
    source_uuid = source.database_uuid
    target_uuid = target.database_uuid
    server = TestServer.start_supervised!()

    on_exit(fn ->
      _ = DatabaseCatalog.close(source_uuid)
      _ = DatabaseCatalog.close(target_uuid)
      _ = DatabaseCatalog.unregister(source_uuid)
      _ = DatabaseCatalog.unregister(target_uuid)
      ElixirDB.TempDatabase.cleanup(source_abs)
      ElixirDB.TempDatabase.cleanup(target_abs)
    end)

    {:ok, source_uuid: source_uuid, target_uuid: target_uuid, base_url: server.base_url}
  end

  test "compressed attachment is not expanded to logical bytes on the replication wire", %{
    source_uuid: source_uuid,
    base_url: base_url
  } do
    payload = String.duplicate("do-not-expand-on-the-wire-", 6_144)

    assert {:ok, %{blob: digest, length: logical}} =
             Attachments.upload_stream(source_uuid, [payload])

    {:ok, bundle} = DatabaseCatalog.bundle_root(source_uuid)
    assert {:ok, stat} = FilesystemStore.stat(bundle, digest)
    assert stat.encoding in [:compressed, :zstd]
    payload_length = encoded_payload_length(bundle, digest, stat)
    assert payload_length < logical

    assert {:ok, response} =
             Req.get(
               base_url <> "/v1/databases/#{source_uuid}/replication/blobs/#{digest}",
               decode_body: false,
               compressed: false
             )

    assert response.status == 200
    assert representation_content_type?(response)
    refute content_encoding?(response)
    assert content_length(response) == payload_length
    assert byte_size(response.body) == payload_length
    assert header(response, "x-elixirdb-blob-encoding") in ["zstd", "compressed"]
  end

  test "target install does not probe or recompress a replicated representation", %{
    source_uuid: source_uuid,
    target_uuid: target_uuid,
    base_url: base_url
  } do
    payload = String.duplicate("no-second-compression-decision-", 4_096)

    assert {:ok, %{blob: digest, length: logical}} =
             Attachments.upload_stream(source_uuid, [payload])

    {:ok, source_bundle} = DatabaseCatalog.bundle_root(source_uuid)
    source_bytes = representation_file_bytes!(source_bundle, digest)

    {:ok, remote} =
      RemoteEndpoint.new(%{
        "kind" => "remote",
        "base_url" => base_url,
        "database_uuid" => target_uuid
      })

    {:ok, source_endpoint} = LocalEndpoint.new(source_uuid)

    probes_before =
      count_probes(fn -> transfer_representation!(source_endpoint, remote, digest) end)

    assert probes_before == 0

    {:ok, target_bundle} = DatabaseCatalog.bundle_root(target_uuid)
    target_bytes = representation_file_bytes!(target_bundle, digest)
    assert target_bytes == source_bytes

    assert {:ok, opened} = Attachments.open_blob(target_uuid, digest)
    assert opened.length == logical
    assert Enum.into(opened.body, <<>>) == payload
  end

  defp transfer_representation!(source_endpoint, remote, digest) do
    assert {:ok, %BlobStream{} = stream} = LocalEndpoint.open_blob(source_endpoint, digest)
    assert :ok = RemoteEndpoint.put_blob(remote, stream)
  end

  defp count_probes(fun) do
    parent = self()
    :erlang.trace_pattern({ElixirDB.Attachments.Compression, :probe, 1}, true, [])
    :erlang.trace(:existing, true, [:call, :set_on_spawn])
    :erlang.trace(parent, false, [:call])

    try do
      fun.()
      receive_probes(0, 50)
    after
      :erlang.trace(:all, false, [:call])
      :erlang.trace_pattern({ElixirDB.Attachments.Compression, :probe, 1}, false, [])
      flush_traces()
    end
  end

  defp receive_probes(count, remaining) when remaining <= 0, do: count

  defp receive_probes(count, remaining) do
    receive do
      {:trace, _pid, :call, {ElixirDB.Attachments.Compression, :probe, _}} ->
        receive_probes(count + 1, remaining)

      {:trace, _pid, :call, _} ->
        receive_probes(count, remaining)
    after
      20 ->
        receive_probes(count, remaining - 1)
    end
  end

  defp flush_traces do
    receive do
      {:trace, _, _, _} -> flush_traces()
    after
      0 -> :ok
    end
  end

  defp encoded_payload_length(bundle, digest, stat) do
    prefix = String.slice(digest, 0, 2)
    blob = Path.join([bundle, "blobs", prefix, digest <> ".blob"])
    zst = Path.join([bundle, "blobs", prefix, digest <> ".zst"])

    cond do
      File.regular?(blob) -> File.stat!(blob).size - 92
      File.regular?(zst) -> File.stat!(zst).size - 48
      true -> stat.physical_size
    end
  end

  defp representation_file_bytes!(bundle, digest) do
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle, "blobs", prefix])

    case Path.wildcard(Path.join(dir, digest <> ".*")) do
      [path] -> File.read!(path)
      other -> flunk("expected one representation file, got: #{inspect(other)}")
    end
  end

  defp representation_content_type?(response) do
    type = header(response, "content-type") || ""
    String.starts_with?(type, "application/vnd.elixirdb.blob-representation")
  end

  defp content_encoding?(response) do
    encoding = header(response, "content-encoding")
    is_binary(encoding) and encoding not in ["", "identity"]
  end

  defp content_length(response) do
    case Integer.parse(header(response, "content-length") || "") do
      {int, ""} -> int
      _ -> -1
    end
  end

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
end
