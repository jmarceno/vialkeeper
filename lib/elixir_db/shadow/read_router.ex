defmodule ElixirDB.Shadow.ReadRouter do
  @moduledoc "Routes eventual reads through an exact ready shadow snapshot."

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Attachments.Representation
  alias ElixirDB.Error
  alias ElixirDB.Replication.BlobRepresentationStream
  alias ElixirDB.Shadow.RouteTable
  alias ElixirDB.Storage.Results

  @default_timeout 30_000

  @spec get(binary(), map(), keyword()) ::
          {:ok, term(), map()} | {:error, Error.t()}
  def get(source_uuid, request, opts \\ []) when is_binary(source_uuid) and is_map(request) do
    primary = Keyword.fetch!(opts, :primary)

    case consistency(opts) do
      :primary -> with_meta(primary.(request), "primary")
      :eventual -> route_get(source_uuid, request, opts, primary)
      {:error, _} = error -> error
    end
  end

  @spec bulk_get(binary(), [map()], keyword()) ::
          {:ok, list(), map()} | {:error, Error.t()}
  def bulk_get(source_uuid, requests, opts \\ [])
      when is_binary(source_uuid) and is_list(requests) do
    primary = Keyword.fetch!(opts, :primary)

    case consistency(opts) do
      :primary -> with_meta(primary.(requests), "primary")
      :eventual -> route_bulk(source_uuid, requests, opts, primary)
      {:error, _} = error -> error
    end
  end

  @spec open_attachment(binary(), map(), keyword()) ::
          {:ok, map(), map()} | {:error, Error.t()}
  def open_attachment(source_uuid, request, opts \\ [])
      when is_binary(source_uuid) and is_map(request) do
    primary = Keyword.fetch!(opts, :primary)

    case consistency(opts) do
      :primary -> with_meta(primary.(request), "primary")
      :eventual -> route_attachment(source_uuid, request, opts, primary)
      {:error, _} = error -> error
    end
  end

  defp route_get(source_uuid, request, opts, primary) do
    case RouteTable.get(source_uuid) do
      {:ok, snapshot} ->
        case shadow_get(snapshot, request, opts) do
          {:ok, document, watermark} -> {:ok, document, shadow_meta(watermark)}
          {:error, _error} -> fallback(source_uuid, snapshot, primary, request)
        end

      :not_found ->
        with_meta(primary.(request), "primary")
    end
  end

  defp route_bulk(source_uuid, requests, opts, primary) do
    case RouteTable.get(source_uuid) do
      {:ok, snapshot} ->
        case shadow_bulk(snapshot, requests, opts) do
          {:ok, documents, watermark} -> {:ok, documents, shadow_meta(watermark)}
          {:error, _error} -> fallback(source_uuid, snapshot, primary, requests)
        end

      :not_found ->
        with_meta(primary.(requests), "primary")
    end
  end

  defp route_attachment(source_uuid, request, opts, primary) do
    case RouteTable.get(source_uuid) do
      {:ok, snapshot} ->
        case shadow_attachment(snapshot, request, opts) do
          {:ok, stream, watermark} -> {:ok, stream, shadow_meta(watermark)}
          {:error, _error} -> fallback(source_uuid, snapshot, primary, request)
        end

      :not_found ->
        with_meta(primary.(request), "primary")
    end
  end

  defp shadow_get(snapshot, request, opts) do
    shadow_request = shadow_request(snapshot, request)

    with {:ok, response} <- call_endpoint(snapshot, :read_document, shadow_request, opts) do
      normalize_document_response(response)
    end
  end

  defp shadow_bulk(snapshot, requests, opts) do
    shadow_request = Map.put(shadow_request(snapshot, %{}), "requests", requests)

    with {:ok, response} <- call_endpoint(snapshot, :bulk_read_documents, shadow_request, opts) do
      normalize_bulk_response(response)
    end
  end

  defp shadow_attachment(snapshot, request, opts) do
    shadow_request = shadow_request(snapshot, request)

    with {:ok, document_response} <-
           call_endpoint(snapshot, :read_document, shadow_request, opts),
         {:ok, document, _document_watermark} <- normalize_document_response(document_response),
         {:ok, attachment} <- attachment_entry(document, request),
         {:ok, response} <-
           call_endpoint(snapshot, :open_attachment_representation, shadow_request, opts),
         {:ok, stream} <- logical_attachment_stream(response, attachment) do
      {:ok, stream, stream_watermark(response)}
    end
  end

  defp call_endpoint(snapshot, function, request, opts) do
    timeout = Keyword.get(opts, :timeout, endpoint_timeout(snapshot))
    endpoint = Map.fetch!(snapshot, :endpoint)
    apply(endpoint.__struct__, function, [endpoint, request, timeout, []])
  rescue
    exception in [ArgumentError, FunctionClauseError, KeyError, UndefinedFunctionError] ->
      {:error,
       Error.database_unavailable("shadow read endpoint failed", %{cause: inspect(exception)})}
  end

  defp fallback(source_uuid, snapshot, primary, request) do
    _ = RouteTable.compare_delete(source_uuid, snapshot)
    with_meta(primary.(request), "primary")
  end

  defp with_meta({:ok, value}, served_by), do: {:ok, value, %{served_by: served_by}}
  defp with_meta({:error, _} = error, _served_by), do: error

  defp shadow_meta(watermark), do: %{served_by: "shadow", source_watermark: watermark}

  defp shadow_request(snapshot, request) do
    request
    |> string_keys()
    |> Map.merge(%{
      "source_uuid" => snapshot.source_uuid,
      "shadow_uuid" => snapshot.shadow_uuid,
      "generation" => snapshot.generation,
      "operation_id" => snapshot.operation_id
    })
  end

  defp normalize_document_response(%{"document" => document} = response) do
    with {:ok, document} <- normalize_document(document) do
      {:ok, document, Map.get(response, "source_watermark", 0)}
    end
  end

  defp normalize_document_response(%{document: document} = response) do
    with {:ok, document} <- normalize_document(document) do
      {:ok, document, Map.get(response, :source_watermark, 0)}
    end
  end

  defp normalize_document_response(document),
    do: normalize_document(document) |> with_zero_watermark()

  defp normalize_document(%Results.GetDocument{} = document), do: {:ok, document}

  defp normalize_document(document) when is_map(document) do
    {:ok, Results.get_document(document)}
  rescue
    ArgumentError -> {:error, Error.shadow_incompatible("shadow document response is invalid")}
  end

  defp normalize_document(_),
    do: {:error, Error.shadow_incompatible("shadow document response is invalid")}

  defp with_zero_watermark({:ok, document}), do: {:ok, document, 0}
  defp with_zero_watermark({:error, _} = error), do: error

  defp normalize_bulk_response(%{"results" => results} = response) when is_list(results),
    do: normalize_bulk_entries(results, Map.get(response, "source_watermark", 0))

  defp normalize_bulk_response(%{results: results} = response) when is_list(results),
    do: normalize_bulk_entries(results, Map.get(response, :source_watermark, 0))

  defp normalize_bulk_response(results) when is_list(results),
    do: normalize_bulk_entries(results, 0)

  defp normalize_bulk_response(_),
    do: {:error, Error.shadow_incompatible("shadow bulk response is invalid")}

  defp normalize_bulk_entries(results, watermark) do
    normalized =
      Enum.map(results, fn
        %{"ok" => document} -> {:ok, document}
        %{ok: document} -> {:ok, document}
        {:ok, document} -> {:ok, document}
        _ -> :error
      end)

    if Enum.any?(normalized, &(&1 == :error)) do
      {:error, Error.shadow_incompatible("shadow bulk response contains an error")}
    else
      {:ok, Enum.map(normalized, fn {:ok, document} -> Results.get_document(document) end),
       watermark}
    end
  rescue
    ArgumentError -> {:error, Error.shadow_incompatible("shadow bulk response is invalid")}
  end

  defp attachment_entry(document, request) do
    name = Map.get(request, "name", Map.get(request, :name))
    attachments = Map.get(document, :attachments, %{}) || %{}

    case Map.get(attachments, to_string(name)) do
      attachment when is_map(attachment) -> {:ok, attachment}
      _ -> {:error, Error.attachment_not_found("attachment is not present in the shadow document")}
    end
  end

  defp logical_attachment_stream(%BlobRepresentationStream{} = stream, attachment) do
    attachment = string_keys(attachment)
    digest = attachment["blob"] || attachment["digest"] || stream.logical_digest
    content_type = attachment["content_type"]

    with {:ok, digest} <- Manifest.validate_digest(digest),
         :ok <-
           Representation.validate_route_digest(digest, BlobRepresentationStream.descriptor(stream)),
         {:ok, body} <- decode_body(stream) do
      {:ok,
       %{
         content_type: content_type || "application/octet-stream",
         content_length: stream.logical_length,
         etag: ~s("#{digest}"),
         body: body,
         close: fn -> :ok end
       }}
    end
  end

  defp logical_attachment_stream(%{"stream" => stream}, attachment),
    do: logical_attachment_stream(stream, attachment)

  defp logical_attachment_stream(_, _),
    do: {:error, Error.shadow_attachment_unavailable("shadow attachment response is invalid")}

  defp decode_body(%BlobRepresentationStream{encoding: :raw, body: body}), do: {:ok, body}

  defp decode_body(%BlobRepresentationStream{encoding: :zstd, body: body}) do
    alias ElixirDB.Attachments.Compression

    with {:ok, context} <- Compression.new_decompression_context() do
      {:ok,
       Stream.transform(body, context, fn chunk, context ->
         case Compression.decompress_chunk(context, chunk) do
           {:ok, output, next} -> {[IO.iodata_to_binary(output)], next}
           {:error, error} -> throw({:shadow_attachment_decode_error, error})
         end
       end)}
    end
  catch
    {:shadow_attachment_decode_error, error} ->
      {:error, Error.internal_error("shadow attachment decode failed", %{cause: error})}
  end

  defp stream_watermark(%{"source_watermark" => watermark}), do: watermark
  defp stream_watermark(%{source_watermark: watermark}), do: watermark
  defp stream_watermark(_), do: 0

  defp consistency(opts) do
    case Keyword.get(opts, :read_consistency, :primary) do
      value when value in [:primary, "primary"] -> :primary
      value when value in [:eventual, "eventual"] -> :eventual
      _ -> {:error, Error.invalid_request("read consistency must be primary or eventual")}
    end
  end

  defp endpoint_timeout(%{endpoint: %{read_timeout: timeout}}) when is_integer(timeout), do: timeout
  defp endpoint_timeout(_), do: @default_timeout

  defp string_keys(map), do: Map.new(map, fn {key, value} -> {to_string(key), value} end)
end
