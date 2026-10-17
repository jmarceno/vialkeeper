defmodule VialKeeper.Shadow.ReadRouter do
  @moduledoc "Routes eventual reads through an exact ready shadow snapshot."

  alias VialKeeper.Attachments.{Compression, Manifest}
  alias VialKeeper.Attachments.Representation
  alias VialKeeper.Error
  alias VialKeeper.Observability.Instrumentation.Shadow, as: ShadowInstr
  alias VialKeeper.Replication.BlobRepresentationStream
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.Reconciler
  alias VialKeeper.Shadow.RouteTable
  alias VialKeeper.Storage.Results

  @miss_codes [:document_not_found, :revision_not_found, :attachment_not_found]

  @spec get(binary(), map(), keyword()) ::
          {:ok, term(), map()} | {:error, Error.t()}
  def get(source_uuid, request, opts \\ []) when is_binary(source_uuid) and is_map(request) do
    primary = Keyword.fetch!(opts, :primary)

    case consistency(opts) do
      :primary -> with_meta(primary.(request), "source")
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
      :primary -> with_meta(primary.(requests), "source")
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
      :primary -> with_meta(primary.(request), "source")
      :eventual -> route_attachment(source_uuid, request, opts, primary)
      {:error, _} = error -> error
    end
  end

  defp route_get(source_uuid, request, opts, primary) do
    routed_read(source_uuid, fn -> primary.(request) end, &shadow_get(&1, request, opts))
  end

  defp route_bulk(source_uuid, requests, opts, primary) do
    routed_read(source_uuid, fn -> primary.(requests) end, &shadow_bulk(&1, requests, opts))
  end

  defp route_attachment(source_uuid, request, opts, primary) do
    routed_read(
      source_uuid,
      fn -> primary.(request) end,
      &shadow_attachment(&1, request, opts)
    )
  end

  defp routed_read(source_uuid, primary_result, shadow_fun) do
    case {DatabaseCatalog.ordinary_open?(source_uuid), RouteTable.get(source_uuid)} do
      {true, {:ok, snapshot}} ->
        ShadowInstr.read(source_uuid, fn ->
          serve_or_fallback(source_uuid, snapshot, primary_result, shadow_fun.(snapshot))
        end)

      _ ->
        with_meta(primary_result.(), "source")
    end
  end

  defp serve_or_fallback(_source_uuid, _snapshot, _primary_result, {:ok, value, watermark}),
    do: {:ok, value, shadow_meta(watermark)}

  defp serve_or_fallback(source_uuid, snapshot, primary_result, {:error, error}) do
    unless miss?(error) do
      ShadowInstr.fallback(source_uuid)
      _ = RouteTable.compare_delete(source_uuid, snapshot)
      _ = Reconciler.notify_invalidation(source_uuid, snapshot)
    end

    with_meta(primary_result.(), "source")
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

    with {:ok, response} <-
           call_endpoint(snapshot, :open_attachment_representation, shadow_request, opts) do
      normalize_attachment_response(response)
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

  defp with_meta({:ok, value}, served_by), do: {:ok, value, %{served_by: served_by}}
  defp with_meta({:error, _} = error, _served_by), do: error

  defp shadow_meta(watermark), do: %{served_by: "shadow", source_watermark: watermark}

  defp miss?(%Error{code: code}) when code in @miss_codes, do: true
  defp miss?(_), do: false

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

  defp normalize_document_response(response) when is_map(response) do
    document = Map.get(response, "document", Map.get(response, :document))

    with {:ok, document} <- fetch_document(document),
         {:ok, document} <- normalize_document(document),
         {:ok, watermark} <- require_watermark(response) do
      {:ok, document, watermark}
    end
  end

  defp normalize_document_response(_),
    do: {:error, Error.shadow_incompatible("shadow document response is invalid")}

  defp fetch_document(nil),
    do: {:error, Error.shadow_incompatible("shadow document response is invalid")}

  defp fetch_document(document), do: {:ok, document}

  defp normalize_document(%Results.GetDocument{} = document), do: {:ok, document}

  defp normalize_document(document) when is_map(document) do
    {:ok, Results.get_document(document)}
  rescue
    ArgumentError -> {:error, Error.shadow_incompatible("shadow document response is invalid")}
  end

  defp normalize_document(_),
    do: {:error, Error.shadow_incompatible("shadow document response is invalid")}

  defp normalize_bulk_response(response) when is_map(response) do
    results = Map.get(response, "results", Map.get(response, :results))

    with {:ok, results} <- fetch_bulk_results(results),
         {:ok, watermark} <- require_watermark(response) do
      normalize_bulk_entries(results, watermark)
    end
  end

  defp normalize_bulk_response(results) when is_list(results),
    do: {:error, Error.shadow_incompatible("shadow bulk response is missing a watermark")}

  defp normalize_bulk_response(_),
    do: {:error, Error.shadow_incompatible("shadow bulk response is invalid")}

  defp fetch_bulk_results(results) when is_list(results), do: {:ok, results}

  defp fetch_bulk_results(_),
    do: {:error, Error.shadow_incompatible("shadow bulk response is invalid")}

  defp normalize_bulk_entries(results, watermark) do
    classified = Enum.map(results, &classify_bulk_entry/1)

    cond do
      Enum.any?(classified, &match?({:protocol, _}, &1)) ->
        {:error, Error.shadow_incompatible("shadow bulk response is invalid")}

      Enum.any?(classified, &match?({:miss, _}, &1)) ->
        {:error, Error.document_not_found("shadow bulk read missed a document")}

      true ->
        {:ok, Enum.map(classified, fn {:ok, document} -> Results.get_document(document) end),
         watermark}
    end
  rescue
    ArgumentError -> {:error, Error.shadow_incompatible("shadow bulk response is invalid")}
  end

  defp classify_bulk_entry(%{"ok" => document}), do: {:ok, document}
  defp classify_bulk_entry(%{ok: document}), do: {:ok, document}
  defp classify_bulk_entry({:ok, document}), do: {:ok, document}
  defp classify_bulk_entry(%{"error" => error}), do: classify_bulk_error(error)
  defp classify_bulk_entry(%{error: error}), do: classify_bulk_error(error)
  defp classify_bulk_entry(_), do: {:protocol, :invalid}

  defp classify_bulk_error(%Error{code: code}) when code in @miss_codes, do: {:miss, code}

  defp classify_bulk_error(%{code: code}) when code in @miss_codes, do: {:miss, code}

  defp classify_bulk_error(%{"code" => code})
       when code in ["document_not_found", "revision_not_found", "attachment_not_found"],
       do: {:miss, String.to_existing_atom(code)}

  defp classify_bulk_error(%{code: code})
       when code in ["document_not_found", "revision_not_found", "attachment_not_found"],
       do: {:miss, String.to_existing_atom(code)}

  defp classify_bulk_error(_), do: {:protocol, :error}

  defp require_watermark(response) when is_map(response) do
    case Map.get(response, "source_watermark", Map.get(response, :source_watermark)) do
      watermark when is_integer(watermark) and watermark >= 0 -> {:ok, watermark}
      _ -> {:error, Error.shadow_incompatible("shadow read watermark is missing")}
    end
  end

  defp normalize_attachment_response(
         %{"stream" => stream, "source_watermark" => watermark} = response
       ) do
    attachment = Map.get(response, "attachment", %{})
    finish_attachment_response(stream, attachment, watermark)
  end

  defp normalize_attachment_response(%{stream: stream, source_watermark: watermark} = response) do
    attachment = Map.get(response, :attachment, %{})
    finish_attachment_response(stream, attachment, watermark)
  end

  defp normalize_attachment_response(_),
    do: {:error, Error.shadow_incompatible("shadow attachment response is invalid")}

  defp finish_attachment_response(stream, attachment, watermark)
       when is_integer(watermark) and watermark >= 0 do
    with {:ok, logical} <- logical_attachment_stream(stream, attachment) do
      {:ok, logical, watermark}
    end
  end

  defp finish_attachment_response(_stream, _attachment, _watermark),
    do: {:error, Error.shadow_incompatible("shadow read watermark is missing")}

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

  # The external CAS is outside the source's control, so the logical bytes are
  # verified as they stream: decoded output may never exceed the declared
  # length, and the final size and digest must match the route digest. A
  # failure emits an error element, which halts the response.
  defp decode_body(%BlobRepresentationStream{} = stream) do
    with {:ok, decoder} <- new_decoder(stream.encoding) do
      {:ok,
       Stream.transform(
         stream.body,
         fn ->
           %{
             decoder: decoder,
             size: 0,
             hash: :crypto.hash_init(:sha256),
             failed?: false
           }
         end,
         &verify_chunk(&1, &2, stream.logical_length),
         &finish_verified_body(&1, stream),
         fn _acc -> :ok end
       )}
    end
  end

  defp new_decoder(:raw), do: {:ok, :raw}

  defp new_decoder(:zstd) do
    case Compression.new_decompression_context() do
      {:ok, context} ->
        {:ok, context}

      {:error, reason} ->
        {:error, Error.internal_error("shadow attachment decode failed", %{cause: reason})}
    end
  end

  defp verify_chunk(_chunk, %{failed?: true} = acc, _limit), do: {:halt, acc}

  defp verify_chunk({:error, %Error{}} = error, acc, _limit), do: {[error], %{acc | failed?: true}}

  defp verify_chunk(chunk, acc, limit) when is_binary(chunk) do
    case decode_chunk(acc.decoder, chunk) do
      {:ok, output, decoder} ->
        size = acc.size + byte_size(output)

        if size > limit do
          {[{:error, Error.integrity_violation("shadow attachment exceeds its logical length")}],
           %{acc | failed?: true}}
        else
          {[output],
           %{acc | decoder: decoder, size: size, hash: :crypto.hash_update(acc.hash, output)}}
        end

      {:error, reason} ->
        {[{:error, Error.integrity_violation("shadow attachment is corrupt", %{cause: reason})}],
         %{acc | failed?: true}}
    end
  end

  defp decode_chunk(:raw, chunk), do: {:ok, chunk, :raw}

  defp decode_chunk(context, chunk) do
    case Compression.decompress_chunk(context, chunk) do
      {:ok, output, next} -> {:ok, IO.iodata_to_binary(output), next}
      {:error, _} = error -> error
    end
  end

  defp finish_verified_body(%{failed?: true} = acc, _stream), do: {[], acc}

  defp finish_verified_body(acc, stream) do
    digest = :crypto.hash_final(acc.hash) |> Base.encode16(case: :lower)

    if acc.size == stream.logical_length and digest == stream.logical_digest do
      {[], acc}
    else
      {[{:error, Error.integrity_violation("shadow attachment does not match its digest")}], acc}
    end
  end

  defp consistency(opts) do
    case Keyword.get(opts, :read_consistency, :eventual) do
      value when value in [:primary, "primary"] -> :primary
      value when value in [:eventual, "eventual"] -> :eventual
      _ -> {:error, Error.invalid_request("read consistency must be primary or eventual")}
    end
  end

  defp endpoint_timeout(%{endpoint: %{read_timeout: timeout}})
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp endpoint_timeout(%{endpoint: %{timeout: timeout}})
       when is_integer(timeout) and timeout > 0,
       do: timeout

  defp endpoint_timeout(_) do
    VialKeeper.Config.request_timeout_ms()
  end

  defp string_keys(map), do: Map.new(map, fn {key, value} -> {to_string(key), value} end)
end
