defmodule VialKeeper.Documents do
  @moduledoc "Validated document operations over the database runtime."
  alias VialKeeper.Attachments
  alias VialKeeper.Attachments.Manifest
  alias VialKeeper.Error
  alias VialKeeper.JSON.Canonical
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Mutation
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.Shadow.ReadRouter

  @type uuid :: binary()
  @type result(ok) :: {:ok, ok} | {:error, Error.t()}

  @bulk_put_keys [
    :type,
    :id,
    :body,
    :if_revision,
    :attachments,
    "type",
    "id",
    "body",
    "if_revision",
    "attachments"
  ]
  @bulk_delete_keys [:type, :id, :if_revision, "type", "id", "if_revision"]
  @bulk_resolve_keys [
    :type,
    :id,
    :expected_live_revisions,
    :chosen_parent_revision,
    :body,
    :attachments,
    "type",
    "id",
    "expected_live_revisions",
    "chosen_parent_revision",
    "body",
    "attachments"
  ]
  @bulk_resolve_delete_all_keys [
    :type,
    :id,
    :expected_live_revisions,
    :delete_all,
    "type",
    "id",
    "expected_live_revisions",
    "delete_all"
  ]

  @spec get(uuid(), map()) :: result(map())
  def get(uuid, request), do: get(uuid, request, [])

  @spec get(uuid(), map(), keyword()) :: result(map())
  def get(uuid, request, opts) when is_list(opts) do
    case validate_get(request) do
      {:ok, request} ->
        get_validated(uuid, request, opts)

      {:error, _} = error ->
        error
    end
  end

  defp get_validated(uuid, request, opts) do
    case ReadRouter.get(
           uuid,
           request,
           Keyword.put(opts, :primary, fn normalized -> get_primary(uuid, normalized) end)
         ) do
      {:ok, value, _meta} -> {:ok, value}
      {:error, _} = error -> error
    end
  end

  @spec get_with_meta(uuid(), map()) :: {:ok, term(), map()} | {:error, Error.t()}
  @spec get_with_meta(uuid(), map(), keyword()) ::
          {:ok, term(), map()} | {:error, Error.t()}
  def get_with_meta(uuid, request, opts \\ []) do
    with {:ok, request} <- validate_get(request) do
      ReadRouter.get(
        uuid,
        request,
        Keyword.put(opts, :primary, fn normalized -> get_primary(uuid, normalized) end)
      )
    end
  end

  @spec put(uuid(), map()) :: result(map())
  def put(uuid, request) do
    with {:ok, request} <-
           Mutation.phase(:put, :validation, fn -> validate_mutation(request, false) end),
         {:ok, attachments, guard} <-
           Mutation.phase(:put, :attachment_manifest, fn ->
             Attachments.resolve_manifest_for_mutation(uuid, request.attachments)
           end) do
      try do
        Mutation.phase(:put, :catalog_route, fn ->
          DatabaseCatalog.command(
            uuid,
            {:command, :put, Map.put(request, :attachments, attachments)}
          )
        end)
      after
        Attachments.release_guard(uuid, guard)
      end
    end
  end

  @spec delete(uuid(), map()) :: result(map())
  def delete(uuid, request) do
    with {:ok, request} <-
           Mutation.phase(:delete, :validation, fn -> validate_mutation(request, true) end) do
      Mutation.phase(:delete, :catalog_route, fn ->
        DatabaseCatalog.command(uuid, {:command, :delete, request})
      end)
    end
  end

  @spec resolve(uuid(), map()) :: result(map())
  def resolve(uuid, request) do
    with {:ok, normalized} <- validate_resolution(request),
         {:ok, attachments, guard} <-
           Attachments.resolve_manifest_for_mutation(uuid, normalized.attachments) do
      try do
        DatabaseCatalog.command(
          uuid,
          {:command, :resolve, Map.put(normalized, :attachments, attachments)}
        )
      after
        Attachments.release_guard(uuid, guard)
      end
    end
  end

  @spec bulk_get(uuid(), [map()]) :: result(list())
  def bulk_get(uuid, requests) when is_list(requests), do: bulk_get(uuid, requests, [])

  @spec bulk_get(term(), term()) :: {:error, Error.t()}
  def bulk_get(_uuid, _requests),
    do: {:error, Error.invalid_request("bulk-get body must be an array")}

  @spec bulk_get(uuid(), [map()], keyword()) :: result(list())
  def bulk_get(uuid, requests, opts) when is_list(requests) and is_list(opts) do
    case bulk_get_with_meta(uuid, requests, opts) do
      {:ok, value, _meta} -> {:ok, value}
      {:error, _} = error -> error
    end
  end

  def bulk_get(_uuid, _requests, _opts),
    do: {:error, Error.invalid_request("bulk-get body must be an array")}

  @spec bulk_get_with_meta(uuid(), [map()]) :: {:ok, list(), map()} | {:error, Error.t()}
  @spec bulk_get_with_meta(uuid(), [map()], keyword()) ::
          {:ok, list(), map()} | {:error, Error.t()}
  def bulk_get_with_meta(uuid, requests, opts \\ [])

  def bulk_get_with_meta(uuid, requests, opts) when is_list(requests) do
    if length(requests) <= (VialKeeper.Config.host_limits()[:max_bulk_operations] || 500),
      do:
        ReadRouter.bulk_get(
          uuid,
          requests,
          Keyword.put(opts, :primary, fn normalized -> bulk_get_primary(uuid, normalized) end)
        ),
      else: {:error, Error.resource_limit("bulk-get operation count exceeds the host limit")}
  end

  def bulk_get_with_meta(_uuid, _requests, _opts),
    do: {:error, Error.invalid_request("bulk-get body must be an array")}

  defp get_primary(uuid, request),
    do: DatabaseCatalog.command(uuid, {:command, :get_document, request})

  defp bulk_get_primary(uuid, requests) do
    {:ok,
     Enum.map(requests, fn request ->
       case validate_get(request) do
         {:ok, normalized} ->
           case get_primary(uuid, normalized) do
             {:ok, value} -> %{ok: value}
             {:error, error} -> %{error: Error.public(error)}
           end

         {:error, error} ->
           %{error: Error.public(error)}
       end
     end)}
  end

  @spec bulk_write(uuid(), [map()]) :: result(list())
  def bulk_write(uuid, operations) when is_list(operations) do
    limit = VialKeeper.Config.host_limits()[:max_bulk_operations] || 500

    if length(operations) > limit do
      {:error, Error.resource_limit("bulk-write operation count exceeds the host limit")}
    else
      dispatch_bulk_write(uuid, operations)
    end
  end

  # SAFETY: see bulk_get/2 — a non-array body must not raise FunctionClauseError.
  def bulk_write(_uuid, _operations),
    do: {:error, Error.invalid_request("bulk-write body must be an array")}

  defp dispatch_bulk_write(uuid, operations) do
    case Mutation.phase(:bulk_write, :validation, fn ->
           prepare_bulk_operations(uuid, operations)
         end) do
      {:ok, normalized, guard} ->
        catalog_bulk_write(uuid, normalized, guard)

      {:error, _} = error ->
        error
    end
  end

  defp catalog_bulk_write(uuid, normalized, guard) do
    Mutation.phase(:bulk_write, :catalog_route, fn ->
      DatabaseCatalog.command(uuid, {:command, :bulk_write, %{operations: normalized}})
    end)
  after
    Attachments.release_guard(uuid, guard)
  end

  defp prepare_bulk_operations(uuid, operations) do
    with {:ok, normalized} <- map_bulk_operations(operations) do
      if bulk_needs_reference_guard?(normalized) do
        prepare_bulk_with_guard(uuid, normalized)
      else
        {:ok, normalized, nil}
      end
    end
  end

  defp bulk_needs_reference_guard?(operations) do
    Enum.any?(operations, &operation_needs_reference_guard?/1)
  end

  defp operation_needs_reference_guard?(op) do
    case Map.get(op, :attachments, :omitted) do
      :omitted -> op.operation in [:put, :resolve] and not Map.get(op, :delete_all, false)
      map when is_map(map) -> map != %{}
      _ -> false
    end
  end

  defp map_bulk_operations(operations) do
    Enum.reduce_while(operations, {:ok, []}, fn operation, {:ok, acc} ->
      case normalize_bulk_operation(operation) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _} = error -> error
    end
  end

  defp prepare_bulk_with_guard(uuid, operations) do
    case Attachments.resolve_manifest_for_mutation(uuid, :omitted) do
      {:ok, :omitted, guard} ->
        case materialize_bulk_explicit(uuid, operations) do
          {:ok, prepared} ->
            {:ok, prepared, guard}

          {:error, _} = error ->
            Attachments.release_guard(uuid, guard)
            error
        end

      {:error, _} = error ->
        error
    end
  end

  defp materialize_bulk_explicit(uuid, operations) do
    Enum.reduce_while(operations, {:ok, []}, fn operation, {:ok, acc} ->
      materialize_one_bulk_operation(uuid, operation, acc)
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _} = error -> error
    end
  end

  defp materialize_one_bulk_operation(uuid, operation, acc) do
    case Map.get(operation, :attachments, :omitted) do
      :omitted ->
        {:cont, {:ok, [operation | acc]}}

      %{} = refs when map_size(refs) == 0 ->
        {:cont, {:ok, [operation | acc]}}

      refs when is_map(refs) ->
        case Attachments.materialize_references(uuid, refs) do
          {:ok, manifest} ->
            {:cont, {:ok, [Map.put(operation, :attachments, manifest) | acc]}}

          {:error, _} = error ->
            {:halt, error}
        end
    end
  end

  defp validate_get(request) when is_map(request) do
    with :ok <-
           known(request, [
             :id,
             :revision,
             :include_conflicts,
             "id",
             "revision",
             "include_conflicts"
           ]),
         id <- MapAccess.get(request, :id),
         :ok <- validate_id(id),
         true <- is_boolean(MapAccess.get(request, :include_conflicts, false)) do
      {:ok,
       %{
         document_id: id,
         revision: MapAccess.get(request, :revision),
         include_conflicts: MapAccess.get(request, :include_conflicts, false)
       }}
    else
      false -> {:error, Error.invalid_request("include_conflicts must be a boolean")}
      {:error, error} -> {:error, error}
    end
  end

  defp validate_get(_), do: {:error, Error.invalid_request("document get requires id")}

  defp validate_mutation(request, true) when is_map(request) do
    with :ok <- known(request, [:id, :if_revision, "id", "if_revision"]) do
      validate_delete(request)
    end
  end

  defp validate_mutation(request, false) when is_map(request) do
    with :ok <-
           known(request, [
             :id,
             :if_revision,
             :body,
             :attachments,
             "id",
             "if_revision",
             "body",
             "attachments"
           ]) do
      validate_put(request)
    end
  end

  defp validate_mutation(_request, _deleted),
    do: {:error, Error.invalid_request("document mutation requires an object")}

  defp validate_put(request) do
    id = MapAccess.get(request, :id)
    body = MapAccess.get(request, :body)

    with :ok <- validate_id(id),
         true <- is_map(body),
         {:ok, canonical, normalized} <- canonicalize_body(body, :put),
         {:ok, revision} <- expected_revision(request),
         {:ok, attachments} <- parse_attachments_field(request) do
      {:ok,
       %{
         document_id: id,
         if_revision: revision,
         body: normalized,
         body_json: canonical,
         attachments: attachments
       }}
    else
      false -> {:error, Error.invalid_request("document body must be an object")}
      {:error, error} -> {:error, error}
    end
  end

  defp validate_delete(request) do
    id = MapAccess.get(request, :id)

    with :ok <- validate_id(id), {:ok, revision} <- expected_revision(request) do
      {:ok, %{document_id: id, if_revision: revision}}
    end
  end

  defp validate_resolution(request) when is_map(request) do
    allowed = [
      :id,
      :document_id,
      :expected_live_revisions,
      :chosen_parent_revision,
      :body,
      :delete_all,
      :attachments,
      "id",
      "document_id",
      "expected_live_revisions",
      "chosen_parent_revision",
      "body",
      "delete_all",
      "attachments"
    ]

    if Enum.all?(Map.keys(request), &(&1 in allowed)) do
      id = MapAccess.get(request, :id, MapAccess.get(request, :document_id))

      expected = MapAccess.get(request, :expected_live_revisions, [])

      chosen = MapAccess.get(request, :chosen_parent_revision)
      body = MapAccess.get(request, :body)
      delete_all = MapAccess.get(request, :delete_all, false)

      with :ok <- validate_id(id),
           true <- is_list(expected) and Enum.all?(expected, &is_binary/1),
           true <- is_boolean(delete_all),
           true <- delete_all or (is_binary(chosen) and is_map(body)),
           {:ok, normalized_body} <- normalize_resolution_body(delete_all, body),
           {:ok, attachments} <- parse_resolution_attachments(request, delete_all) do
        {:ok,
         %{
           document_id: id,
           expected_live_revisions: expected,
           chosen_parent_revision: chosen,
           body: normalized_body,
           delete_all: delete_all,
           attachments: attachments
         }}
      else
        false ->
          {:error, Error.invalid_request("conflict resolution request is invalid")}

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, Error.invalid_request("request contains an unknown field")}
    end
  end

  defp validate_resolution(_),
    do: {:error, Error.invalid_request("conflict resolution request must be an object")}

  defp normalize_resolution_body(true, _body), do: {:ok, nil}

  defp normalize_resolution_body(false, body) do
    with {:ok, canonical} <- Canonical.encode(body) do
      Canonical.decode_encoded(body, canonical, json_decode_opts())
    end
  end

  defp parse_resolution_attachments(_request, true), do: {:ok, %{}}

  defp parse_resolution_attachments(request, false), do: parse_attachments_field(request)

  defp parse_attachments_field(request) when is_map(request) do
    cond do
      Map.has_key?(request, :attachments) ->
        normalize_client_attachments(Map.get(request, :attachments))

      Map.has_key?(request, "attachments") ->
        normalize_client_attachments(Map.get(request, "attachments"))

      true ->
        {:ok, :omitted}
    end
  end

  defp normalize_client_attachments(attachments) when attachments == %{}, do: {:ok, %{}}

  defp normalize_client_attachments(attachments) when is_map(attachments) do
    Manifest.normalize_references(attachments)
  end

  defp normalize_client_attachments(_),
    do: {:error, Error.invalid_request("attachments must be an object")}

  defp expected_revision(request) do
    revision = MapAccess.get(request, :if_revision)

    if is_nil(revision) or is_binary(revision),
      do: {:ok, revision},
      else: {:error, Error.invalid_request("if_revision must be a string or null")}
  end

  defp validate_id(id) when is_binary(id) do
    cond do
      id == "" ->
        {:error, Error.invalid_request("document id must not be empty")}

      byte_size(id) > (VialKeeper.Config.host_limits()[:max_document_id_bytes] || 512) ->
        {:error, Error.resource_limit("document id exceeds the configured limit")}

      String.contains?(id, <<0>>) ->
        {:error, Error.invalid_request("document id contains NUL")}

      String.starts_with?(id, "_system/") ->
        {:error, Error.invalid_request("reserved document id")}

      not String.valid?(id) ->
        {:error, Error.invalid_request("document id must be valid UTF-8")}

      Enum.any?(String.to_charlist(id), &(&1 < 0x20)) ->
        {:error, Error.invalid_request("document id contains a control character")}

      true ->
        :ok
    end
  end

  defp validate_id(_),
    do: {:error, Error.invalid_request("document id must be a string")}

  defp known(request, allowed) when is_map(request) do
    if Enum.all?(Map.keys(request), &(&1 in allowed)),
      do: :ok,
      else: {:error, Error.invalid_request("request contains an unknown field")}
  end

  defp normalize_bulk_operation(%{"type" => "delete", "id" => id} = operation) do
    with :ok <- known(operation, @bulk_delete_keys) do
      {:ok, %{operation: :delete, document_id: id, if_revision: bulk_if_revision(operation)}}
    end
  end

  defp normalize_bulk_operation(%{"type" => "put", "id" => id, "body" => body} = operation) do
    with :ok <- known(operation, @bulk_put_keys) do
      normalize_bulk_put(id, body, operation)
    end
  end

  defp normalize_bulk_operation(%{type: :delete, id: id} = operation) do
    with :ok <- known(operation, @bulk_delete_keys) do
      {:ok, %{operation: :delete, document_id: id, if_revision: bulk_if_revision(operation)}}
    end
  end

  defp normalize_bulk_operation(%{type: :put, id: id, body: body} = operation) do
    with :ok <- known(operation, @bulk_put_keys) do
      normalize_bulk_put(id, body, operation)
    end
  end

  defp normalize_bulk_operation(
         %{
           "type" => "resolve",
           "id" => id,
           "expected_live_revisions" => expected,
           "chosen_parent_revision" => chosen,
           "body" => body
         } = operation
       ) do
    with :ok <- known(operation, @bulk_resolve_keys) do
      normalize_bulk_resolve(id, expected, chosen, body, operation)
    end
  end

  defp normalize_bulk_operation(
         %{
           "type" => "resolve",
           "id" => id,
           "expected_live_revisions" => expected,
           "delete_all" => true
         } = operation
       ) do
    with :ok <- known(operation, @bulk_resolve_delete_all_keys) do
      {:ok,
       %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         delete_all: true,
         attachments: %{}
       }}
    end
  end

  defp normalize_bulk_operation(
         %{
           type: :resolve,
           id: id,
           expected_live_revisions: expected,
           chosen_parent_revision: chosen,
           body: body
         } = operation
       ) do
    with :ok <- known(operation, @bulk_resolve_keys) do
      normalize_bulk_resolve(id, expected, chosen, body, operation)
    end
  end

  defp normalize_bulk_operation(
         %{
           type: :resolve,
           id: id,
           expected_live_revisions: expected,
           delete_all: true
         } = operation
       ) do
    with :ok <- known(operation, @bulk_resolve_delete_all_keys) do
      {:ok,
       %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         delete_all: true,
         attachments: %{}
       }}
    end
  end

  defp normalize_bulk_operation(operation),
    do:
      {:error,
       Error.invalid_request("bulk-write operation is invalid", %{
         operation: inspect(operation)
       })}

  defp normalize_bulk_resolve(id, expected, chosen, body, operation) do
    with {:ok, attachments} <- parse_attachments_field(operation) do
      {:ok,
       %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         chosen_parent_revision: chosen,
         body: body,
         delete_all: false,
         attachments: attachments
       }}
    end
  end

  defp normalize_bulk_put(id, body, operation) do
    with {:ok, canonical, normalized} <- canonicalize_body(body, :bulk_write),
         {:ok, attachments} <- parse_attachments_field(operation) do
      {:ok,
       %{
         operation: :put,
         document_id: id,
         if_revision: bulk_if_revision(operation),
         body: normalized,
         body_json: canonical,
         attachments: attachments
       }}
    end
  end

  defp canonicalize_body(body, operation) do
    opts = json_decode_opts()

    with {:ok, canonical} <-
           Mutation.phase(operation, :canonical_encode, fn -> Canonical.encode(body) end),
         {:ok, normalized} <-
           Mutation.phase(operation, :strict_decode, fn ->
             Canonical.decode_encoded(body, canonical, opts)
           end) do
      {:ok, canonical, normalized}
    end
  end

  defp json_decode_opts do
    [max_depth: VialKeeper.Config.host_limits()[:max_json_nesting_depth] || 100]
  end

  defp bulk_if_revision(operation), do: MapAccess.get(operation, :if_revision)
end
