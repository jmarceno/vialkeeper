defmodule ElixirDB.Documents do
  @moduledoc "Validated document operations over the database runtime."
  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Shadow.ReadRouter

  def get(uuid, request), do: get(uuid, request, [])

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

  def get_with_meta(uuid, request, opts \\ []) do
    with {:ok, request} <- validate_get(request) do
      ReadRouter.get(
        uuid,
        request,
        Keyword.put(opts, :primary, fn normalized -> get_primary(uuid, normalized) end)
      )
    end
  end

  def put(uuid, request) do
    with {:ok, request} <- validate_mutation(request, false),
         {:ok, attachments, guard} <-
           Attachments.resolve_manifest_for_mutation(uuid, request.attachments) do
      try do
        DatabaseCatalog.command(
          uuid,
          {:command, :put, Map.put(request, :attachments, attachments)}
        )
      after
        Attachments.release_guard(uuid, guard)
      end
    end
  end

  def delete(uuid, request) do
    with {:ok, request} <- validate_mutation(request, true) do
      DatabaseCatalog.command(uuid, {:command, :delete, request})
    end
  end

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

  def bulk_get(uuid, requests) when is_list(requests), do: bulk_get(uuid, requests, [])

  def bulk_get(_uuid, _requests),
    do: {:error, ElixirDB.Error.invalid_request("bulk-get body must be an array")}

  def bulk_get(uuid, requests, opts) when is_list(requests) and is_list(opts) do
    case bulk_get_with_meta(uuid, requests, opts) do
      {:ok, value, _meta} -> {:ok, value}
      {:error, _} = error -> error
    end
  end

  def bulk_get(_uuid, _requests, _opts),
    do: {:error, ElixirDB.Error.invalid_request("bulk-get body must be an array")}

  def bulk_get_with_meta(uuid, requests, opts \\ [])

  def bulk_get_with_meta(uuid, requests, opts) when is_list(requests) do
    if length(requests) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500),
      do:
        ReadRouter.bulk_get(
          uuid,
          requests,
          Keyword.put(opts, :primary, fn normalized -> bulk_get_primary(uuid, normalized) end)
        ),
      else:
        {:error, ElixirDB.Error.resource_limit("bulk-get operation count exceeds the host limit")}
  end

  def bulk_get_with_meta(_uuid, _requests, _opts),
    do: {:error, ElixirDB.Error.invalid_request("bulk-get body must be an array")}

  defp get_primary(uuid, request),
    do: DatabaseCatalog.command(uuid, {:command, :get_document, request})

  defp bulk_get_primary(uuid, requests) do
    {:ok,
     Enum.map(requests, fn request ->
       case validate_get(request) do
         {:ok, normalized} ->
           case get_primary(uuid, normalized) do
             {:ok, value} -> %{ok: value}
             {:error, error} -> %{error: ElixirDB.Error.public(error)}
           end

         {:error, error} ->
           %{error: ElixirDB.Error.public(error)}
       end
     end)}
  end

  def bulk_write(uuid, operations) when is_list(operations) do
    limit = ElixirDB.Config.host_limits()[:max_bulk_operations] || 500

    if length(operations) > limit do
      {:error, ElixirDB.Error.resource_limit("bulk-write operation count exceeds the host limit")}
    else
      case prepare_bulk_operations(uuid, operations) do
        {:ok, normalized, guard} ->
          try do
            DatabaseCatalog.command(uuid, {:command, :bulk_write, %{operations: normalized}})
          after
            Attachments.release_guard(uuid, guard)
          end

        {:error, _} = error ->
          error
      end
    end
  end

  # SAFETY: see bulk_get/2 — a non-array body must not raise FunctionClauseError.
  def bulk_write(_uuid, _operations),
    do: {:error, ElixirDB.Error.invalid_request("bulk-write body must be an array")}

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
      false -> {:error, ElixirDB.Error.invalid_request("include_conflicts must be a boolean")}
      {:error, error} -> {:error, error}
    end
  end

  defp validate_get(_), do: {:error, ElixirDB.Error.invalid_request("document get requires id")}

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
    do: {:error, ElixirDB.Error.invalid_request("document mutation requires an object")}

  defp validate_put(request) do
    id = MapAccess.get(request, :id)
    body = MapAccess.get(request, :body)

    with :ok <- validate_id(id),
         true <- is_map(body),
         {:ok, canonical} <- Canonical.encode(body),
         {:ok, normalized} <- StrictDecoder.decode(canonical),
         {:ok, revision} <- expected_revision(request),
         {:ok, attachments} <- parse_attachments_field(request) do
      {:ok,
       %{
         document_id: id,
         if_revision: revision,
         body: normalized,
         attachments: attachments
       }}
    else
      false -> {:error, ElixirDB.Error.invalid_request("document body must be an object")}
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
        false -> {:error, ElixirDB.Error.invalid_request("conflict resolution request is invalid")}
        {:error, error} -> {:error, error}
      end
    else
      {:error, ElixirDB.Error.invalid_request("request contains an unknown field")}
    end
  end

  defp validate_resolution(_),
    do: {:error, ElixirDB.Error.invalid_request("conflict resolution request must be an object")}

  defp normalize_resolution_body(true, _body), do: {:ok, nil}

  defp normalize_resolution_body(false, body) do
    with {:ok, canonical} <- Canonical.encode(body) do
      StrictDecoder.decode(canonical)
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
    do: {:error, ElixirDB.Error.invalid_request("attachments must be an object")}

  defp expected_revision(request) do
    revision = MapAccess.get(request, :if_revision)

    if is_nil(revision) or is_binary(revision),
      do: {:ok, revision},
      else: {:error, ElixirDB.Error.invalid_request("if_revision must be a string or null")}
  end

  defp validate_id(id) when is_binary(id) do
    cond do
      id == "" ->
        {:error, ElixirDB.Error.invalid_request("document id must not be empty")}

      byte_size(id) > (ElixirDB.Config.host_limits()[:max_document_id_bytes] || 512) ->
        {:error, ElixirDB.Error.resource_limit("document id exceeds the configured limit")}

      String.contains?(id, <<0>>) ->
        {:error, ElixirDB.Error.invalid_request("document id contains NUL")}

      String.starts_with?(id, "_system/") ->
        {:error, ElixirDB.Error.invalid_request("reserved document id")}

      not String.valid?(id) ->
        {:error, ElixirDB.Error.invalid_request("document id must be valid UTF-8")}

      Enum.any?(String.to_charlist(id), &(&1 < 0x20)) ->
        {:error, ElixirDB.Error.invalid_request("document id contains a control character")}

      true ->
        :ok
    end
  end

  defp validate_id(_), do: {:error, ElixirDB.Error.invalid_request("document id must be a string")}

  defp known(request, allowed) when is_map(request) do
    if Enum.all?(Map.keys(request), &(&1 in allowed)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("request contains an unknown field")}
  end

  defp normalize_bulk_operation(%{"type" => "delete", "id" => id} = operation) do
    {:ok, %{operation: :delete, document_id: id, if_revision: bulk_if_revision(operation)}}
  end

  defp normalize_bulk_operation(%{"type" => "put", "id" => id, "body" => body} = operation) do
    with {:ok, attachments} <- parse_attachments_field(operation) do
      {:ok,
       %{
         operation: :put,
         document_id: id,
         if_revision: bulk_if_revision(operation),
         body: body,
         attachments: attachments
       }}
    end
  end

  defp normalize_bulk_operation(%{type: :delete, id: id} = operation) do
    {:ok, %{operation: :delete, document_id: id, if_revision: bulk_if_revision(operation)}}
  end

  defp normalize_bulk_operation(%{type: :put, id: id, body: body} = operation) do
    with {:ok, attachments} <- parse_attachments_field(operation) do
      {:ok,
       %{
         operation: :put,
         document_id: id,
         if_revision: bulk_if_revision(operation),
         body: body,
         attachments: attachments
       }}
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
    normalize_bulk_resolve(id, expected, chosen, body, operation)
  end

  defp normalize_bulk_operation(%{
         "type" => "resolve",
         "id" => id,
         "expected_live_revisions" => expected,
         "delete_all" => true
       }),
       do:
         {:ok,
          %{
            operation: :resolve,
            document_id: id,
            expected_live_revisions: expected,
            delete_all: true,
            attachments: %{}
          }}

  defp normalize_bulk_operation(
         %{
           type: :resolve,
           id: id,
           expected_live_revisions: expected,
           chosen_parent_revision: chosen,
           body: body
         } = operation
       ) do
    normalize_bulk_resolve(id, expected, chosen, body, operation)
  end

  defp normalize_bulk_operation(%{
         type: :resolve,
         id: id,
         expected_live_revisions: expected,
         delete_all: true
       }),
       do:
         {:ok,
          %{
            operation: :resolve,
            document_id: id,
            expected_live_revisions: expected,
            delete_all: true,
            attachments: %{}
          }}

  defp normalize_bulk_operation(operation),
    do:
      {:error,
       ElixirDB.Error.invalid_request("bulk-write operation is invalid", %{
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

  defp bulk_if_revision(operation), do: MapAccess.get(operation, :if_revision)
end
