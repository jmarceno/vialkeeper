defmodule ElixirDB.Documents do
  @moduledoc "Validated document operations over the database runtime."
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Runtime.DatabaseCatalog

  def get(uuid, request) do
    with {:ok, request} <- validate_get(request) do
      DatabaseCatalog.command(uuid, {:command, :get_document, request})
    end
  end

  def put(uuid, request) do
    with {:ok, request} <- validate_mutation(request, false) do
      DatabaseCatalog.command(uuid, {:command, :put, request})
    end
  end

  def delete(uuid, request) do
    with {:ok, request} <- validate_mutation(request, true) do
      DatabaseCatalog.command(uuid, {:command, :delete, request})
    end
  end

  def resolve(uuid, request) do
    with {:ok, normalized} <- validate_resolution(request) do
      DatabaseCatalog.command(uuid, {:command, :resolve, normalized})
    end
  end

  def bulk_get(uuid, requests) when is_list(requests) do
    if length(requests) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500),
      do:
        {:ok,
         Enum.map(requests, fn request ->
           case get(uuid, request) do
             {:ok, value} -> %{ok: value}
             {:error, error} -> %{error: ElixirDB.Error.public(error)}
           end
         end)},
      else:
        {:error, ElixirDB.Error.resource_limit("bulk-get operation count exceeds the host limit")}
  end

  def bulk_write(uuid, operations) when is_list(operations) do
    if length(operations) <= (ElixirDB.Config.host_limits()[:max_bulk_operations] || 500),
      do:
        DatabaseCatalog.command(
          uuid,
          {:command, :bulk_write, %{operations: Enum.map(operations, &normalize_bulk_operation/1)}}
        ),
      else:
        {:error, ElixirDB.Error.resource_limit("bulk-write operation count exceeds the host limit")}
  end

  defp validate_get(%{id: id} = request) do
    with :ok <- known(request, [:id, :revision, :include_conflicts]),
         :ok <- validate_id(id),
         true <- is_boolean(Map.get(request, :include_conflicts, false)) do
      {:ok,
       %{
         document_id: id,
         revision: Map.get(request, :revision),
         include_conflicts: Map.get(request, :include_conflicts, false)
       }}
    else
      false -> {:error, ElixirDB.Error.invalid_request("include_conflicts must be a boolean")}
      {:error, error} -> {:error, error}
    end
  end

  defp validate_get(%{"id" => id} = request) do
    with :ok <- known(request, ["id", "revision", "include_conflicts"]) do
      validate_get(%{
        id: id,
        revision: Map.get(request, "revision"),
        include_conflicts: Map.get(request, "include_conflicts", false)
      })
    end
  end

  defp validate_get(_), do: {:error, ElixirDB.Error.invalid_request("document get requires id")}

  defp validate_mutation(request, true) when is_map(request) do
    with :ok <- known(request, [:id, :if_revision, "id", "if_revision"]) do
      validate_delete(request)
    end
  end

  defp validate_mutation(request, false) when is_map(request) do
    with :ok <- known(request, [:id, :if_revision, :body, "id", "if_revision", "body"]) do
      validate_put(request)
    end
  end

  defp validate_mutation(_request, _deleted),
    do: {:error, ElixirDB.Error.invalid_request("document mutation requires an object")}

  defp validate_put(request) do
    id = request[:id] || request["id"]
    body = request[:body] || request["body"]

    with :ok <- validate_id(id),
         true <- is_map(body),
         {:ok, canonical} <- Canonical.encode(body),
         {:ok, normalized} <- ElixirDB.JSON.StrictDecoder.decode(canonical),
         {:ok, revision} <- expected_revision(request) do
      {:ok, %{document_id: id, if_revision: revision, body: normalized}}
    else
      false -> {:error, ElixirDB.Error.invalid_request("document body must be an object")}
      {:error, error} -> {:error, error}
    end
  end

  defp validate_delete(request) do
    id = request[:id] || request["id"]

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
      "id",
      "document_id",
      "expected_live_revisions",
      "chosen_parent_revision",
      "body",
      "delete_all"
    ]

    if Enum.all?(Map.keys(request), &(&1 in allowed)) do
      id = request[:id] || request[:document_id] || request["id"] || request["document_id"]

      expected =
        request[:expected_live_revisions] || request["expected_live_revisions"] || []

      chosen = request[:chosen_parent_revision] || request["chosen_parent_revision"]
      body = Map.get(request, :body, Map.get(request, "body"))
      delete_all = Map.get(request, :delete_all, Map.get(request, "delete_all", false))

      with :ok <- validate_id(id),
           true <- is_list(expected) and Enum.all?(expected, &is_binary/1),
           true <- is_boolean(delete_all),
           true <- delete_all or (is_binary(chosen) and is_map(body)),
           {:ok, canonical_body} <- if(delete_all, do: {:ok, nil}, else: Canonical.encode(body)),
           {:ok, normalized_body} <-
             if(delete_all,
               do: {:ok, nil},
               else: ElixirDB.JSON.StrictDecoder.decode(canonical_body)
             ) do
        {:ok,
         %{
           document_id: id,
           expected_live_revisions: expected,
           chosen_parent_revision: chosen,
           body: normalized_body,
           delete_all: delete_all
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

  defp expected_revision(request) do
    revision = Map.get(request, :if_revision, Map.get(request, "if_revision"))

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

  defp normalize_bulk_operation(%{"type" => "delete", "id" => id, "if_revision" => revision}),
    do: %{operation: :delete, document_id: id, if_revision: revision}

  defp normalize_bulk_operation(%{
         "type" => "put",
         "id" => id,
         "if_revision" => revision,
         "body" => body
       }),
       do: %{operation: :put, document_id: id, if_revision: revision, body: body}

  defp normalize_bulk_operation(%{type: :delete, id: id, if_revision: revision}),
    do: %{operation: :delete, document_id: id, if_revision: revision}

  defp normalize_bulk_operation(%{type: :put, id: id, if_revision: revision, body: body}),
    do: %{operation: :put, document_id: id, if_revision: revision, body: body}

  defp normalize_bulk_operation(%{
         "type" => "resolve",
         "id" => id,
         "expected_live_revisions" => expected,
         "chosen_parent_revision" => chosen,
         "body" => body
       }),
       do: %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         chosen_parent_revision: chosen,
         body: body,
         delete_all: false
       }

  defp normalize_bulk_operation(%{
         "type" => "resolve",
         "id" => id,
         "expected_live_revisions" => expected,
         "delete_all" => true
       }),
       do: %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         delete_all: true
       }

  defp normalize_bulk_operation(%{
         type: :resolve,
         id: id,
         expected_live_revisions: expected,
         chosen_parent_revision: chosen,
         body: body
       }),
       do: %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         chosen_parent_revision: chosen,
         body: body,
         delete_all: false
       }

  defp normalize_bulk_operation(%{
         type: :resolve,
         id: id,
         expected_live_revisions: expected,
         delete_all: true
       }),
       do: %{
         operation: :resolve,
         document_id: id,
         expected_live_revisions: expected,
         delete_all: true
       }

  defp normalize_bulk_operation(operation), do: operation
end
