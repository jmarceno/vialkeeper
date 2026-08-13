defmodule ElixirDB.HTTP.Routes.Shadows do
  @moduledoc "Source-side shadow desired-state and redacted status API."
  use Plug.Router

  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.Shadow.{Definition, Reconciler, Registry, RouteTable}

  @uuid_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  plug(:match)
  plug(:dispatch)

  get "/" do
    case Registry.get(Request.uuid(conn)) do
      {:ok, entry} -> Response.ok(conn, redacted_entry(entry))
      :not_found -> Response.ok(conn, %{"state" => "absent", "enabled" => false})
      {:error, error} -> Response.error(conn, error)
    end
  end

  put "/" do
    Request.call(
      conn,
      [
        allowed_fields: ["enabled", "location", "attachment_location"],
        unknown_message: "shadow configuration contains an unknown field"
      ],
      fn body, conn -> update_shadow(conn, body) end
    )
  end

  match _ do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp update_shadow(conn, body) when is_map(body) do
    source_uuid = Request.uuid(conn)

    with :ok <- source_uuid_valid(source_uuid),
         :ok <- ordinary_source?(source_uuid),
         {:ok, enabled} <- enabled_value(body) do
      cond do
        enabled and not controller_enabled?() ->
          Response.error(conn, ElixirDB.Error.invalid_request("shadow controller is disabled"))

        enabled ->
          enable(conn, source_uuid, body)

        true ->
          disable(conn, source_uuid)
      end
    else
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp update_shadow(conn, _body),
    do:
      Response.error(conn, ElixirDB.Error.invalid_request("shadow configuration must be an object"))

  defp enable(conn, source_uuid, body) do
    current = current_definition(source_uuid)

    attrs = %{
      "location" => body["location"],
      "attachment_location" => body["attachment_location"]
    }

    with {:ok, definition} <- next_definition(source_uuid, current, attrs),
         :ok <- RouteTable.delete(source_uuid),
         {:ok, definition} <- Registry.put_desired(definition),
         :ok <- enqueue(definition) do
      Response.ok(conn, redacted_definition(definition, :provisioning), 202)
    else
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp disable(conn, source_uuid) do
    with :ok <- RouteTable.delete(source_uuid),
         {:ok, definition} <- Registry.disable(source_uuid),
         :ok <- enqueue(definition) do
      Response.ok(conn, redacted_definition(definition, :destroying), 202)
    else
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp enqueue(definition) do
    case Reconciler.enqueue(definition) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("shadow reconciliation could not be scheduled", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp current_definition(source_uuid) do
    case Registry.get(source_uuid) do
      {:ok, %{desired: definition}} -> definition
      _ -> nil
    end
  end

  defp next_definition(_source_uuid, %Definition{enabled: true} = current, attrs) do
    if current.location == attrs["location"] and
         current.attachment_location == attrs["attachment_location"],
       do: {:ok, current},
       else: Definition.replace(current, attrs)
  end

  defp next_definition(_source_uuid, %Definition{} = current, attrs),
    do: Definition.replace(current, Map.put(attrs, "generation", current.generation + 1))

  defp next_definition(source_uuid, nil, attrs),
    do: Definition.new(source_uuid, Map.put(attrs, "generation", 1))

  defp ordinary_source?(source_uuid) do
    case DatabaseCatalog.list() do
      {:ok, entries} ->
        if Enum.any?(entries, &(&1.database_uuid == source_uuid and &1.database_kind == :ordinary)),
          do: :ok,
          else:
            {:error,
             ElixirDB.Error.database_not_found(
               "source database is not an ordinary registered database"
             )}

      {:error, error} ->
        {:error, error}
    end
  end

  defp source_uuid_valid(value) when is_binary(value) do
    if Regex.match?(@uuid_pattern, value),
      do: :ok,
      else: {:error, ElixirDB.Error.database_not_found("source database is not registered")}
  end

  defp source_uuid_valid(_),
    do: {:error, ElixirDB.Error.database_not_found("source database is not registered")}

  defp controller_enabled?,
    do: Keyword.get(Application.get_env(:elixir_db, :shadow_controller, []), :enabled, false)

  defp enabled_value(%{"enabled" => enabled}) when is_boolean(enabled), do: {:ok, enabled}

  defp enabled_value(_),
    do: {:error, ElixirDB.Error.invalid_request("shadow enabled must be a boolean")}

  defp redacted_entry(%{desired: desired, observed: observed, orphans: orphans}) do
    %{
      "enabled" => if(desired, do: desired.enabled, else: false),
      "desired" => if(desired, do: redacted_definition(desired, observed.state), else: nil),
      "observed" => %{
        "state" => Atom.to_string(observed.state),
        "worker_node_id" => observed.worker_node_id,
        "applied_source_sequence" => observed.applied_source_sequence,
        "last_error_code" =>
          if(observed.last_error_code, do: Atom.to_string(observed.last_error_code)),
        "updated_at" => observed.updated_at
      },
      "orphans" => Enum.map(orphans, &redacted_orphan/1)
    }
  end

  defp redacted_definition(definition, state) do
    %{
      "enabled" => definition.enabled,
      "location" => definition.location,
      "attachment_location" => definition.attachment_location,
      "generation" => definition.generation,
      "shadow_uuid" => definition.shadow_uuid,
      "operation_id" => definition.operation_id,
      "state" => Atom.to_string(state)
    }
  end

  defp redacted_orphan(orphan) when is_map(orphan),
    do: Map.drop(orphan, ["token", "path", "storage_root"])

  defp redacted_orphan(_), do: %{}
end
