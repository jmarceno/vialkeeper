defmodule ElixirDB.HTTP.Routes.Databases do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}
  alias ElixirDB.HTTP.Schemas
  alias ElixirDB.Replication.Wire
  alias ElixirDB.Runtime.DatabaseCatalog
  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      Schemas.opts(:database_create, "database creation contains an unknown field"),
      fn body, conn ->
        path = body["path"] || "#{ElixirDB.UUID.v4()}.db"

        if Map.has_key?(body, "path") and not is_binary(body["path"]) do
          Response.error(conn, ElixirDB.Error.invalid_request("database path must be a string"))
        else
          case body["config"] do
            config when is_map(config) or is_nil(config) ->
              case DatabaseCatalog.create(
                     path,
                     if(config, do: %{config: config}, else: %{})
                   ) do
                {:ok, info} -> Response.ok(conn, info, 201)
                {:error, error} -> Response.error(conn, error)
              end

            _ ->
              Response.error(
                conn,
                ElixirDB.Error.invalid_request("database config must be an object")
              )
          end
        end
      end
    )
  end

  get "/" do
    case DatabaseCatalog.list() do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/:uuid" do
    case DatabaseCatalog.info(Request.uuid(conn)) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/:uuid/config" do
    case DatabaseCatalog.info(Request.uuid(conn)) do
      {:ok, data} -> Response.ok(conn, data.config || %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  put "/:uuid/config" do
    Request.call(conn, fn body, conn ->
      if is_map(body),
        do:
          Response.result(
            conn,
            DatabaseCatalog.command(
              Request.uuid(conn),
              {:command, :update_config, body}
            )
          ),
        else:
          Response.error(conn, ElixirDB.Error.invalid_request("configuration must be an object"))
    end)
  end

  post "/:uuid/close" do
    case DatabaseCatalog.close(Request.uuid(conn)) do
      :ok -> Response.ok(conn, %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  post "/:uuid/integrity-check" do
    case DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :integrity_check, %{}}
         ) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  post "/:uuid/compact" do
    Request.call(
      conn,
      Schemas.opts(:compact_retention, "compact retention contains an unknown field"),
      fn body, conn ->
        Response.result(
          conn,
          compact_result(Request.uuid(conn), body || %{})
        )
      end
    )
  end

  match _ do
    not_found(conn)
  end

  @doc false
  def register(conn) do
    Request.call(
      conn,
      Schemas.opts(:database_register, "registration contains an unknown field"),
      fn body, conn ->
        case DatabaseCatalog.register(body["path"]) do
          {:ok, info} -> Response.ok(conn, info, 201)
          {:error, error} -> Response.error(conn, error)
        end
      end
    )
  end

  @doc false
  def unregister(conn) do
    case DatabaseCatalog.unregister(Request.uuid(conn)) do
      :ok -> Response.ok(conn, %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  defp not_found(conn) do
    Response.error(
      conn,
      ElixirDB.Error.invalid_request("route not found", %{path: conn.request_path})
    )
  end

  defp compact_result(uuid, request) do
    case DatabaseCatalog.command(uuid, {:command, :compact_retention, request}) do
      {:ok, stats} -> {:ok, Wire.compact_stats(stats)}
      error -> error
    end
  end
end
