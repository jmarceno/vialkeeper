defmodule ElixirDB.HTTP.Routes.Databases do
  @moduledoc false
  use Plug.Router
  alias ElixirDB.HTTP.{Request, Response}

  plug(:match)
  plug(:dispatch)

  post "/" do
    Request.call(
      conn,
      [
        allowed_fields: ["path", "config"],
        unknown_message: "database creation contains an unknown field"
      ],
      fn body, conn ->
        path = body["path"] || "#{ElixirDB.UUID.v4()}.db"

        if Map.has_key?(body, "path") and not is_binary(body["path"]) do
          Response.error(conn, ElixirDB.Error.invalid_request("database path must be a string"))
        else
          case body["config"] do
            config when is_map(config) or is_nil(config) ->
              case ElixirDB.Runtime.DatabaseCatalog.create(
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
    case ElixirDB.Runtime.DatabaseCatalog.list() do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/:uuid" do
    case ElixirDB.Runtime.DatabaseCatalog.info(Request.uuid(conn)) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  get "/:uuid/config" do
    case ElixirDB.Runtime.DatabaseCatalog.info(Request.uuid(conn)) do
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
            ElixirDB.Runtime.DatabaseCatalog.command(
              Request.uuid(conn),
              {:command, :update_config, body}
            )
          ),
        else:
          Response.error(conn, ElixirDB.Error.invalid_request("configuration must be an object"))
    end)
  end

  post "/:uuid/close" do
    case ElixirDB.Runtime.DatabaseCatalog.close(Request.uuid(conn)) do
      :ok -> Response.ok(conn, %{})
      {:error, error} -> Response.error(conn, error)
    end
  end

  post "/:uuid/integrity-check" do
    case ElixirDB.Runtime.DatabaseCatalog.command(
           Request.uuid(conn),
           {:command, :integrity_check, %{}}
         ) do
      {:ok, data} -> Response.ok(conn, data)
      {:error, error} -> Response.error(conn, error)
    end
  end

  match _ do
    not_found(conn)
  end

  @doc false
  def register(conn) do
    Request.call(
      conn,
      [
        allowed_fields: ["path"],
        unknown_message: "registration contains an unknown field"
      ],
      fn body, conn ->
        case ElixirDB.Runtime.DatabaseCatalog.register(body["path"]) do
          {:ok, info} -> Response.ok(conn, info, 201)
          {:error, error} -> Response.error(conn, error)
        end
      end
    )
  end

  @doc false
  def unregister(conn) do
    case ElixirDB.Runtime.DatabaseCatalog.unregister(Request.uuid(conn)) do
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
end
