defmodule ElixirDB.HTTP.Request do
  @moduledoc false

  def call(conn, fun) do
    case ElixirDB.HTTP.BodyReader.read(conn) do
      {:ok, body, conn} -> fun.(body, conn)
      {:error, error} -> ElixirDB.HTTP.Response.error(conn, error)
    end
  end

  def uuid(conn), do: conn.path_params["uuid"]
end
