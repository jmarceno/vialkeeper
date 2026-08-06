defmodule ElixirDB.HTTP.Request do
  @moduledoc false

  @doc """
  Reads the request body and invokes `fun.(body, conn)`.

  Pass BodyReader options such as `:allowed_fields` and `:unknown_message` to
  enforce `API-009` unknown top-level field rejection at the HTTP boundary.
  """
  def call(conn, fun) when is_function(fun, 2), do: call(conn, [], fun)

  def call(conn, opts, fun) when is_list(opts) and is_function(fun, 2) do
    case ElixirDB.HTTP.BodyReader.read(conn, opts) do
      {:ok, body, conn} -> fun.(body, conn)
      {:error, error} -> ElixirDB.HTTP.Response.error(conn, error)
    end
  end

  def uuid(conn), do: conn.path_params["uuid"]
end
