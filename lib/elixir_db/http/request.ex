defmodule ElixirDB.HTTP.Request do
  import Plug.Conn, only: [get_req_header: 2]

  alias ElixirDB.HTTP.BodyReader
  alias ElixirDB.HTTP.Response
  @moduledoc "Shared request-body and path-parameter helpers for HTTP routes."

  @doc """
  Reads the request body and invokes `fun.(body, conn)`.

  Pass BodyReader options such as `:allowed_fields` and `:unknown_message` to
  enforce `API-009` unknown top-level field rejection at the HTTP boundary.
  """
  def call(conn, fun) when is_function(fun, 2), do: call(conn, [], fun)

  def call(conn, opts, fun) when is_list(opts) and is_function(fun, 2) do
    case BodyReader.read(conn, opts) do
      {:ok, body, conn} -> fun.(body, conn)
      {:error, error} -> Response.error(conn, error)
    end
  end

  def uuid(conn), do: conn.path_params["uuid"]

  @doc "Parses the optional read-consistency request header."
  def read_consistency(conn) do
    case get_req_header(conn, "x-elixirdb-read-consistency") do
      [] ->
        {:ok, :primary}

      ["primary"] ->
        {:ok, :primary}

      ["eventual"] ->
        {:ok, :eventual}

      [_] ->
        {:error, ElixirDB.Error.invalid_request("read consistency must be primary or eventual")}

      _ ->
        {:error, ElixirDB.Error.invalid_request("read consistency header must appear once")}
    end
  end

  @doc """
  Bounds-checks a URL path parameter used as an identifier (index_id, job_id,
  replication_id).

  Path parameters are raw URL-decoded strings and are never run through the JSON
  body validators, so a 10 MB segment or one containing NUL/control bytes could
  otherwise reach storage or be used as an ETS key unchanged. This guarantees any
  such segment is rejected with a typed 400 at the HTTP boundary.

  Returns `:ok` or `{:error, %ElixirDB.Error{}}`.
  """
  def validate_path_id(nil),
    do: {:error, ElixirDB.Error.invalid_request("path identifier is missing")}

  def validate_path_id(value) when is_binary(value) do
    max = ElixirDB.Config.host_limits()[:max_document_id_bytes] || 512

    cond do
      value == "" ->
        {:error, ElixirDB.Error.invalid_request("path identifier must not be empty")}

      byte_size(value) > max ->
        {:error, ElixirDB.Error.resource_limit("path identifier exceeds the configured limit")}

      String.contains?(value, <<0>>) ->
        {:error, ElixirDB.Error.invalid_request("path identifier contains NUL")}

      not String.valid?(value) ->
        {:error, ElixirDB.Error.invalid_request("path identifier must be valid UTF-8")}

      Enum.any?(String.to_charlist(value), &(&1 < 0x20)) ->
        {:error, ElixirDB.Error.invalid_request("path identifier contains a control character")}

      true ->
        :ok
    end
  end

  def validate_path_id(_),
    do: {:error, ElixirDB.Error.invalid_request("path identifier must be a string")}
end
