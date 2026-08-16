defmodule VialKeeper.HTTP.Request do
  @moduledoc "Shared request-body and path-parameter helpers for HTTP routes."

  import Plug.Conn, only: [get_req_header: 2]

  alias VialKeeper.Error
  alias VialKeeper.HTTP.BodyReader
  alias VialKeeper.HTTP.Response

  @doc """
  Reads the request body and invokes `fun.(body, conn)`.

  Pass BodyReader options such as `:allowed_fields` and `:unknown_message` to
  enforce `API-009` unknown top-level field rejection at the HTTP boundary.
  """
  @spec call(Plug.Conn.t(), (map(), Plug.Conn.t() -> Plug.Conn.t())) :: Plug.Conn.t()
  def call(conn, fun) when is_function(fun, 2), do: call(conn, [], fun)

  @spec call(Plug.Conn.t(), keyword(), (map(), Plug.Conn.t() -> Plug.Conn.t())) :: Plug.Conn.t()
  def call(conn, opts, fun) when is_list(opts) and is_function(fun, 2) do
    case BodyReader.read(conn, opts) do
      {:ok, body, conn} -> fun.(body, conn)
      {:error, error} -> Response.error(conn, error)
    end
  end

  @spec uuid(Plug.Conn.t()) :: binary() | nil
  def uuid(conn), do: conn.path_params["uuid"]

  @doc "Parses the optional read-consistency request header."
  @spec read_consistency(Plug.Conn.t()) ::
          {:ok, :primary | :eventual} | {:error, Error.t()}
  def read_consistency(conn) do
    case get_req_header(conn, "x-vialkeeper-read-consistency") do
      [] ->
        {:ok, :eventual}

      ["primary"] ->
        {:ok, :primary}

      ["eventual"] ->
        {:ok, :eventual}

      [_] ->
        {:error, Error.invalid_request("read consistency must be primary or eventual")}

      _ ->
        {:error, Error.invalid_request("read consistency header must appear once")}
    end
  end

  @doc """
  Bounds-checks a URL path parameter used as an identifier (index_id, job_id,
  replication_id).

  Path parameters are raw URL-decoded strings and are never run through the JSON
  body validators, so a 10 MB segment or one containing NUL/control bytes could
  otherwise reach storage or be used as an ETS key unchanged. This guarantees any
  such segment is rejected with a typed 400 at the HTTP boundary.

  Returns `:ok` or `{:error, %Error{}}`.
  """
  @spec validate_path_id(term()) :: :ok | {:error, Error.t()}
  def validate_path_id(nil),
    do: {:error, Error.invalid_request("path identifier is missing")}

  def validate_path_id(value) when is_binary(value) do
    max = VialKeeper.Config.host_limits()[:max_document_id_bytes] || 512

    cond do
      value == "" ->
        {:error, Error.invalid_request("path identifier must not be empty")}

      byte_size(value) > max ->
        {:error, Error.resource_limit("path identifier exceeds the configured limit")}

      String.contains?(value, <<0>>) ->
        {:error, Error.invalid_request("path identifier contains NUL")}

      not String.valid?(value) ->
        {:error, Error.invalid_request("path identifier must be valid UTF-8")}

      Enum.any?(String.to_charlist(value), &(&1 < 0x20)) ->
        {:error, Error.invalid_request("path identifier contains a control character")}

      true ->
        :ok
    end
  end

  def validate_path_id(_),
    do: {:error, Error.invalid_request("path identifier must be a string")}
end
