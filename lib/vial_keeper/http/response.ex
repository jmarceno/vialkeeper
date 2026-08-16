defmodule VialKeeper.HTTP.Response do
  @moduledoc "Builds stable JSON responses and error envelopes for HTTP routes."
  import Plug.Conn

  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.Results

  @spec request_id(Plug.Conn.t()) :: binary()
  def request_id(conn) do
    case get_req_header(conn, "x-request-id") do
      [value | _] when byte_size(value) <= 128 and value != "" ->
        if Regex.match?(~r/^[A-Za-z0-9._-]+$/, value), do: value, else: VialKeeper.UUID.v4()

      _ ->
        VialKeeper.UUID.v4()
    end
  end

  @spec ok(Plug.Conn.t(), term()) :: Plug.Conn.t()
  @spec ok(Plug.Conn.t(), term(), pos_integer()) :: Plug.Conn.t()
  def ok(conn, data, status \\ 200) do
    send_json(conn, status, %{"request_id" => request_id(conn), "data" => data})
  end

  @spec error(Plug.Conn.t(), VialKeeper.Error.t()) :: Plug.Conn.t()
  def error(conn, %VialKeeper.Error{} = error),
    do:
      send_json(conn, error.http_status, %{
        "request_id" => request_id(conn),
        "error" => VialKeeper.Error.public(error)
      })

  @spec result(Plug.Conn.t(), :ok | {:ok, term()} | {:error, VialKeeper.Error.t()}) ::
          Plug.Conn.t()
  @spec result(Plug.Conn.t(), :ok | {:ok, term()} | {:error, VialKeeper.Error.t()}, pos_integer()) ::
          Plug.Conn.t()
  def result(conn, result, status \\ 200)

  def result(conn, {:ok, data}, status),
    do: ok(conn, Results.to_public(data), status)

  def result(conn, {:error, error}, _status), do: error(conn, error)
  def result(conn, :ok, status), do: ok(conn, %{}, status)

  @spec result_with_read_meta(
          Plug.Conn.t(),
          {:ok, term(), map()} | {:error, VialKeeper.Error.t()}
        ) :: Plug.Conn.t()
  @spec result_with_read_meta(
          Plug.Conn.t(),
          {:ok, term(), map()} | {:error, VialKeeper.Error.t()},
          pos_integer()
        ) :: Plug.Conn.t()
  def result_with_read_meta(conn, result, status \\ 200)

  def result_with_read_meta(conn, {:ok, data, meta}, status) when is_map(meta) do
    conn = put_read_headers(conn, meta)
    result(conn, {:ok, data}, status)
  end

  def result_with_read_meta(conn, {:error, _} = error, _status), do: result(conn, error)

  @spec put_read_headers(Plug.Conn.t(), map()) :: Plug.Conn.t()
  def put_read_headers(conn, %{served_by: "shadow", source_watermark: watermark})
      when is_integer(watermark) and watermark >= 0 do
    conn
    |> put_resp_header("x-vialkeeper-read-served-by", "shadow")
    |> put_resp_header("x-vialkeeper-source-watermark", Integer.to_string(watermark))
  end

  def put_read_headers(conn, %{served_by: served_by}) when served_by in ["source", "primary"],
    do: put_resp_header(conn, "x-vialkeeper-read-served-by", "source")

  def put_read_headers(conn, _meta), do: conn

  @spec send_json(Plug.Conn.t(), pos_integer(), map()) :: Plug.Conn.t()
  def send_json(conn, status, body) do
    request_id = MapAccess.get(body, :request_id, request_id(conn))

    conn
    |> put_resp_header("x-request-id", request_id)
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode_to_iodata!(body))
  end

  @doc "Streams binary chunks onto an already-chunked response connection."
  @spec stream_chunks(Plug.Conn.t(), Enumerable.t()) :: Plug.Conn.t()
  def stream_chunks(conn, enumerable) do
    Enum.reduce_while(enumerable, conn, &stream_chunk/2)
  end

  defp stream_chunk({:error, %VialKeeper.Error{}}, conn), do: {:halt, conn}

  defp stream_chunk(chunk, conn) when is_binary(chunk) do
    case chunk(conn, chunk) do
      {:ok, conn} -> {:cont, conn}
      {:error, :closed} -> {:halt, conn}
    end
  end
end
