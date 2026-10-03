defmodule VialKeeper.HTTP.Response do
  @moduledoc "Builds stable JSON responses and error envelopes for HTTP routes."
  import Plug.Conn

  require VialKeeper.Probe

  alias VialKeeper.HTTP.EncodedJSON
  alias VialKeeper.MapAccess
  alias VialKeeper.Probe
  alias VialKeeper.Storage.Results

  @json_content_type "application/json; charset=utf-8"
  @unsent [:unset, :set, :set_upgrade, :set_chunked, :set_file]

  @doc """
  Returns the caller's `x-request-id` when it is 1-128 characters of
  `[A-Za-z0-9._-]`, otherwise a fresh UUID.
  """
  @spec request_id(Plug.Conn.t()) :: binary()
  def request_id(conn) do
    case get_req_header(conn, "x-request-id") do
      [value | _] when byte_size(value) <= 128 and value != "" ->
        if request_id_chars?(value), do: value, else: VialKeeper.UUID.v4()

      _ ->
        VialKeeper.UUID.v4()
    end
  end

  defp request_id_chars?(<<>>), do: true

  defp request_id_chars?(<<char, rest::binary>>)
       when char in ?A..?Z or char in ?a..?z or char in ?0..?9 or char in [?., ?_, ?-],
       do: request_id_chars?(rest)

  defp request_id_chars?(_value), do: false

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
    do: ok(conn, public_data(data), status)

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
    |> put_safe_header("x-vialkeeper-read-served-by", "shadow")
    |> put_safe_header("x-vialkeeper-source-watermark", Integer.to_string(watermark))
  end

  def put_read_headers(conn, %{served_by: served_by}) when served_by in ["source", "primary"],
    do: put_safe_header(conn, "x-vialkeeper-read-served-by", "source")

  def put_read_headers(conn, _meta), do: conn

  # Query results carry the same rows as both `documents` and `results` (a
  # documented contract); encode them once and embed the JSON under both keys.
  defp public_data(%{documents: rows, results: rows} = data) when is_list(rows) do
    shared = EncodedJSON.encode(Results.to_public(rows))

    data
    |> Map.drop([:documents, :results])
    |> Results.to_public()
    |> Map.merge(%{"documents" => shared, "results" => shared})
  end

  defp public_data(data), do: Results.to_public(data)

  @spec send_json(Plug.Conn.t(), pos_integer(), map()) :: Plug.Conn.t()
  def send_json(conn, status, body) do
    request_id =
      case MapAccess.get(body, :request_id) do
        nil -> request_id(conn)
        request_id -> request_id
      end

    Probe.measure :http_response_encode do
      conn
      |> put_request_id_header(request_id)
      |> put_safe_header("content-type", @json_content_type)
      |> send_resp(status, JSON.encode_to_iodata!(body))
    end
  end

  defp put_request_id_header(conn, request_id) do
    if request_id_chars?(request_id),
      do: put_safe_header(conn, "x-request-id", request_id),
      else: put_resp_header(conn, "x-request-id", request_id)
  end

  # `put_resp_header/3` checks every value for CR, LF and NUL, compiling a
  # match pattern on each call, which made header writes a visible share of a
  # small response. Values passed here are constants, decimal integers, or
  # request ids limited to `[A-Za-z0-9._-]`, so they are stored directly with
  # the same replace-by-key semantics. Once a response is sent this defers to
  # `put_resp_header/3`, which raises as before.
  defp put_safe_header(%Plug.Conn{state: state, resp_headers: headers} = conn, key, value)
       when state in @unsent,
       do: %{conn | resp_headers: List.keystore(headers, key, 0, {key, value})}

  defp put_safe_header(conn, key, value), do: put_resp_header(conn, key, value)

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
