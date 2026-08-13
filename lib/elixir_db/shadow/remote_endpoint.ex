defmodule ElixirDB.Shadow.RemoteEndpoint do
  @moduledoc "Shadow endpoint backed by an authenticated remote worker."

  @behaviour ElixirDB.Shadow.Endpoint

  alias ElixirDB.Error
  alias ElixirDB.Replication.BlobRepresentationStream
  alias ElixirDB.Shadow.{Protocol, RemoteTransport}

  defstruct [:base_url, :auth_token, :control_timeout_ms, :read_timeout_ms]

  @type t :: %__MODULE__{
          base_url: binary(),
          auth_token: binary(),
          control_timeout_ms: pos_integer(),
          read_timeout_ms: pos_integer()
        }

  @spec new(map()) :: {:ok, t()} | {:error, Error.t()}
  def new(attrs) when is_map(attrs) do
    attrs = Protocol.string_keys(attrs)
    base_url = String.trim(attrs["control_base_url"] || attrs["base_url"] || "")
    auth_token = attrs["control_bearer_token"] || attrs["auth_token"]

    with :ok <- valid_origin(base_url),
         :ok <- required_text(auth_token, "control bearer token"),
         {:ok, control_timeout_ms} <- positive_timeout(attrs["control_timeout_ms"]),
         {:ok, read_timeout_ms} <- positive_timeout(attrs["read_timeout_ms"]) do
      {:ok,
       %__MODULE__{
         base_url: base_url,
         auth_token: auth_token,
         control_timeout_ms: control_timeout_ms,
         read_timeout_ms: read_timeout_ms
       }}
    end
  end

  def new(_), do: {:error, Error.invalid_request("remote shadow endpoint is invalid")}

  @impl true
  def capabilities(%__MODULE__{} = endpoint, timeout),
    do: call(endpoint, :get, "/v1/control-plane/capabilities", nil, timeout)

  @impl true
  def provision(%__MODULE__{} = endpoint, request, timeout),
    do: call(endpoint, :put, generation_path(request), request, timeout)

  @impl true
  def inspect(%__MODULE__{} = endpoint, request, timeout),
    do: call(endpoint, :get, generation_path(request), nil, timeout)

  @impl true
  def destroy(%__MODULE__{} = endpoint, request, timeout),
    do: call(endpoint, :delete, generation_path(request), nil, timeout)

  @impl true
  def read_document(%__MODULE__{} = endpoint, request, timeout, _opts),
    do: call(endpoint, :post, generation_path(request) <> "/reads/document", request, timeout)

  @impl true
  def bulk_read_documents(%__MODULE__{} = endpoint, request, timeout, _opts),
    do:
      call(
        endpoint,
        :post,
        generation_path(request) <> "/reads/documents/bulk",
        request,
        timeout
      )

  @impl true
  def open_attachment_representation(%__MODULE__{} = endpoint, request, timeout, _opts) do
    with {:ok, descriptor, body, watermark, content_type} <-
           RemoteTransport.open_stream(
             endpoint.base_url,
             generation_path(request) <> "/reads/attachment",
             request,
             nil,
             endpoint.auth_token,
             timeout
           ),
         {:ok, stream} <- BlobRepresentationStream.new(Map.put(descriptor, :body, body)) do
      {:ok,
       %{
         "stream" => stream,
         "source_watermark" => watermark,
         "attachment" => %{"blob" => stream.logical_digest, "content_type" => content_type}
       }}
    end
  end

  defp call(endpoint, method, path, body, timeout) do
    RemoteTransport.request(
      endpoint.base_url,
      method,
      path,
      body,
      endpoint.auth_token,
      timeout
    )
  end

  defp generation_path(request) when is_map(request) do
    request = Protocol.string_keys(request)
    source = URI.encode_www_form(request["source_uuid"] || "")
    generation = request["generation"] || ""
    "/v1/control-plane/shadows/#{source}/generations/#{generation}"
  end

  defp generation_path(_), do: "/v1/control-plane/shadows//generations/"

  defp valid_origin(value) do
    uri = URI.parse(value)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.userinfo == nil and
         uri.query == nil and uri.fragment == nil and uri.path in [nil, "", "/"],
       do: :ok,
       else: {:error, Error.invalid_request("remote shadow endpoint URL is invalid")}
  end

  defp required_text(value, field) when is_binary(value) do
    if String.trim(value) != "",
      do: :ok,
      else: {:error, Error.invalid_request("#{field} is required")}
  end

  defp required_text(_, field), do: {:error, Error.invalid_request("#{field} is required")}

  defp positive_timeout(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp positive_timeout(_),
    do: {:error, Error.invalid_request("shadow endpoint timeout must be positive")}
end
