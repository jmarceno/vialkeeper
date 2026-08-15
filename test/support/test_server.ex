defmodule VialKeeper.TestServer do
  @moduledoc """
  Starts an ephemeral Bandit/HTTP server for end-to-end tests.

  Binds loopback on an OS-assigned port so concurrent e2e scenarios do not
  collide with the application listener or each other.
  """

  @doc """
  Starts a temporary HTTP server and returns its pid, port, and base URL.
  """
  @spec start(keyword()) :: {:ok, map()} | {:error, term()}
  def start(opts \\ []) do
    plug = Keyword.get(opts, :plug, VialKeeper.HTTP.Router)
    request_hook = Keyword.get(opts, :request_hook)
    request_body_hook = Keyword.get(opts, :request_body_hook)
    ip = Keyword.get(opts, :ip, {127, 0, 0, 1})
    port = Keyword.get(opts, :port, 0)

    bandit_opts = [
      plug: {__MODULE__.HookPlug, {plug, request_hook, request_body_hook}},
      scheme: :http,
      ip: ip,
      port: port,
      http_options: [compress: false]
    ]

    case Bandit.start_link(bandit_opts) do
      {:ok, pid} ->
        {:ok, {bound_ip, bound_port}} = ThousandIsland.listener_info(pid)
        host = format_ip(bound_ip)

        {:ok,
         %{
           pid: pid,
           ip: bound_ip,
           port: bound_port,
           base_url: "http://#{host}:#{bound_port}"
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stops a server started by `start/1`.
  """
  @spec stop(map() | pid()) :: :ok
  def stop(%{pid: pid}), do: stop(pid)

  def stop(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        GenServer.stop(pid, :normal, 5_000)
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc """
  Starts a server and registers ExUnit cleanup for the current test process.
  """
  @spec start_supervised!(keyword()) :: map()
  def start_supervised!(opts \\ []) do
    {:ok, server} = start(opts)

    ExUnit.Callbacks.on_exit(fn ->
      stop(server)
    end)

    server
  end

  defp format_ip({a, b, c, d}), do: "#{a}.#{b}.#{c}.#{d}"
  defp format_ip(ip) when is_tuple(ip), do: :inet.ntoa(ip) |> to_string()

  defmodule HookPlug do
    @moduledoc false

    alias VialKeeper.TestServer.BodyCapture

    @spec init(
            {module(), (Plug.Conn.t() -> Plug.Conn.t()) | nil,
             (BodyCapture.request() -> any()) | nil}
          ) :: tuple()
    def init(opts), do: opts

    @spec call(
            Plug.Conn.t(),
            {module(), (Plug.Conn.t() -> Plug.Conn.t()) | nil,
             (BodyCapture.request() -> any()) | nil}
          ) :: Plug.Conn.t()
    def call(conn, {router, request_hook, request_body_hook}) do
      conn
      |> BodyCapture.wrap(request_body_hook)
      |> call_request_hook(router, request_hook)
      |> BodyCapture.unwrap()
    end

    defp call_request_hook(conn, router, nil), do: router.call(conn, router.init([]))

    defp call_request_hook(conn, router, hook) when is_function(hook, 1) do
      case hook.(conn) do
        {:halt, conn} ->
          conn

        {:after, conn, after_fun} when is_function(after_fun, 0) ->
          conn = router.call(conn, router.init([]))
          after_fun.()
          conn

        conn ->
          router.call(conn, router.init([]))
      end
    end
  end

  defmodule BodyCapture do
    @moduledoc """
    Observes the exact request-body chunks consumed by a test server.

    The callback runs after Bandit reports the final body chunk, so capturing a
    body does not read ahead of the router or alter its stream.
    """

    @behaviour Plug.Conn.Adapter

    @type request :: %{
            method: binary(),
            path: binary(),
            headers: [{binary(), binary()}],
            body: binary()
          }

    @spec wrap(Plug.Conn.t(), (request() -> any()) | nil) :: Plug.Conn.t()
    def wrap(conn, nil), do: conn

    def wrap(%Plug.Conn{adapter: {adapter, adapter_state}} = conn, callback)
        when is_function(callback, 1) do
      request = %{
        method: conn.method,
        path: conn.request_path,
        headers: conn.req_headers,
        body: ""
      }

      %{conn | adapter: {__MODULE__, capture_state(adapter, adapter_state, callback, request)}}
    end

    @spec unwrap(Plug.Conn.t()) :: Plug.Conn.t()
    def unwrap(%Plug.Conn{adapter: {__MODULE__, state}} = conn),
      do: %{conn | adapter: {state.adapter, state.adapter_state}}

    def unwrap(conn), do: conn

    @impl true
    def read_req_body(state, opts) do
      case state.adapter.read_req_body(state.adapter_state, opts) do
        {:more, body, adapter_state} ->
          {:more, body, %{state | adapter_state: adapter_state, chunks: [body | state.chunks]}}

        {:ok, body, adapter_state} ->
          chunks = [body | state.chunks]

          state.callback.(%{
            state.request
            | body: chunks |> Enum.reverse() |> IO.iodata_to_binary()
          })

          {:ok, body, %{state | adapter_state: adapter_state, chunks: chunks}}
      end
    end

    @impl true
    def send_resp(state, status, headers, body),
      do:
        wrap_conn_result(state, state.adapter.send_resp(state.adapter_state, status, headers, body))

    @impl true
    def send_file(state, status, headers, path, offset, length),
      do:
        wrap_conn_result(
          state,
          state.adapter.send_file(state.adapter_state, status, headers, path, offset, length)
        )

    @impl true
    def send_chunked(state, status, headers),
      do:
        wrap_conn_result(
          state,
          state.adapter.send_chunked(state.adapter_state, status, headers)
        )

    @impl true
    def chunk(state, body),
      do: wrap_conn_result(state, state.adapter.chunk(state.adapter_state, body))

    @impl true
    def inform(state, status, headers),
      do:
        wrap_adapter_result(
          state,
          state.adapter.inform(state.adapter_state, status, headers)
        )

    @impl true
    def upgrade(state, protocol, opts),
      do:
        wrap_adapter_result(
          state,
          state.adapter.upgrade(state.adapter_state, protocol, opts)
        )

    @impl true
    def push(state, path, headers), do: state.adapter.push(state.adapter_state, path, headers)

    @impl true
    def get_peer_data(state), do: state.adapter.get_peer_data(state.adapter_state)

    @impl true
    def get_sock_data(state), do: state.adapter.get_sock_data(state.adapter_state)

    @impl true
    def get_ssl_data(state), do: state.adapter.get_ssl_data(state.adapter_state)

    @impl true
    def get_http_protocol(state), do: state.adapter.get_http_protocol(state.adapter_state)

    defp capture_state(adapter, adapter_state, callback, request) do
      %{
        adapter: adapter,
        adapter_state: adapter_state,
        callback: callback,
        request: request,
        chunks: []
      }
    end

    defp wrap_conn_result(state, {:ok, value, adapter_state}),
      do: {:ok, value, %{state | adapter_state: adapter_state}}

    defp wrap_conn_result(_state, {:error, _reason} = error), do: error

    defp wrap_adapter_result(state, {:ok, adapter_state}),
      do: {:ok, %{state | adapter_state: adapter_state}}

    defp wrap_adapter_result(_state, {:error, _reason} = error), do: error
  end
end
