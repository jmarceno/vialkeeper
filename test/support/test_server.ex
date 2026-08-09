defmodule ElixirDB.TestServer do
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
    plug = Keyword.get(opts, :plug, ElixirDB.HTTP.Router)
    request_hook = Keyword.get(opts, :request_hook)
    ip = Keyword.get(opts, :ip, {127, 0, 0, 1})
    port = Keyword.get(opts, :port, 0)

    bandit_opts = [
      plug: {__MODULE__.HookPlug, {plug, request_hook}},
      scheme: :http,
      ip: ip,
      port: port
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

    @spec init({module(), (Plug.Conn.t() -> Plug.Conn.t()) | nil}) :: tuple()
    def init(opts), do: opts

    @spec call(Plug.Conn.t(), {module(), (Plug.Conn.t() -> Plug.Conn.t()) | nil}) ::
            Plug.Conn.t()
    def call(conn, {router, nil}), do: router.call(conn, router.init([]))

    def call(conn, {router, hook}) when is_function(hook, 1) do
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
end
