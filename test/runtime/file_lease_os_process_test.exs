defmodule ElixirDB.Runtime.FileLeaseOsProcessTest do
  @moduledoc """
  Gap D3: lease exclusion across real OS processes (not same-BEAM GenServers).

  A child `mix run` / `elixir -e` process holds `FileLease`; the parent asserts
  `database_in_use`. OS PIDs must differ.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Runtime.FileLease
  alias ElixirDB.Storage.SQLite.Adapter

  @moduletag :os_process
  @moduletag :slow

  setup do
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-os-lease")
    path = ElixirDB.TempDatabase.sqlite_path(bundle_path)
    {:ok, adapter} = Adapter.create(path, %{})
    :ok = Adapter.close(adapter)

    ready = path <> ".ready"
    stop = path <> ".stop"

    on_exit(fn ->
      _ = File.write(stop, "stop")
      ElixirDB.TempDatabase.cleanup(bundle_path)
      _ = File.rm(ready)
      _ = File.rm(stop)
    end)

    {:ok, path: path, bundle_path: bundle_path, ready: ready, stop: stop}
  end

  test "separate OS process holding FileLease yields database_in_use in parent", %{
    path: path,
    ready: ready,
    stop: stop
  } do
    parent_os_pid = to_string(:os.getpid())
    project = File.cwd!()

    script = """
    {:ok, _} = Application.ensure_all_started(:exqlite)
    path = #{inspect(path)}
    ready = #{inspect(ready)}
    stop = #{inspect(stop)}

    case GenServer.start(ElixirDB.Runtime.FileLease, path) do
      {:ok, lease} ->
        File.write!(ready, "HELD:" <> to_string(:os.getpid()))

        Enum.reduce_while(1..6_000, :ok, fn _, acc ->
          if File.exists?(stop) do
            _ = GenServer.stop(lease)
            {:halt, :ok}
          else
            Process.sleep(10)
            {:cont, acc}
          end
        end)

      {:error, reason} ->
        File.write!(ready, "ERROR:" <> inspect(reason))
        System.halt(1)
    end
    """

    holder =
      Task.async(fn ->
        System.cmd(
          "mix",
          ["run", "--no-start", "-e", script],
          cd: project,
          env: [{"MIX_ENV", "test"}],
          stderr_to_stdout: true
        )
      end)

    ready_body =
      ElixirDB.Eventual.eventually(
        fn ->
          case File.read(ready) do
            {:ok, "HELD:" <> _ = body} -> {:ok, body}
            {:ok, "ERROR:" <> reason} -> flunk("child failed to acquire lease: #{reason}")
            _ -> false
          end
        end,
        timeout: 30_000,
        message: "child OS process did not signal lease held"
      )

    "HELD:" <> child_os_pid = ready_body
    assert child_os_pid != parent_os_pid
    assert String.match?(child_os_pid, ~r/^\d+$/)

    assert {:error, %ElixirDB.Error{code: :database_in_use, retryable: true}} =
             GenServer.start(FileLease, path)

    File.write!(stop, "stop")
    {output, status} = Task.await(holder, 60_000)

    assert status == 0, "child mix run failed (status=#{status}): #{output}"

    assert {:ok, lease} = GenServer.start(FileLease, path)
    assert :ok = GenServer.stop(lease)
  end
end
