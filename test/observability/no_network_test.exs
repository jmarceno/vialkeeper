defmodule ElixirDB.Observability.NoNetworkWhenUnconfiguredTest do
  @moduledoc """
  Plan §9 acceptance: with `ELIXIRDB_OTLP_ENDPOINT` unset, the app wires NO
  exporter and opens no collector connection; with it set, the OTLP exporter
  is wired.

  The gate lives in `config/runtime.exs` and is intentionally skipped in the
  test env (so tests can wire the in-memory exporters), so this test evaluates
  the gate in fresh dev-env VMs via `mix run` — one subprocess per direction.
  """

  use ExUnit.Case, async: false

  @moduletag :slow

  @project_root Path.expand("../..", __DIR__)

  test "env unset: app starts, no exporter is wired, exporter app never starts" do
    script = """
    apps = Enum.map(Application.started_applications(), &elem(&1, 0))
    IO.puts("EXPORTER_STARTED=" <> inspect(:opentelemetry_exporter in apps))
    IO.puts("TRACES_EXPORTER=" <> inspect(Application.get_env(:opentelemetry, :traces_exporter)))
    IO.puts("READERS=" <> inspect(Application.get_env(:opentelemetry_experimental, :readers)))
    """

    {out, status} =
      System.cmd("mix", ["run", "-e", script],
        cd: @project_root,
        stderr_to_stdout: true,
        env: %{
          # nil removes the variable from the subprocess environment.
          "ELIXIRDB_OTLP_ENDPOINT" => nil,
          "MIX_ENV" => "dev",
          # Bind an OS-assigned port so the probe cannot collide.
          "ELIXIR_DB_PORT" => "0"
        }
      )

    assert status == 0, "app failed to start with the endpoint unset:\n#{out}"
    assert out =~ "EXPORTER_STARTED=false", out
    assert out =~ "TRACES_EXPORTER=:none", out
    assert out =~ "READERS=[]", out
  end

  test "env set: the OTLP exporter and a metric reader are wired" do
    script = """
    IO.puts("TRACES_EXPORTER=" <> inspect(Application.get_env(:opentelemetry, :traces_exporter)))
    readers = Application.get_env(:opentelemetry_experimental, :readers)
    IO.puts("READERS=" <> Integer.to_string(length(List.wrap(readers))))
    """

    {out, status} =
      System.cmd("mix", ["run", "--no-start", "-e", script],
        cd: @project_root,
        stderr_to_stdout: true,
        env: %{
          # Unroutable loopback port: config wiring is asserted WITHOUT any
          # real collector behind it.
          "ELIXIRDB_OTLP_ENDPOINT" => "http://127.0.0.1:9",
          "MIX_ENV" => "dev"
        }
      )

    assert status == 0, "runtime config failed with the endpoint set:\n#{out}"
    assert out =~ "TRACES_EXPORTER={:opentelemetry_exporter", out
    assert out =~ "READERS=1", out
  end
end
