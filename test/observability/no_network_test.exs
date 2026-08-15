defmodule VialKeeper.Observability.NoNetworkWhenUnconfiguredTest do
  @moduledoc """
  With no otlp_endpoint configured in host.toml, the app
  wires NO exporter and opens no collector connection; with one set, the OTLP
  exporter is wired.

  The gate lives in `config/runtime.exs` and is intentionally skipped in the
  test env (so tests can wire the in-memory exporters), so this test evaluates
  the gate in fresh dev-env VMs via `mix run` — one subprocess per direction.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  @moduletag :slow

  @project_root Path.expand("../..", __DIR__)

  defp fresh_root do
    Path.join(System.tmp_dir!(), "vialkeeper-nonet-#{System.unique_integer([:positive])}")
  end

  test "otlp_endpoint unset: app starts, no exporter is wired, exporter app never starts" do
    root = fresh_root()
    File.mkdir_p!(root)
    # host.toml absent on first run → template created → otlp_endpoint empty.
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
        env: %{"MIX_ENV" => "dev", "VIAL_KEEPER_ROOT" => root}
      )

    _ = File.rm_rf(root)

    assert status == 0, "app failed to start with the endpoint unset:\n#{out}"
    assert out =~ "EXPORTER_STARTED=false", out
    assert out =~ "TRACES_EXPORTER=:none", out
    assert out =~ "READERS=[]", out
  end

  test "otlp_endpoint set: the OTLP exporter and a metric reader are wired" do
    root = fresh_root()
    File.mkdir_p!(root)

    File.write!(Path.join(root, "host.toml"), """
    [observability]
    otlp_endpoint = "http://127.0.0.1:9"
    """)

    script = """
    IO.puts("TRACES_EXPORTER=" <> inspect(Application.get_env(:opentelemetry, :traces_exporter)))
    readers = Application.get_env(:opentelemetry_experimental, :readers)
    IO.puts("READERS=" <> Integer.to_string(length(List.wrap(readers))))
    """

    {out, status} =
      System.cmd("mix", ["run", "--no-start", "-e", script],
        cd: @project_root,
        stderr_to_stdout: true,
        env: %{"MIX_ENV" => "dev", "VIAL_KEEPER_ROOT" => root}
      )

    _ = File.rm_rf(root)

    assert status == 0, "runtime config failed with the endpoint set:\n#{out}"
    assert out =~ "TRACES_EXPORTER={:opentelemetry_exporter", out
    assert out =~ "READERS=1", out
  end
end
