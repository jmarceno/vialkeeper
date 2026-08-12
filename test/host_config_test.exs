defmodule ElixirDB.HostConfigTest do
  @moduledoc """
  Host configuration loader (`CONFIG-001`): first-run template creation,
  never-overwrite semantics, field-level validation errors, auth/TLS guards,
  and the template/defaults drift test.
  """
  use ExUnit.Case, async: false

  alias ElixirDB.Federation.SavedQueries
  alias ElixirDB.HostConfig

  # SHA-256 digest of a known token, used by auth round-trip cases.
  @known_token "deadbeef" <> String.duplicate("00", 28)
  @known_digest String.downcase(:crypto.hash(:sha256, @known_token) |> Base.encode16(case: :lower))

  setup do
    previous_root = System.get_env("ELIXIR_DB_ROOT")
    dir = Path.join(System.tmp_dir!(), "elixirdb-hostcfg-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      if previous_root,
        do: System.put_env("ELIXIR_DB_ROOT", previous_root),
        else: System.delete_env("ELIXIR_DB_ROOT")

      _ = File.rm_rf(dir)
    end)

    System.put_env("ELIXIR_DB_ROOT", dir)
    {:ok, dir: dir}
  end

  defp host_toml(dir), do: Path.join(dir, "host.toml")

  defp write_config(dir, contents) do
    File.write!(host_toml(dir), contents)
  end

  test "first run in empty root creates host.toml equal to the shipped template", %{dir: dir} do
    refute File.exists?(host_toml(dir))

    assert {:ok, _} = HostConfig.load_from(dir)
    assert File.exists?(host_toml(dir))

    template =
      :code.priv_dir(:elixir_db)
      |> Path.join("host.toml")
      |> File.read!()

    assert template == File.read!(host_toml(dir))
  end

  test "an existing host.toml is never overwritten", %{dir: dir} do
    write_config(dir, "[listener]\nip = \"127.0.0.1\"\nport = 9999\n")

    assert {:ok, _} = HostConfig.load_from(dir)
    assert String.contains?(File.read!(host_toml(dir)), "port = 9999")
  end

  test "absent root is auto-created", %{dir: dir} do
    nested = Path.join(dir, "does-not-exist-yet")

    assert {:ok, _} = HostConfig.load_from(nested)
    assert File.exists?(Path.join(nested, "host.toml"))
  end

  test "default load yields loopback listener and disabled auth/tls", %{dir: dir} do
    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :listener) == [ip: {127, 0, 0, 1}, port: 4000]
    assert Keyword.get(config, :auth)[:enabled] == false
    assert Keyword.get(config, :tls)[:enabled] == false
    assert Keyword.get(config, :security)[:allow_insecure_remote] == false
    assert Keyword.get(config, :otlp_endpoint) == ""
    assert Keyword.get(config, :web_ui) == [enabled: true]
    # host_limits defaults are populated
    limits = Keyword.get(config, :host_limits) |> Map.new()
    assert limits[:max_document_bytes] == 1_048_576
    assert limits[:admission_limit] == 128
    assert limits[:max_replication_concurrent_chain_fetches] == 32
    assert limits[:max_replication_concurrent_blob_transfers] == 32
    assert limits[:max_replication_transfer_bytes_in_flight] == 4_294_967_296
    assert limits[:max_views_per_database] == 256
    assert limits[:max_view_batch_changes] == 500
    assert limits[:max_view_consistent_wait_ms] == 30_000

    policy = Keyword.get(config, :admission_policy) |> Map.new()
    assert policy[:foreground_weight] == 8
    assert policy[:subscription_weight] == 4
    assert policy[:replication_weight] == 2
    assert policy[:maintenance_weight] == 1
    assert policy[:foreground_reserved_slots] == 1
    assert policy[:subscription_reserved_slots] == 1
    assert policy[:replication_reserved_slots] == 1
    assert policy[:maintenance_reserved_slots] == 1
  end

  test "listener ip and port override", %{dir: dir} do
    write_config(dir, "[listener]\nip = \"10.0.0.5\"\nport = 8080\n")

    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :listener) == [ip: {10, 0, 0, 5}, port: 8080]
  end

  test "invalid listener ip is named in the error", %{dir: dir} do
    write_config(dir, "[listener]\nip = \"not-an-ip\"\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "listener.ip")
  end

  test "out-of-range port is named in the error", %{dir: dir} do
    write_config(dir, "[listener]\nport = 99999\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "listener.port")
  end

  test "unknown section is named in the error", %{dir: dir} do
    write_config(dir, "[unknown_section]\nfoo = 1\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "unknown section")
  end

  test "unknown key within a section is named in the error", %{dir: dir} do
    write_config(dir, "[listener]\nbogus = true\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "bogus")
  end

  test "non-positive limit is named in the error", %{dir: dir} do
    write_config(dir, "[limits]\nmax_open_databases = 0\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "max_open_databases")
  end

  test "partial limits override still validates admission against default admission_limit", %{
    dir: dir
  } do
    write_config(dir, "[limits]\nmax_open_databases = 32\n")

    assert {:ok, config} = HostConfig.load_from(dir)
    limits = Keyword.get(config, :host_limits) |> Map.new()
    assert limits[:max_open_databases] == 32
    assert limits[:admission_limit] == 128

    policy = Keyword.get(config, :admission_policy) |> Map.new()
    assert policy[:foreground_weight] == 8
    assert policy[:foreground_reserved_slots] == 1
  end

  test "auth enabled with empty tokens is rejected", %{dir: dir} do
    write_config(dir, "[auth]\nenabled = true\ntokens = []\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "auth.tokens")
  end

  test "auth enabled with a valid digest loads the digest list", %{dir: dir} do
    write_config(dir, "[auth]\nenabled = true\ntokens = [\"#{@known_digest}\"]\n")

    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :auth)[:enabled] == true
    assert Keyword.get(config, :auth)[:token_digests] == [@known_digest]
  end

  test "auth token of wrong length is rejected", %{dir: dir} do
    write_config(dir, "[auth]\nenabled = true\ntokens = [\"abc\"]\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "SHA-256")
  end

  test "non-hex auth token is rejected", %{dir: dir} do
    bad = String.duplicate("z", 64)
    write_config(dir, "[auth]\nenabled = true\ntokens = [\"#{bad}\"]\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "hexadecimal")
  end

  test "tls enabled with missing cert file is rejected", %{dir: dir} do
    write_config(
      dir,
      "[tls]\nenabled = true\ncertfile = \"missing.pem\"\nkeyfile = \"missing.pem\"\n"
    )

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "cannot be read")
  end

  test "tls enabled with cert escaping the root is rejected", %{dir: dir} do
    write_config(
      dir,
      "[tls]\nenabled = true\ncertfile = \"../escape.pem\"\nkeyfile = \"../escape.pem\"\n"
    )

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "escapes the database root")
  end

  test "tls enabled with a symlink escaping the root is rejected", %{dir: dir} do
    outside =
      Path.join(
        System.tmp_dir!(),
        "elixirdb-outside-cert-#{System.unique_integer([:positive])}"
      )

    File.write!(outside, "certificate")
    on_exit(fn -> File.rm(outside) end)

    assert :ok = File.ln_s(outside, Path.join(dir, "cert.pem"))
    File.write!(Path.join(dir, "key.pem"), "key")
    write_config(dir, "[tls]\nenabled = true\ncertfile = \"cert.pem\"\nkeyfile = \"key.pem\"\n")

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "escapes the database root")
  end

  test "tls enabled with readable in-root cert loads", %{dir: dir} do
    File.write!(Path.join(dir, "cert.pem"), "fake cert")
    File.write!(Path.join(dir, "key.pem"), "fake key")
    write_config(dir, "[tls]\nenabled = true\ncertfile = \"cert.pem\"\nkeyfile = \"key.pem\"\n")

    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :tls)[:enabled] == true
  end

  test "shipped template decodes to the compiled defaults (no drift)" do
    template =
      :code.priv_dir(:elixir_db)
      |> Path.join("host.toml")
      |> File.read!()

    {:ok, parsed} = Toml.decode(template)
    assert parsed == HostConfig.defaults()
  end

  test "otlp_endpoint is parsed when present", %{dir: dir} do
    write_config(dir, "[observability]\notlp_endpoint = \"http://collector:4318\"\n")

    assert {:ok, config} = HostConfig.load_from(dir)
    assert Keyword.get(config, :otlp_endpoint) == "http://collector:4318"
  end

  test "admission policy defaults load from template", %{dir: dir} do
    assert {:ok, config} = HostConfig.load_from(dir)
    policy = Keyword.get(config, :admission_policy) |> Map.new()
    assert policy[:foreground_weight] == 8
    assert policy[:maintenance_reserved_slots] == 1
  end

  test "admission weight out of range is named in the error", %{dir: dir} do
    write_config(dir, "[admission]\nforeground_weight = 0\n")

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "admission.foreground_weight")
  end

  test "admission reserved slot above limit is named in the error", %{dir: dir} do
    write_config(dir, "[admission]\nforeground_reserved_slots = 200\n")

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "admission.foreground_reserved_slots")
  end

  test "sum of reserved slots exceeding admission_limit is rejected", %{dir: dir} do
    write_config(
      dir,
      """
      [limits]
      admission_limit = 3

      [admission]
      foreground_reserved_slots = 2
      subscription_reserved_slots = 2
      """
    )

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "reserved slots")
  end

  test "admission negative reserved slot is named in the error", %{dir: dir} do
    write_config(dir, "[admission]\nforeground_reserved_slots = -1\n")

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "admission.foreground_reserved_slots")
  end

  test "unknown admission key is named in the error", %{dir: dir} do
    write_config(dir, "[admission]\nunknown_weight = 1\n")

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "unknown_weight")
  end

  test "federation bounds and relationships are validated", %{dir: dir} do
    write_config(dir, "[federation]\nmax_sources = 2\nmax_concurrent_sources = 3\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "max_concurrent_sources")

    write_config(
      dir,
      "[federation]\nmax_sources = 2\nmax_concurrent_sources = 2\nmax_candidates = 1\n"
    )

    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "max_candidates")

    write_config(dir, "[federation]\nmax_sources = 257\n")
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "max_sources")
  end

  test "saved query JSON is normalized and registration is deferred", %{dir: dir} do
    write_config(
      dir,
      """
      [federation]

      [[federation.saved_query]]
      name = "open"
      sources = ["123e4567-e89b-12d3-a456-426614174000"]
      query_json = '{"selector":{"/state":"open"}}'
      """
    )

    assert {:ok, config} = HostConfig.load_from(dir)
    [saved] = config[:federation][:saved_queries]
    assert saved.name == "open"
    assert saved.databases == ["123e4567-e89b-12d3-a456-426614174000"]
    assert saved.query.selector == %{"/state" => "open"}
    assert is_binary(saved.fingerprint)
    assert SavedQueries.list() == []
  end

  test "saved query names and bookmarks are rejected", %{dir: dir} do
    duplicate = """
    [federation]
    [[federation.saved_query]]
    name = "same"
    sources = ["123e4567-e89b-12d3-a456-426614174000"]
    query_json = '{}'
    [[federation.saved_query]]
    name = "same"
    sources = ["123e4567-e89b-12d3-a456-426614174001"]
    query_json = '{}'
    """

    write_config(dir, duplicate)
    assert {:error, msg} = HostConfig.load_from(dir)
    assert String.contains?(msg, "names must be unique")
  end

  test "saved query bookmarks are rejected", %{dir: dir} do
    write_config(
      dir,
      """
      [federation]
      saved_query = [{name = "bookmark", sources = ["123e4567-e89b-12d3-a456-426614174000"], query_json = '{"bookmark":"x"}'}]
      """
    )

    assert {:error, _msg} = HostConfig.load_from(dir)
  end
end
