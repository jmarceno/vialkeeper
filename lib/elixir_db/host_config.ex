defmodule ElixirDB.HostConfig do
  @moduledoc """
  Host configuration loaded from `<database_root>/host.toml` (`CONFIG-001`).

  The single TOML file is the operator-facing configuration layer; compiled
  defaults are the floor. On first run in an empty root the file is created
  from a shipped template and then never overwritten, so copying the database
  root relocates a complete, working configuration.
  """

  alias ElixirDB.Federation.Normalizer
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.PathSafety
  alias ElixirDB.Runtime.{AdmissionPolicy, AtomicWrite}

  @filename "host.toml"

  # Floor defaults. The single source of truth for `priv/host.toml`; the drift
  # test asserts the shipped template decodes to exactly this map.
  @default_listener %{"ip" => "127.0.0.1", "port" => 4000}

  @default_limits %{
    "max_document_bytes" => 1_048_576,
    "max_request_bytes" => 2_097_152,
    "max_document_id_bytes" => 512,
    "max_bulk_operations" => 500,
    "max_query_results" => 500,
    "max_changes_batch" => 500,
    "max_replication_batch_documents" => 500,
    "max_replication_batch_bytes" => 16_777_216,
    "max_replication_attempts" => 32,
    "max_replication_delay_ms" => 300_000,
    "max_full_scan_documents" => 1_000,
    "max_query_execution_ms" => 5_000,
    "max_wait_ms" => 30_000,
    "max_open_databases" => 64,
    "max_replication_workers" => 32,
    "max_replication_concurrent_chain_fetches" => 32,
    "max_replication_concurrent_blob_transfers" => 32,
    "max_replication_transfer_bytes_in_flight" => 4_294_967_296,
    "admission_limit" => 128,
    "read_pool_size" => 4,
    "read_queue_limit" => 128,
    "max_json_nesting_depth" => 100,
    "max_attachment_bytes" => 4_294_967_296,
    "max_concurrent_attachment_reads" => 1024,
    "max_concurrent_attachment_writes" => 256,
    "max_query_subscriptions" => 4_096,
    "max_query_subscription_members" => 10_000,
    "max_query_subscription_buffered_events" => 4_096,
    "max_query_subscription_heartbeat_ms" => 300_000,
    "max_views_per_database" => 256,
    "max_view_batch_changes" => 500,
    "max_view_consistent_wait_ms" => 30_000,
    "max_materialized_view_sources" => 32,
    "max_materialized_view_concurrent_sources" => 16,
    "max_materialized_view_batch_documents" => 500,
    "max_materialized_view_retry_delay_ms" => 300_000
  }

  @limit_key_atoms %{
    "max_document_bytes" => :max_document_bytes,
    "max_request_bytes" => :max_request_bytes,
    "max_document_id_bytes" => :max_document_id_bytes,
    "max_bulk_operations" => :max_bulk_operations,
    "max_query_results" => :max_query_results,
    "max_changes_batch" => :max_changes_batch,
    "max_replication_batch_documents" => :max_replication_batch_documents,
    "max_replication_batch_bytes" => :max_replication_batch_bytes,
    "max_replication_attempts" => :max_replication_attempts,
    "max_replication_delay_ms" => :max_replication_delay_ms,
    "max_full_scan_documents" => :max_full_scan_documents,
    "max_query_execution_ms" => :max_query_execution_ms,
    "max_wait_ms" => :max_wait_ms,
    "max_open_databases" => :max_open_databases,
    "max_replication_workers" => :max_replication_workers,
    "max_replication_concurrent_chain_fetches" => :max_replication_concurrent_chain_fetches,
    "max_replication_concurrent_blob_transfers" => :max_replication_concurrent_blob_transfers,
    "max_replication_transfer_bytes_in_flight" => :max_replication_transfer_bytes_in_flight,
    "admission_limit" => :admission_limit,
    "read_pool_size" => :read_pool_size,
    "read_queue_limit" => :read_queue_limit,
    "max_json_nesting_depth" => :max_json_nesting_depth,
    "max_attachment_bytes" => :max_attachment_bytes,
    "max_concurrent_attachment_reads" => :max_concurrent_attachment_reads,
    "max_concurrent_attachment_writes" => :max_concurrent_attachment_writes,
    "max_query_subscriptions" => :max_query_subscriptions,
    "max_query_subscription_members" => :max_query_subscription_members,
    "max_query_subscription_buffered_events" => :max_query_subscription_buffered_events,
    "max_query_subscription_heartbeat_ms" => :max_query_subscription_heartbeat_ms,
    "max_views_per_database" => :max_views_per_database,
    "max_view_batch_changes" => :max_view_batch_changes,
    "max_view_consistent_wait_ms" => :max_view_consistent_wait_ms,
    "max_materialized_view_sources" => :max_materialized_view_sources,
    "max_materialized_view_concurrent_sources" => :max_materialized_view_concurrent_sources,
    "max_materialized_view_batch_documents" => :max_materialized_view_batch_documents,
    "max_materialized_view_retry_delay_ms" => :max_materialized_view_retry_delay_ms
  }

  @default_auth %{"enabled" => false, "tokens" => []}
  @default_tls %{"enabled" => false, "certfile" => "cert.pem", "keyfile" => "key.pem"}
  @default_security %{"allow_insecure_remote" => false}
  @default_observability %{"otlp_endpoint" => ""}
  @default_web_ui %{"enabled" => true}
  @default_shadow_controller %{
    "enabled" => false,
    "source_base_url" => "",
    "source_bearer_token" => "",
    "location" => []
  }
  @default_shadow_worker %{
    "enabled" => false,
    "storage_root" => "shadows",
    "control_token_digests" => [],
    "allowed_attachment_roots" => [],
    "allowed_source_origins" => []
  }

  @default_admission AdmissionPolicy.default_toml_map()

  @known_sections ~w(listener limits admission auth tls security observability federation web_ui shadow_controller shadow_worker)
  @allowed_listener ~w(ip port)
  @allowed_auth ~w(enabled tokens)
  @allowed_tls ~w(enabled certfile keyfile)
  @allowed_security ~w(allow_insecure_remote)
  @allowed_observability ~w(otlp_endpoint)
  @allowed_web_ui ~w(enabled)
  @allowed_shadow_controller ~w(enabled source_base_url source_bearer_token location)
  @allowed_shadow_location ~w(name kind control_base_url control_bearer_token control_timeout_ms read_timeout_ms)
  @allowed_shadow_worker ~w(enabled storage_root control_token_digests allowed_attachment_roots allowed_source_origins)
  @allowed_federation ~w(max_sources max_concurrent_sources max_candidates max_execution_ms saved_query)
  @allowed_admission Map.keys(@default_admission)

  @admission_key_atoms %{
    "foreground_weight" => :foreground_weight,
    "subscription_weight" => :subscription_weight,
    "replication_weight" => :replication_weight,
    "maintenance_weight" => :maintenance_weight,
    "foreground_reserved_slots" => :foreground_reserved_slots,
    "subscription_reserved_slots" => :subscription_reserved_slots,
    "replication_reserved_slots" => :replication_reserved_slots,
    "maintenance_reserved_slots" => :maintenance_reserved_slots
  }

  @digest_hex_length 64

  @spec defaults :: map
  def defaults do
    %{
      "listener" => @default_listener,
      "limits" => @default_limits,
      "admission" => @default_admission,
      "auth" => @default_auth,
      "tls" => @default_tls,
      "security" => @default_security,
      "observability" => @default_observability,
      "web_ui" => @default_web_ui,
      "shadow_controller" => @default_shadow_controller,
      "shadow_worker" => @default_shadow_worker,
      "federation" => %{
        "max_sources" => 16,
        "max_concurrent_sources" => 8,
        "max_candidates" => 10_000,
        "max_execution_ms" => 10_000
      }
    }
  end

  @doc """
  Resolves the configured database root from the `ELIXIR_DB_ROOT` environment
  variable, defaulting to `./data` relative to the working directory.
  """
  @spec database_root :: String.t()
  def database_root do
    case System.get_env("ELIXIR_DB_ROOT") do
      value when is_binary(value) and value != "" -> Path.expand(value)
      _ -> Path.expand("data", File.cwd!())
    end
  end

  @doc """
  Loads host configuration from `<database_root>/host.toml`.

  Creates the root and a fully-commented template file on first run. Returns
  `{:ok, keyword}` (a `:elixir_db` config keyword list) or `{:error, reason}`
  with a message naming the offending field.

  The test-environment short-circuit lives in `config/runtime.exs` (which
  guards the call); this function always reads the file. Use `load_from/1` to
  exercise the loader directly against an explicit root.
  """
  @spec load :: {:ok, keyword} | {:error, String.t()}
  def load, do: load_from(database_root())

  @doc """
  Loads host configuration from an explicit root directory.

  Creates the root and the template on first run, then reads, parses, and
  validates `host.toml`. Public so the loader can be exercised independently
  of the `Mix.env()` guard in `load/0`.
  """
  @spec load_from(String.t()) :: {:ok, keyword} | {:error, String.t()}
  def load_from(root) do
    with :ok <- ensure_root(root),
         path = Path.join(root, @filename),
         :ok <- maybe_create_template(path),
         {:ok, contents} <- read_file(path),
         {:ok, raw} <- parse(contents) do
      validate(raw, root)
    end
  end

  defp ensure_root(root) do
    case File.mkdir_p(root) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, "cannot create database root #{inspect(root)}: #{inspect(reason)}"}
    end
  end

  defp maybe_create_template(path) do
    if File.exists?(path) do
      :ok
    else
      template = template_path()

      case File.read(template) do
        {:ok, contents} ->
          AtomicWrite.write(path, contents)

        {:error, reason} ->
          {:error,
           "cannot read host configuration template #{inspect(template)}: #{inspect(reason)}"}
      end
    end
  end

  defp template_path do
    :code.priv_dir(:elixir_db) |> Path.join(@filename)
  end

  defp read_file(path) do
    case File.read(path) do
      {:ok, contents} -> {:ok, contents}
      {:error, reason} -> {:error, "cannot read #{inspect(path)}: #{inspect(reason)}"}
    end
  end

  defp parse(contents) do
    if String.valid?(contents) do
      case Toml.decode(contents) do
        {:ok, map} when is_map(map) -> {:ok, map}
        {:error, reason} -> {:error, "host.toml parse error: #{inspect(reason)}"}
      end
    else
      {:error, "host.toml parse error: contents are not valid UTF-8"}
    end
  end

  # Validation produces a flat `:elixir_db` keyword list.
  defp validate(raw, root) do
    with :ok <- validate_sections(raw),
         {:ok, listener} <- validate_listener(raw["listener"]),
         {:ok, limits} <- validate_limits(raw["limits"]),
         {:ok, admission} <- validate_admission(raw["admission"], limits),
         {:ok, auth} <- validate_auth(raw["auth"]),
         {:ok, tls} <- validate_tls(raw["tls"], root),
         {:ok, security} <- validate_security(raw["security"]),
         {:ok, obs} <- validate_observability(raw["observability"]),
         {:ok, web_ui} <- validate_web_ui(raw["web_ui"]),
         {:ok, shadow_controller} <- validate_shadow_controller(raw["shadow_controller"], limits),
         {:ok, shadow_worker} <- validate_shadow_worker(raw["shadow_worker"], root),
         {:ok, federation} <- validate_federation(raw["federation"], limits) do
      # host_limits consumers (`ElixirDB.Config.host_limits/0`) expect atom keys,
      # matching the original `config/config.exs` keyword form. The keys are
      # bounded by `validate_limits` to the fixed allow-list in @default_limits,
      # so atom-table growth is not a concern here.
      host_limits =
        limits
        |> Enum.map(fn {key, value} -> {@limit_key_atoms[key], value} end)

      config =
        []
        |> Keyword.put(:listener, listener)
        |> Keyword.put(:host_limits, host_limits)
        |> Keyword.put(:admission_policy, admission)
        |> Keyword.put(:auth, auth)
        |> Keyword.put(:tls, tls)
        |> Keyword.put(:security, security)
        |> Keyword.put(:otlp_endpoint, obs)
        |> Keyword.put(:web_ui, web_ui)
        |> Keyword.put(:shadow_controller, shadow_controller)
        |> Keyword.put(:shadow_worker, shadow_worker)
        |> Keyword.put(:federation, federation)

      {:ok, config}
    end
  end

  defp validate_sections(raw) do
    case Map.keys(raw) -- @known_sections do
      [] -> :ok
      unknown -> {:error, "host.toml: unknown section [#{hd(unknown)}]"}
    end
  end

  defp validate_listener(nil), do: {:ok, [ip: {127, 0, 0, 1}, port: 4000]}

  defp validate_listener(%{} = listener) do
    with :ok <- allow_only(listener, @allowed_listener, "listener"),
         :ok <- validate_ip(listener["ip"]),
         :ok <- validate_port(listener["port"]) do
      {:ok, [ip: parse_ip!(listener["ip"]), port: listener["port"]]}
    end
  end

  defp validate_listener(_), do: {:error, "host.toml: [listener] must be a table"}

  defp validate_ip(nil), do: :ok
  defp validate_ip(ip) when is_binary(ip), do: ip_valid?(ip)
  defp validate_ip(_), do: {:error, "host.toml: listener.ip must be an IP address string"}

  defp ip_valid?(ip) do
    case :inet.parse_address(String.to_charlist(ip)) do
      {:ok, _} -> :ok
      {:error, _} -> {:error, "host.toml: listener.ip is not a valid IP address"}
    end
  end

  defp parse_ip!(nil), do: {127, 0, 0, 1}

  defp parse_ip!(ip) do
    {:ok, parsed} = :inet.parse_address(String.to_charlist(ip))
    parsed
  end

  defp validate_port(nil), do: :ok
  defp validate_port(port) when is_integer(port) and port > 0 and port <= 65_535, do: :ok
  defp validate_port(_), do: {:error, "host.toml: listener.port must be an integer 1..65535"}

  defp validate_limits(nil), do: {:ok, @default_limits}

  defp validate_limits(%{} = limits) do
    with :ok <- allow_only(limits, Map.keys(@default_limits), "limits") do
      limits
      |> Enum.reduce_while(:ok, fn {key, value}, :ok ->
        validate_limit_entry(key, value)
      end)
      |> case do
        :ok ->
          validate_transfer_limit(Map.merge(@default_limits, limits))

        error ->
          error
      end
    end
  end

  defp validate_limits(_), do: {:error, "host.toml: [limits] must be a table"}

  defp validate_admission(nil, limits), do: validate_admission(@default_admission, limits)

  defp validate_admission(%{} = admission, limits) do
    merged_limits = Map.merge(@default_limits, limits)
    merged = Map.merge(@default_admission, admission)
    admission_limit = merged_limits["admission_limit"]
    keyword = admission_keyword(merged)

    with :ok <- allow_only(admission, @allowed_admission, "admission"),
         {:ok, _policy} <- AdmissionPolicy.from_keyword(keyword, admission_limit) do
      {:ok, keyword}
    end
  end

  defp validate_admission(_, _limits), do: {:error, "host.toml: [admission] must be a table"}

  defp admission_keyword(admission) do
    Enum.map(admission, fn {key, value} -> {@admission_key_atoms[key], value} end)
  end

  defp validate_limit_entry(key, value) when not is_integer(value),
    do: {:halt, {:error, "host.toml: limits.#{key} must be an integer"}}

  defp validate_limit_entry("read_pool_size", value) when value in 1..32, do: {:cont, :ok}

  defp validate_limit_entry("read_pool_size", _),
    do: {:halt, {:error, "host.toml: limits.read_pool_size must be an integer 1..32"}}

  defp validate_limit_entry("read_queue_limit", value) when value in 1..4096, do: {:cont, :ok}

  defp validate_limit_entry("read_queue_limit", _),
    do: {:halt, {:error, "host.toml: limits.read_queue_limit must be an integer 1..4096"}}

  defp validate_limit_entry(key, value) when value <= 0,
    do: {:halt, {:error, "host.toml: limits.#{key} must be positive"}}

  defp validate_limit_entry(_key, _value), do: {:cont, :ok}

  defp validate_transfer_limit(limits) do
    if limits["max_replication_transfer_bytes_in_flight"] >= limits["max_attachment_bytes"],
      do: {:ok, limits},
      else:
        {:error,
         "host.toml: limits.max_replication_transfer_bytes_in_flight must be at least limits.max_attachment_bytes"}
  end

  defp validate_auth(nil), do: {:ok, [enabled: false, token_digests: []]}

  defp validate_auth(%{} = auth) do
    with :ok <- allow_only(auth, @allowed_auth, "auth"),
         :ok <- validate_bool(auth["enabled"], "auth.enabled"),
         :ok <- validate_tokens(auth["tokens"]) do
      digests = (auth["tokens"] || []) |> Enum.map(&String.downcase/1)

      if auth["enabled"] == true and digests == [] do
        {:error, "host.toml: auth.tokens must list at least one digest when auth is enabled"}
      else
        {:ok, [enabled: auth["enabled"] == true, token_digests: digests]}
      end
    end
  end

  defp validate_auth(_), do: {:error, "host.toml: [auth] must be a table"}

  defp validate_tokens(nil), do: :ok

  defp validate_tokens(tokens) when is_list(tokens) do
    Enum.reduce_while(tokens, :ok, fn token, :ok ->
      cond do
        not is_binary(token) ->
          {:halt, {:error, "host.toml: each auth token must be a string"}}

        byte_size(token) != @digest_hex_length ->
          {:halt,
           {:error,
            "host.toml: auth token must be a #{@digest_hex_length}-character SHA-256 hex digest"}}

        not Regex.match?(~r/^[0-9a-fA-F]+$/, token) ->
          {:halt, {:error, "host.toml: auth token must be a hexadecimal SHA-256 digest"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp validate_tokens(_), do: {:error, "host.toml: auth.tokens must be an array"}

  defp validate_tls(nil, _root),
    do: {:ok, [enabled: false, certfile: "cert.pem", keyfile: "key.pem"]}

  defp validate_tls(%{} = tls, root) do
    with :ok <- allow_only(tls, @allowed_tls, "tls"),
         :ok <- validate_bool(tls["enabled"], "tls.enabled") do
      certfile = tls["certfile"] || "cert.pem"
      keyfile = tls["keyfile"] || "key.pem"

      validate_tls_state(tls["enabled"], certfile, keyfile, root)
    end
  end

  defp validate_tls(_, _root), do: {:error, "host.toml: [tls] must be a table"}

  defp validate_tls_state(true, certfile, keyfile, root) do
    with :ok <- validate_path_inside_root(certfile, root, "tls.certfile"),
         :ok <- validate_path_inside_root(keyfile, root, "tls.keyfile"),
         :ok <- readable?(resolve_path(root, certfile), "tls.certfile"),
         :ok <- readable?(resolve_path(root, keyfile), "tls.keyfile") do
      {:ok, [enabled: true, certfile: certfile, keyfile: keyfile]}
    end
  end

  defp validate_tls_state(_enabled, certfile, keyfile, _root),
    do: {:ok, [enabled: false, certfile: certfile, keyfile: keyfile]}

  defp validate_security(nil), do: {:ok, [allow_insecure_remote: false]}

  defp validate_security(%{} = security) do
    with :ok <- allow_only(security, @allowed_security, "security"),
         :ok <- validate_bool(security["allow_insecure_remote"], "security.allow_insecure_remote") do
      {:ok, [allow_insecure_remote: security["allow_insecure_remote"] == true]}
    end
  end

  defp validate_security(_), do: {:error, "host.toml: [security] must be a table"}

  defp validate_observability(nil), do: {:ok, ""}

  defp validate_observability(%{} = obs) do
    with :ok <- allow_only(obs, @allowed_observability, "observability") do
      case obs["otlp_endpoint"] do
        nil -> {:ok, ""}
        value when is_binary(value) -> {:ok, String.trim(value)}
        _ -> {:error, "host.toml: observability.otlp_endpoint must be a string"}
      end
    end
  end

  defp validate_observability(_), do: {:error, "host.toml: [observability] must be a table"}

  defp validate_web_ui(nil), do: {:ok, [enabled: true]}

  defp validate_web_ui(%{} = web_ui) do
    with :ok <- allow_only(web_ui, @allowed_web_ui, "web_ui"),
         :ok <- validate_bool(web_ui["enabled"], "web_ui.enabled") do
      enabled = if is_boolean(web_ui["enabled"]), do: web_ui["enabled"], else: true
      {:ok, [enabled: enabled]}
    end
  end

  defp validate_web_ui(_), do: {:error, "host.toml: [web_ui] must be a table"}

  defp validate_shadow_controller(nil, limits),
    do: validate_shadow_controller(@default_shadow_controller, limits)

  defp validate_shadow_controller(%{} = controller, limits) do
    with :ok <- allow_only(controller, @allowed_shadow_controller, "shadow_controller"),
         :ok <- validate_bool(controller["enabled"], "shadow_controller.enabled"),
         :ok <-
           validate_optional_text(
             controller["source_base_url"],
             "shadow_controller.source_base_url"
           ),
         :ok <-
           validate_optional_secret(
             controller["source_bearer_token"],
             "shadow_controller.source_bearer_token"
           ),
         {:ok, locations} <- validate_shadow_locations(controller["location"], limits),
         :ok <- validate_shadow_controller_requirements(controller, locations) do
      {:ok,
       [
         enabled: controller["enabled"] == true,
         source_base_url: String.trim(controller["source_base_url"] || ""),
         source_bearer_token: controller["source_bearer_token"] || "",
         locations: locations
       ]}
    end
  end

  defp validate_shadow_controller(_, _limits),
    do: {:error, "host.toml: [shadow_controller] must be a table"}

  defp validate_shadow_controller_requirements(%{"enabled" => true} = controller, locations) do
    remote? = Enum.any?(locations, &(&1.kind == :remote))

    cond do
      locations == [] ->
        {:error, "host.toml: enabled shadow_controller requires at least one location"}

      remote? and String.trim(controller["source_base_url"] || "") == "" ->
        {:error, "host.toml: remote shadow_controller requires source_base_url"}

      remote? and String.trim(controller["source_bearer_token"] || "") == "" ->
        {:error, "host.toml: remote shadow_controller requires source_bearer_token"}

      true ->
        :ok
    end
  end

  defp validate_shadow_controller_requirements(_controller, _locations), do: :ok

  defp validate_shadow_worker(nil, root), do: validate_shadow_worker(@default_shadow_worker, root)

  defp validate_shadow_worker(%{} = worker, root) do
    with :ok <- allow_only(worker, @allowed_shadow_worker, "shadow_worker"),
         :ok <- validate_bool(worker["enabled"], "shadow_worker.enabled"),
         :ok <- validate_storage_root(worker["storage_root"], root),
         :ok <-
           validate_digest_list(
             worker["control_token_digests"],
             "shadow_worker.control_token_digests"
           ),
         {:ok, attachment_roots} <-
           validate_allowed_roots(
             worker["allowed_attachment_roots"],
             "shadow_worker.allowed_attachment_roots"
           ),
         {:ok, origins} <-
           validate_origins(
             worker["allowed_source_origins"],
             "shadow_worker.allowed_source_origins"
           ) do
      {:ok,
       [
         enabled: worker["enabled"] == true,
         storage_root: worker["storage_root"] || "shadows",
         control_token_digests: Enum.map(worker["control_token_digests"] || [], &String.downcase/1),
         allowed_attachment_roots: attachment_roots,
         allowed_source_origins: origins
       ]}
    end
  end

  defp validate_shadow_worker(_, _root),
    do: {:error, "host.toml: [shadow_worker] must be a table"}

  defp validate_shadow_locations(nil, _limits), do: {:ok, []}

  defp validate_shadow_locations(locations, limits) when is_list(locations) do
    Enum.reduce_while(
      locations,
      {:ok, {MapSet.new(), []}},
      &validate_shadow_location_entry(&1, &2, limits)
    )
    |> case do
      {:ok, {_names, values}} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp validate_shadow_locations(_, _limits),
    do: {:error, "host.toml: shadow_controller.location must be an array"}

  defp validate_shadow_location_entry(location, {:ok, {names, acc}}, limits) do
    case validate_shadow_location(location, limits) do
      {:ok, value} -> add_shadow_location(value, names, acc)
      {:error, _} = error -> {:halt, error}
    end
  end

  defp add_shadow_location(%{name: name} = value, names, acc) do
    if MapSet.member?(names, name),
      do: {:halt, {:error, "host.toml: shadow_controller.location names must be unique"}},
      else: {:cont, {:ok, {MapSet.put(names, name), [value | acc]}}}
  end

  defp validate_shadow_location(%{} = location, limits) do
    with :ok <- allow_only(location, @allowed_shadow_location, "shadow_controller.location"),
         {:ok, name} <- required_text(location["name"], "shadow_controller.location.name"),
         {:ok, kind} <- validate_shadow_kind(location["kind"]),
         :ok <- validate_shadow_location_shape(location, kind),
         {:ok, control_timeout} <-
           validate_timeout(location["control_timeout_ms"], kind, limits, "control_timeout_ms"),
         {:ok, read_timeout} <-
           validate_timeout(location["read_timeout_ms"], kind, limits, "read_timeout_ms") do
      {:ok,
       %{
         name: name,
         kind: kind,
         control_base_url: String.trim(location["control_base_url"] || ""),
         control_bearer_token: location["control_bearer_token"] || "",
         control_timeout_ms: control_timeout,
         read_timeout_ms: read_timeout
       }}
    end
  end

  defp validate_shadow_location(_, _limits),
    do: {:error, "host.toml: shadow_controller.location entries must be tables"}

  defp validate_shadow_kind("local"), do: {:ok, :local}
  defp validate_shadow_kind("remote"), do: {:ok, :remote}

  defp validate_shadow_kind(_),
    do: {:error, "host.toml: shadow_controller.location.kind must be local or remote"}

  defp validate_shadow_location_shape(location, :local) do
    if Enum.any?(
         ~w(control_base_url control_bearer_token control_timeout_ms read_timeout_ms),
         &Map.has_key?(location, &1)
       ),
       do: {:error, "host.toml: local shadow locations must omit remote fields"},
       else: :ok
  end

  defp validate_shadow_location_shape(location, :remote) do
    with {:ok, _} <-
           required_text(
             location["control_base_url"],
             "shadow_controller.location.control_base_url"
           ),
         :ok <-
           validate_url(location["control_base_url"], "shadow_controller.location.control_base_url"),
         {:ok, _} <-
           required_text(
             location["control_bearer_token"],
             "shadow_controller.location.control_bearer_token"
           ),
         :ok <-
           validate_positive_integer(
             location["control_timeout_ms"],
             "shadow_controller.location.control_timeout_ms"
           ) do
      validate_positive_integer(
        location["read_timeout_ms"],
        "shadow_controller.location.read_timeout_ms"
      )
    end
  end

  defp validate_timeout(_value, :local, _limits, _field), do: {:ok, nil}

  defp validate_timeout(value, :remote, limits, field) do
    max_wait_ms = limits["max_wait_ms"]

    case value do
      value when is_integer(value) and value > 0 and value <= max_wait_ms -> {:ok, value}
      _ -> {:error, "host.toml: shadow_controller.location.#{field} must be 1..#{max_wait_ms}"}
    end
  end

  defp validate_storage_root(nil, _root), do: :ok

  defp validate_storage_root(value, root) when is_binary(value) do
    expanded = Path.expand(value, root)

    if Path.type(value) != :absolute and PathSafety.within_root?(expanded, root) and
         PathSafety.no_symlink_components?(expanded),
       do: :ok,
       else: {:error, "host.toml: shadow_worker.storage_root must remain beneath the database root"}
  end

  defp validate_storage_root(_, _root),
    do: {:error, "host.toml: shadow_worker.storage_root must be a relative path"}

  defp validate_allowed_roots(nil, _field), do: {:ok, []}

  defp validate_allowed_roots(values, field) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      if is_binary(value) and Path.type(value) == :absolute and
           PathSafety.no_symlink_components?(value),
         do: {:cont, {:ok, [Path.expand(value) | acc]}},
         else:
           {:halt,
            {:error, "host.toml: #{field} entries must be absolute, readable, non-symlink paths"}}
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp validate_allowed_roots(_, field), do: {:error, "host.toml: #{field} must be an array"}

  @doc """
  Canonicalizes an allow-list entry or a provision-time source origin so
  configuration and requests are compared under one rule.

  Accepts only a bare HTTP(S) origin: a non-empty host, no userinfo or query
  or fragment, and an empty or `/` path. Lowercases the scheme and host, drops
  default ports (80 for `http`, 443 for `https`), preserves non-default ports,
  and wraps IPv6 hosts in brackets. Returns `{:ok, canonical}` or
  `{:error, reason}`.
  """
  @spec canonical_origin(String.t()) :: {:ok, String.t()} | {:error, String.t()}
  def canonical_origin(value) when is_binary(value) do
    uri = URI.parse(String.trim(value))

    cond do
      uri.scheme not in ["http", "https"] ->
        {:error, "must be an HTTP(S) origin"}

      not origin_host?(uri.host) ->
        {:error, "must include a non-empty host"}

      uri.userinfo != nil or uri.query != nil or uri.fragment != nil ->
        {:error, "must not include userinfo, query, or fragment"}

      uri.path not in [nil, "", "/"] ->
        {:error, "must not include a path"}

      true ->
        {:ok, canonical_authority(uri)}
    end
  end

  def canonical_origin(_), do: {:error, "must be an HTTP(S) origin string"}

  defp canonical_authority(uri) do
    host = canonical_host(uri.host)
    default = default_port(uri.scheme)

    case uri.port do
      ^default -> "#{uri.scheme}://#{host}"
      port -> "#{uri.scheme}://#{host}:#{port}"
    end
  end

  defp canonical_host(host) do
    host = String.downcase(host)
    if String.contains?(host, ":"), do: "[#{host}]", else: host
  end

  defp default_port("https"), do: 443
  defp default_port(_), do: 80

  defp origin_host?(host), do: is_binary(host) and host != ""

  defp validate_origins(nil, _field), do: {:ok, []}

  defp validate_origins(values, field) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case canonical_origin(value) do
        {:ok, canonical} -> {:cont, {:ok, [canonical | acc]}}
        {:error, reason} -> {:halt, {:error, "host.toml: #{field} #{reason}"}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp validate_origins(_, field), do: {:error, "host.toml: #{field} must be an array"}

  defp validate_digest_list(nil, _field), do: :ok

  defp validate_digest_list(values, field) when is_list(values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      if is_binary(value) and byte_size(value) == @digest_hex_length and
           Regex.match?(~r/^[0-9a-fA-F]+$/, value),
         do: {:cont, :ok},
         else: {:halt, {:error, "host.toml: #{field} entries must be SHA-256 hex digests"}}
    end)
  end

  defp validate_digest_list(_, field), do: {:error, "host.toml: #{field} must be an array"}

  defp validate_optional_text(nil, _field), do: :ok

  defp validate_optional_text(value, field) when is_binary(value),
    do: validate_url_or_empty(value, field)

  defp validate_optional_text(_, field), do: {:error, "host.toml: #{field} must be a string"}

  defp validate_optional_secret(nil, _field), do: :ok
  defp validate_optional_secret(value, _field) when is_binary(value), do: :ok

  defp validate_optional_secret(_, field),
    do: {:error, "host.toml: #{field} must be a string"}

  defp validate_url_or_empty("", _field), do: :ok
  defp validate_url_or_empty(value, field), do: validate_url(value, field)

  defp validate_url(value, field) when is_binary(value) do
    uri = URI.parse(String.trim(value))

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.userinfo == nil and
         uri.query == nil and uri.fragment == nil and uri.path in [nil, "", "/"],
       do: :ok,
       else:
         {:error,
          "host.toml: #{field} must be a canonical HTTP(S) origin without userinfo, query, or fragment"}
  end

  defp validate_url(_, field), do: {:error, "host.toml: #{field} must be an HTTP(S) URL"}

  defp required_text(value, field) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: {:error, "host.toml: #{field} must not be empty"}, else: {:ok, value}
  end

  defp required_text(_, field), do: {:error, "host.toml: #{field} must be a string"}

  defp validate_positive_integer(value, _field) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_integer(_, field),
    do: {:error, "host.toml: #{field} must be a positive integer"}

  defp validate_federation(nil, _limits),
    do:
      {:ok,
       [
         max_sources: 16,
         max_concurrent_sources: 8,
         max_candidates: 10_000,
         max_execution_ms: 10_000,
         saved_queries: []
       ]}

  defp validate_federation(%{} = federation, limits) do
    with :ok <- allow_only(federation, @allowed_federation, "federation"),
         {:ok, sources} <- positive_default(federation["max_sources"], 16, "max_sources", 256),
         {:ok, concurrent} <-
           positive_default(federation["max_concurrent_sources"], 8, "max_concurrent_sources", 64),
         {:ok, candidates} <-
           positive_default(federation["max_candidates"], 10_000, "max_candidates", 1_000_000),
         {:ok, execution} <-
           positive_default(federation["max_execution_ms"], 10_000, "max_execution_ms", 300_000),
         {:ok, saved_queries} <-
           validate_saved_queries(
             federation["saved_query"],
             limits["max_query_results"],
             sources
           ),
         :ok <-
           if(concurrent <= sources,
             do: :ok,
             else:
               {:error, "host.toml: federation.max_concurrent_sources must not exceed max_sources"}
           ),
         :ok <-
           if(candidates >= sources,
             do: :ok,
             else: {:error, "host.toml: federation.max_candidates must be at least max_sources"}
           ) do
      {:ok,
       [
         max_sources: sources,
         max_concurrent_sources: concurrent,
         max_candidates: candidates,
         max_execution_ms: execution,
         saved_queries: saved_queries
       ]}
    end
  end

  defp validate_federation(_, _limits), do: {:error, "host.toml: [federation] must be a table"}

  defp validate_saved_queries(nil, _max_query_results, _max_sources), do: {:ok, []}

  defp validate_saved_queries(values, max_query_results, max_sources) when is_list(values) do
    Enum.reduce_while(values, {:ok, {MapSet.new(), []}}, fn definition, {:ok, {names, acc}} ->
      with true <- is_map(definition),
           :ok <- allow_only(definition, ~w(name sources query_json), "federation.saved_query"),
           name when is_binary(name) and name != "" <- definition["name"],
           false <- MapSet.member?(names, name),
           sources when is_list(sources) <- definition["sources"],
           query_json when is_binary(query_json) <- definition["query_json"],
           {:ok, query} when is_map(query) <- StrictDecoder.decode(query_json),
           false <- Map.has_key?(query, "bookmark"),
           {:ok, normalized} <-
             Normalizer.normalize(%{"databases" => sources, "query" => query},
               max_query_results: max_query_results,
               max_sources: max_sources
             ) do
        saved = %{
          name: name,
          databases: normalized.databases,
          query: normalized.query,
          fingerprint: normalized.fingerprint
        }

        {:cont, {:ok, {MapSet.put(names, name), [saved | acc]}}}
      else
        true -> {:halt, {:error, "host.toml: federation.saved_query names must be unique"}}
        false -> {:halt, {:error, "host.toml: federation.saved_query bookmarks are not allowed"}}
        _ -> {:halt, {:error, "host.toml: invalid federation.saved_query definition"}}
      end
    end)
    |> case do
      {:ok, {_names, values}} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp validate_saved_queries(_, _max_query_results, _max_sources),
    do: {:error, "host.toml: federation.saved_query must be an array"}

  defp positive(value, _key, max) when is_integer(value) and value > 0 and value <= max,
    do: {:ok, value}

  defp positive(_value, key, max),
    do: {:error, "host.toml: federation.#{key} must be a positive integer <= #{max}"}

  defp positive_default(nil, default, key, max), do: positive(default, key, max)
  defp positive_default(value, _default, key, max), do: positive(value, key, max)

  defp allow_only(%{} = map, allowed, section) do
    case Map.keys(map) -- allowed do
      [] -> :ok
      [key | _] -> {:error, "host.toml: unknown key #{key} in [#{section}]"}
    end
  end

  defp validate_bool(nil, _field), do: :ok
  defp validate_bool(value, _field) when is_boolean(value), do: :ok
  defp validate_bool(_value, field), do: {:error, "host.toml: #{field} must be a boolean"}

  # TLS material must resolve inside the database root (TLS-001/002). Relative
  # paths anchor at the root; absolute paths are accepted only if they remain
  # within it.
  @spec resolve_path(String.t(), String.t()) :: String.t()
  def resolve_path(root, path) do
    if Path.type(path) == :absolute,
      do: Path.expand(path),
      else: Path.expand(path, root)
  end

  defp validate_path_inside_root(path, root, _field) when is_binary(path) do
    root_abs = Path.expand(root)
    resolved = resolve_path(root, path)

    if String.starts_with?(resolved <> "/", root_abs <> "/") and
         ElixirDB.PathSafety.no_symlink_components?(resolved) do
      :ok
    else
      {:error, "host.toml: tls path #{inspect(path)} escapes the database root"}
    end
  end

  defp readable?(path, field) do
    case File.read(path) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error, "host.toml: #{field} #{inspect(path)} cannot be read: #{inspect(reason)}"}
    end
  end
end
