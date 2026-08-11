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

  @default_admission AdmissionPolicy.default_toml_map()

  @known_sections ~w(listener limits admission auth tls security observability federation web_ui)
  @allowed_listener ~w(ip port)
  @allowed_auth ~w(enabled tokens)
  @allowed_tls ~w(enabled certfile keyfile)
  @allowed_security ~w(allow_insecure_remote)
  @allowed_observability ~w(otlp_endpoint)
  @allowed_web_ui ~w(enabled)
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
    case Toml.decode(contents) do
      {:ok, map} when is_map(map) -> {:ok, map}
      {:error, reason} -> {:error, "host.toml parse error: #{inspect(reason)}"}
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

    if String.starts_with?(resolved <> "/", root_abs <> "/"),
      do: :ok,
      else: {:error, "host.toml: tls path #{inspect(path)} escapes the database root"}
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
