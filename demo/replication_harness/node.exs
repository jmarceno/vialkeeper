Application.put_env(:vial_keeper, :observability_dashboard, true)

dashboard_reader = %{
  id: :vial_keeper_demo_dashboard_reader,
  module: :otel_metric_reader,
  config: %{
    exporter: {VialKeeper.Observability.Dashboard, %{}},
    export_interval_ms: 1_000
  }
}

configured_readers = Application.get_env(:opentelemetry_experimental, :readers, [])

Application.put_env(
  :opentelemetry_experimental,
  :readers,
  [dashboard_reader | Enum.reject(configured_readers, &(&1[:id] == dashboard_reader.id))]
)

defmodule VialKeeper.ReplicationHarness.Node do
  @frankenstein_path Path.join([__DIR__, "fixtures", "frankenstein.md"])
  @frankenstein_index "frankenstein"

  @poll_interval 100

  def main([mode]) when mode in ["web", "peer", "worker"] do
    configure_runtime!(mode)
    {:ok, _} = Application.ensure_all_started(:vial_keeper)

    case mode do
      "web" -> web_node()
      "peer" -> peer_node()
      "worker" -> worker_node()
    end
  end

  def main(_),
    do: raise("usage: mix run demo/replication_harness/node.exs web|peer|worker")

  defp configure_runtime!(mode) do
    root = database_root()
    port = System.fetch_env!("VIAL_KEEPER_PORT") |> String.to_integer()

    Application.put_env(:vial_keeper, :database_root, root)
    Application.put_env(:vial_keeper, :listener, ip: {127, 0, 0, 1}, port: port)

    case mode do
      "web" ->
        Application.put_env(:vial_keeper, :auth, auth_config())
        Application.put_env(:vial_keeper, :shadow_controller, shadow_controller_config())

      "peer" ->
        Application.put_env(:vial_keeper, :auth, auth_config())

      "worker" ->
        Application.put_env(:vial_keeper, :auth, enabled: false, token_digests: [])

        Application.put_env(:vial_keeper, :shadow_worker,
          enabled: true,
          storage_root: "shadows",
          control_token_digests: [digest(control_token())],
          allowed_attachment_roots: allowed_attachment_roots(),
          allowed_source_origins: [source_url()],
          auto_replicate: true
        )
    end
  end

  defp web_node do
    server_url = server_url()
    main_config = System.fetch_env!("DEMO_MAIN_CONFIG")
    peer_config = System.fetch_env!("DEMO_PEER_CONFIG")
    ready_config = System.fetch_env!("DEMO_READY_CONFIG")
    private_config = System.fetch_env!("DEMO_PRIVATE_CONFIG")
    worker_a_config = System.fetch_env!("DEMO_WORKER_A_CONFIG")
    worker_b_config = System.fetch_env!("DEMO_WORKER_B_CONFIG")

    a = create_over_http!(server_url, "web-a.db")
    b = create_over_http!(server_url, "web-b.db")
    fts = create_over_http!(server_url, "web-fts.db")
    a_uuid = a["database_uuid"]
    b_uuid = b["database_uuid"]
    fts_uuid = fts["database_uuid"]

    post_json!(server_url, "/v1/databases/#{a_uuid}/documents/put", %{
      id: "seed",
      body: %{"source" => "Client A", "message" => "Seeded by the harness", "n" => 0}
    })

    seed_frankenstein!(server_url, fts_uuid)
    attachment_location = attachment_location!("web-a.db")

    write_json!(main_config, %{
      "server_url" => server_url,
      "clients" => [
        client_config("a", "Client A", "Database A", a_uuid, server_url),
        client_config("b", "Client B", "Database B", b_uuid, server_url)
      ],
      "fts" => fts_config(fts_uuid, server_url),
      "http_peer" => nil,
      "shadow" => %{
        "source_database_uuid" => a_uuid,
        "locations" => ["worker-a", "worker-b"]
      }
    })

    IO.puts("Web database node started")
    IO.puts("  Client A: #{a_uuid}")
    IO.puts("  Client B: #{b_uuid}")
    IO.puts("  Full-text corpus: #{fts_uuid}")
    IO.puts("  HTTP: #{server_url}")
    IO.puts("Waiting for the HTTP peer at #{peer_config}…")

    peer = wait_for_json!(peer_config, 120_000)
    _worker_a = wait_for_json!(worker_a_config, 120_000)
    _worker_b = wait_for_json!(worker_b_config, 120_000)
    c_uuid = peer["database_uuid"]
    c_url = peer["endpoint"]

    jobs = %{
      "a_to_b" => put_job!(server_url, a_uuid, remote_endpoint(b_uuid, server_url)),
      "b_to_a" => put_job!(server_url, b_uuid, remote_endpoint(a_uuid, server_url)),
      "a_to_c" => put_job!(server_url, a_uuid, remote_endpoint(c_uuid, c_url))
    }

    ready = %{
      "server_url" => server_url,
      "clients" => [
        client_config("a", "Client A", "Database A", a_uuid, server_url),
        client_config("b", "Client B", "Database B", b_uuid, server_url)
      ],
      "fts" => fts_config(fts_uuid, server_url),
      "http_peer" => Map.merge(peer, %{"replication_source" => a_uuid}),
      "jobs" => jobs,
      "shadow" => %{
        "source_database_uuid" => a_uuid,
        "locations" => ["worker-a", "worker-b"],
        "status_path" => "/api/lab/state"
      }
    }

    write_json!(ready_config, ready)

    write_json!(private_config, %{
      "project_root" => System.fetch_env!("DEMO_PROJECT_ROOT"),
      "source_endpoint" => server_url,
      "source_token" => source_token(),
      "source_database_uuid" => a_uuid,
      "source_attachment_location" => attachment_location,
      "allowed_attachment_roots" => allowed_attachment_roots(),
      "clients" => ready["clients"],
      "http_peer" => ready["http_peer"],
      "workers" => [
        private_worker_config("a", worker_a_config),
        private_worker_config("b", worker_b_config)
      ]
    })

    IO.puts("Replication jobs enabled: A ↔ B and A → HTTP peer C")
    IO.puts("Shadow workers available: worker-a and worker-b")
    IO.puts("Manual UI configuration written to #{ready_config}")
    keep_alive()
  end

  defp peer_node do
    peer_config = System.fetch_env!("DEMO_PEER_CONFIG")
    ready_config = System.fetch_env!("DEMO_READY_CONFIG")
    port = System.fetch_env!("VIAL_KEEPER_PORT")
    endpoint = "http://127.0.0.1:#{port}"
    c = create_over_http!(endpoint, "http-peer-c.db")

    write_json!(peer_config, %{
      "database_uuid" => c["database_uuid"],
      "database_label" => "Database C",
      "endpoint" => endpoint
    })

    IO.puts("HTTP peer client started")
    IO.puts("  Database C: #{c["database_uuid"]}")
    IO.puts("  Changes feed: POST /v1/databases/:uuid/changes")
    IO.puts("  Waiting for A → C replication…")

    wait_for_file!(ready_config, 120_000)
    print_changes(endpoint, c["database_uuid"], 0)
  end

  defp worker_node do
    config_path = System.fetch_env!("DEMO_WORKER_CONFIG")
    key = System.get_env("DEMO_WORKER_KEY", "worker")
    endpoint = server_url()

    write_json!(config_path, %{
      "key" => key,
      "label" => "Shadow worker #{String.upcase(key)}",
      "endpoint" => endpoint,
      "control_endpoint" => endpoint,
      "control_token" => control_token(),
      "database_root" => database_root(),
      "port" => System.fetch_env!("VIAL_KEEPER_PORT") |> String.to_integer(),
      "pid" => System.pid()
    })

    IO.puts("Shadow worker #{key} started at #{endpoint}")
    keep_alive()
  end

  defp print_changes(endpoint, uuid, since) do
    case post_json(endpoint, "/v1/databases/#{uuid}/changes", %{
           since: since,
           limit: 100,
           wait_ms: 30_000
         }) do
      {:ok, %{"results" => changes, "last_sequence" => last_sequence}} ->
        Enum.each(changes, fn change ->
          IO.puts(
            "[http-peer-c] replicated sequence=#{change["sequence"]} " <>
              "document=#{change["document_id"]} revision=#{change["winning_revision"]} " <>
              "origin=#{change["origin"]}"
          )
        end)

        print_changes(endpoint, uuid, max(since, last_sequence))

      {:error, error} ->
        IO.puts(:stderr, "[http-peer-c] changes feed error: #{error}")
        Process.sleep(@poll_interval)
        print_changes(endpoint, uuid, since)
    end
  end

  defp create_over_http!(endpoint, path) do
    wait_for_http!(endpoint)

    case post_json(endpoint, "/v1/databases", %{path: path}) do
      {:ok, %{"database_uuid" => _uuid} = identity} ->
        identity

      {:ok, response} ->
        raise "HTTP database creation returned an invalid response: #{inspect(response)}"

      {:error, reason} ->
        raise "HTTP database creation failed: #{reason}"
    end
  end

  defp wait_for_http!(endpoint) do
    deadline = System.monotonic_time(:millisecond) + 120_000
    wait_for_http_until!(endpoint, deadline)
  end

  defp wait_for_http_until!(endpoint, deadline) do
    request = [
      headers: [{"authorization", "Bearer #{source_token()}"}],
      connect_options: [timeout: 5_000],
      receive_timeout: 5_000
    ]

    case Req.get(endpoint <> "/v1/databases", request) do
      {:ok, %Req.Response{status: 200}} -> :ok
      _ -> retry_http(endpoint, deadline)
    end
  end

  defp retry_http(endpoint, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise "timed out waiting for HTTP peer at #{endpoint}"
    end

    Process.sleep(@poll_interval)
    wait_for_http_until!(endpoint, deadline)
  end

  defp post_json(endpoint, path, body) do
    request = [
      json: body,
      headers: [{"authorization", "Bearer #{source_token()}"}],
      connect_options: [timeout: 5_000],
      receive_timeout: 35_000
    ]

    case Req.post(endpoint <> path, request) do
      {:ok, %Req.Response{status: status, body: %{"data" => data}}} when status in 200..299 ->
        {:ok, data}

      {:ok, %Req.Response{status: status, body: %{"error" => error}}} ->
        {:error, "HTTP #{status}: #{inspect(error)}"}

      {:ok, %Req.Response{status: status, body: body}} ->
        {:error, "HTTP #{status}: #{inspect(body)}"}

      {:error, exception} ->
        {:error, Exception.message(exception)}
    end
  end

  defp post_json!(endpoint, path, body) do
    case post_json(endpoint, path, body) do
      {:ok, response} -> response
      {:error, reason} -> raise "HTTP request to #{path} failed: #{reason}"
    end
  end

  defp put_job!(server_url, source_uuid, endpoint) do
    definition = %{
      "persist" => true,
      "mode" => "continuous",
      "direction" => "push",
      "endpoint" => endpoint,
      "enabled" => true,
      "wait_ms" => 250,
      "retry" => %{
        "max_attempts" => 32,
        "base_delay_ms" => 100,
        "max_delay_ms" => 2_000,
        "jitter_ms" => 25
      }
    }

    case post_json!(server_url, "/v1/databases/#{source_uuid}/replications", definition) do
      %{"job_id" => job_id} ->
        job_id

      response ->
        raise "HTTP replication creation returned an invalid response: #{inspect(response)}"
    end
  end

  defp remote_endpoint(database_uuid, base_url),
    do: %{
      "kind" => "remote",
      "database_uuid" => database_uuid,
      "base_url" => base_url,
      "auth_token" => source_token()
    }

  defp client_config(key, label, database_label, uuid, endpoint),
    do: %{
      "key" => key,
      "label" => label,
      "database_label" => database_label,
      "database_uuid" => uuid,
      "endpoint" => endpoint
    }

  defp fts_config(uuid, endpoint),
    do: %{
      "key" => "fts",
      "label" => "Frankenstein",
      "database_label" => "Full-text corpus",
      "database_uuid" => uuid,
      "endpoint" => endpoint,
      "index" => @frankenstein_index
    }

  defp seed_frankenstein!(endpoint, uuid) do
    Code.require_file("frankenstein_corpus.exs", __DIR__)

    operations =
      Enum.map(
        VialKeeper.ReplicationHarness.FrankensteinCorpus.documents!(@frankenstein_path),
        fn %{id: id, body: body} -> %{"type" => "put", "id" => id, "body" => body} end
      )

    post_json!(endpoint, "/v1/databases/#{uuid}/documents/bulk-write", operations)

    post_json!(endpoint, "/v1/databases/#{uuid}/indexes", %{
      "name" => @frankenstein_index,
      "type" => "full_text",
      "fields" => ["/text"]
    })
  end

  defp private_worker_config(key, config_path) do
    worker = wait_for_json!(config_path, 120_000)

    %{
      "key" => key,
      "label" => worker["label"],
      "endpoint" => worker["endpoint"],
      "control_endpoint" => worker["control_endpoint"],
      "control_token" => worker["control_token"],
      "database_root" => worker["database_root"],
      "port" => worker["port"],
      "pid" => worker["pid"],
      "config_path" => config_path
    }
  end

  defp shadow_controller_config do
    [
      enabled: true,
      source_base_url: server_url(),
      source_bearer_token: source_token(),
      location: [shadow_location("a"), shadow_location("b")]
    ]
  end

  defp shadow_location(key) do
    upper = String.upcase(key)

    %{
      name: "worker-#{key}",
      kind: :remote,
      control_base_url: System.fetch_env!("DEMO_WORKER_#{upper}_ENDPOINT"),
      control_bearer_token: System.fetch_env!("DEMO_WORKER_#{upper}_CONTROL_TOKEN"),
      control_timeout_ms: 5_000,
      read_timeout_ms: 5_000
    }
  end

  defp auth_config, do: [enabled: true, token_digests: [digest(source_token())]]

  defp digest(token), do: :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)

  defp source_token, do: System.fetch_env!("DEMO_SOURCE_TOKEN")

  defp control_token, do: System.fetch_env!("DEMO_WORKER_CONTROL_TOKEN")

  defp allowed_attachment_roots do
    System.get_env("DEMO_ALLOWED_ATTACHMENT_ROOTS", "")
    |> String.split(":", trim: true)
    |> Enum.map(&Path.expand/1)
  end

  defp attachment_location!(relative_path),
    do: Path.join([database_root(), relative_path, "blobs"])

  defp server_url do
    port = System.fetch_env!("VIAL_KEEPER_PORT")
    "http://127.0.0.1:#{port}"
  end

  defp source_url do
    System.get_env("DEMO_SOURCE_ENDPOINT", server_url())
  end

  defp database_root, do: System.fetch_env!("VIAL_KEEPER_ROOT") |> Path.expand()

  defp write_json!(path, value) do
    File.mkdir_p!(Path.dirname(path))
    temp_path = "#{path}.tmp-#{System.unique_integer([:positive])}"
    File.write!(temp_path, JSON.encode_to_iodata!(value))
    File.rename!(temp_path, path)
  end

  defp wait_for_json!(path, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for_json_until!(path, deadline)
  end

  defp wait_for_json_until!(path, deadline) do
    case File.read(path) do
      {:ok, body} ->
        case JSON.decode(body) do
          {:ok, value} -> value
          {:error, _} -> retry_file(path, deadline)
        end

      {:error, _} ->
        retry_file(path, deadline)
    end
  end

  defp retry_file(path, deadline) do
    if System.monotonic_time(:millisecond) >= deadline do
      raise "timed out waiting for #{path}"
    end

    Process.sleep(@poll_interval)
    wait_for_json_until!(path, deadline)
  end

  defp wait_for_file!(path, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    wait_for_file_until!(path, deadline)
  end

  defp wait_for_file_until!(path, deadline) do
    unless File.exists?(path) do
      if System.monotonic_time(:millisecond) >= deadline,
        do: raise("timed out waiting for #{path}")

      Process.sleep(@poll_interval)
      wait_for_file_until!(path, deadline)
    end
  end

  defp keep_alive do
    receive do
      :stop -> :ok
    after
      :infinity -> :ok
    end
  end
end

VialKeeper.ReplicationHarness.Node.main(System.argv())
