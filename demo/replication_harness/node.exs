defmodule ElixirDB.ReplicationHarness.Node do
  alias ElixirDB.Runtime.DatabaseCatalog

  @poll_interval 100

  def main([mode]) when mode in ["web", "cli"] do
    {:ok, _} = Application.ensure_all_started(:elixir_db)

    case mode do
      "web" -> web_node()
      "cli" -> cli_node()
    end
  end

  def main(_), do: raise("usage: mix run demo/replication_harness/node.exs web|cli")

  defp web_node do
    server_url = server_url()
    main_config = System.fetch_env!("DEMO_MAIN_CONFIG")
    cli_config = System.fetch_env!("DEMO_C_CONFIG")
    ready_config = System.fetch_env!("DEMO_READY_CONFIG")

    a = create!("web-a.db")
    b = create!("web-b.db")

    assert_ok!(
      ElixirDB.Documents.put(a.database_uuid, %{
        id: "seed",
        body: %{"source" => "Client A", "message" => "Seeded by the harness", "n" => 0}
      })
    )

    write_json!(main_config, %{
      "server_url" => server_url,
      "clients" => [
        client_config("a", "Client A", "Database A", a.database_uuid, server_url),
        client_config("b", "Client B", "Database B", b.database_uuid, server_url)
      ],
      "native_client" => nil
    })

    IO.puts("Web database node started")
    IO.puts("  Client A: #{a.database_uuid}")
    IO.puts("  Client B: #{b.database_uuid}")
    IO.puts("  HTTP: #{server_url}")
    IO.puts("Waiting for the native Elixir node at #{cli_config}…")

    cli = wait_for_json!(cli_config, 120_000)
    c_uuid = cli["database_uuid"]
    c_url = cli["endpoint"]

    jobs = %{
      "a_to_b" => put_job!(a.database_uuid, remote_endpoint(b.database_uuid, server_url)),
      "b_to_a" => put_job!(b.database_uuid, remote_endpoint(a.database_uuid, server_url)),
      "a_to_c" => put_job!(a.database_uuid, remote_endpoint(c_uuid, c_url))
    }

    ready = %{
      "server_url" => server_url,
      "clients" => [
        client_config("a", "Client A", "Database A", a.database_uuid, server_url),
        client_config("b", "Client B", "Database B", b.database_uuid, server_url)
      ],
      "native_client" => Map.merge(cli, %{"replication_source" => a.database_uuid}),
      "jobs" => jobs
    }

    write_json!(ready_config, ready)

    IO.puts("Replication jobs enabled: A ↔ B and A → native C")
    IO.puts("Manual UI configuration written to #{ready_config}")
    keep_alive()
  end

  defp cli_node do
    cli_config = System.fetch_env!("DEMO_C_CONFIG")
    ready_config = System.fetch_env!("DEMO_READY_CONFIG")
    port = System.fetch_env!("ELIXIR_DB_PORT")
    c = create!("native-c.db")
    endpoint = "http://127.0.0.1:#{port}"

    write_json!(cli_config, %{
      "database_uuid" => c.database_uuid,
      "database_label" => "Database C",
      "endpoint" => endpoint
    })

    IO.puts("Elixir native client started")
    IO.puts("  Database C: #{c.database_uuid}")
    IO.puts("  Native changes feed: ElixirDB.Changes.wait/2")
    IO.puts("  Waiting for A → C replication…")

    wait_for_file!(ready_config, 120_000)
    print_changes(c.database_uuid, 0)
  end

  defp print_changes(uuid, since) do
    case ElixirDB.Changes.wait(uuid, %{since: since, limit: 100, wait_ms: 30_000}) do
      {:ok, %{results: changes, last_sequence: last_sequence}} ->
        Enum.each(changes, fn change ->
          IO.puts(
            "[native-c] replicated sequence=#{change.sequence} " <>
              "document=#{change.document_id} revision=#{change.winning_revision} " <>
              "origin=#{change.origin}"
          )
        end)

        print_changes(uuid, max(since, last_sequence))

      {:error, %ElixirDB.Error{} = error} ->
        IO.puts(:stderr, "[native-c] changes feed error code=#{error.code}: #{error.message}")
        Process.sleep(@poll_interval)
        print_changes(uuid, since)
    end
  end

  defp create!(path) do
    case DatabaseCatalog.create(path) do
      {:ok, identity} -> identity
      {:error, error} -> raise "could not create #{path}: #{inspect(error)}"
    end
  end

  defp put_job!(source_uuid, endpoint) do
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

    case ElixirDB.Replication.JobManager.put(source_uuid, definition) do
      {:ok, %{job_id: job_id}} -> job_id
      {:error, error} -> raise "could not enable replication: #{inspect(error)}"
    end
  end

  defp remote_endpoint(database_uuid, base_url),
    do: %{"kind" => "remote", "database_uuid" => database_uuid, "base_url" => base_url}

  defp client_config(key, label, database_label, uuid, endpoint),
    do: %{
      "key" => key,
      "label" => label,
      "database_label" => database_label,
      "database_uuid" => uuid,
      "endpoint" => endpoint
    }

  defp server_url do
    port = System.fetch_env!("ELIXIR_DB_PORT")
    "http://127.0.0.1:#{port}"
  end

  defp write_json!(path, value) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode_to_iodata!(value))
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

  defp assert_ok!({:ok, value}), do: value
  defp assert_ok!({:error, error}), do: raise("harness seed failed: #{inspect(error)}")

  defp keep_alive do
    receive do
      :stop -> :ok
    after
      :infinity -> :ok
    end
  end
end

ElixirDB.ReplicationHarness.Node.main(System.argv())
