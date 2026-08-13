defmodule ElixirDB.Benchmarks.ReplicationWire do
  @moduledoc """
  Repeatable remote-replication wire benchmark.

  Exercises real `RemoteEndpoint` traffic through Bandit and counts HTTP entity
  bodies without TLS framing. Run with
  `mix bench.replication -- --format json --seed 20260812`.
  """

  alias ElixirDB.Attachments
  alias ElixirDB.Attachments.FilesystemStore
  alias ElixirDB.Documents
  alias ElixirDB.Replication.Id
  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Runtime.DatabaseCatalog
  alias ElixirDB.TestServer

  @default_seed 20_260_812
  @scenarios [
    :small_compressible_json,
    :large_compressible_json,
    :mixed_revision_chain_json,
    :raw_attachment,
    :zstd_attachment
  ]

  @doc false
  @spec main([binary()]) :: :ok
  def main(argv) do
    options = parse_options(argv)
    ensure_test_env!()

    with_isolated_runtime(fn ->
      started_at = DateTime.utc_now() |> DateTime.to_iso8601()
      seed = options[:seed] || @default_seed
      scenarios = parse_scenarios(options[:scenario])

      results = Enum.map(scenarios, fn scenario -> run_scenario(scenario, seed) end)

      report = %{
        "schema_version" => 1,
        "started_at" => started_at,
        "git_revision" => git_revision(),
        "runtime" => runtime_metadata(),
        "configuration" => %{
          "seed" => seed,
          "format" => "json",
          "job_direction" => "push",
          "mode" => "one_shot"
        },
        "results" => results
      }

      output = options[:output] || default_output_path()
      write_report(report, output)
      print_summary(report, output)
    end)
  end

  defp with_isolated_runtime(fun) do
    root = Path.join(System.tmp_dir!(), "elixir-db-replication-wire-bench-#{unique_suffix()}")
    previous_root = Application.get_env(:elixir_db, :database_root)
    previous_listener = Application.get_env(:elixir_db, :listener)
    ensure_application_stopped!()
    File.mkdir_p!(root)
    Application.put_env(:elixir_db, :database_root, root)
    Application.put_env(:elixir_db, :listener, ip: {127, 0, 0, 1}, port: 0)

    try do
      {:ok, _started} = Application.ensure_all_started(:elixir_db)
      fun.()
    after
      _ = Application.stop(:elixir_db)
      restore_application_env(:database_root, previous_root)
      restore_application_env(:listener, previous_listener)
      _ = File.rm_rf(root)
    end
  end

  defp restore_application_env(key, nil), do: Application.delete_env(:elixir_db, key)
  defp restore_application_env(key, value), do: Application.put_env(:elixir_db, key, value)

  defp ensure_application_stopped! do
    if Enum.any?(Application.started_applications(), &match?({:elixir_db, _, _}, &1)) do
      Mix.raise("benchmark must be launched with mix run --no-start")
    end
  end

  defp parse_options(argv) do
    argv = if List.first(argv) == "--", do: tl(argv), else: argv

    {options, positional, invalid} =
      OptionParser.parse(argv,
        strict: [
          format: :string,
          seed: :integer,
          scenario: :string,
          output: :string,
          help: :boolean
        ],
        aliases: [o: :output, s: :scenario]
      )

    if options[:help] do
      IO.puts(usage())
      System.halt(0)
    end

    if positional != [] or invalid != [] do
      Mix.raise("invalid benchmark arguments: #{inspect(positional ++ invalid)}\n\n#{usage()}")
    end

    case options[:format] do
      nil -> options
      "json" -> options
      other -> Mix.raise("unsupported --format #{inspect(other)}; allowed: json")
    end
  end

  defp parse_scenarios(nil), do: @scenarios

  defp parse_scenarios(value) do
    atoms =
      value
      |> String.split(",", trim: true)
      |> Enum.map(&String.to_existing_atom/1)

    if atoms != [] and Enum.all?(atoms, &(&1 in @scenarios)) do
      atoms
    else
      Mix.raise("unknown scenario #{inspect(value)}")
    end
  rescue
    ArgumentError -> Mix.raise("unknown scenario #{inspect(value)}")
  end

  defp run_scenario(scenario, seed) do
    prefix = "rwb-#{scenario}-#{System.unique_integer([:positive])}"
    a_path = prefix <> "-a.elixirdb"
    b_path = prefix <> "-b.elixirdb"
    root = ElixirDB.Config.database_root()

    {:ok, a} = DatabaseCatalog.create(a_path)
    {:ok, b} = DatabaseCatalog.create(b_path)
    a_uuid = a.database_uuid
    b_uuid = b.database_uuid
    {:ok, counter} = Agent.start_link(fn -> [] end)
    {:ok, server} = TestServer.start(request_hook: fn conn -> count_hook(conn, counter) end)

    try do
      fixture = build_fixture(scenario, seed, a_uuid)
      {:ok, replication_id} = Id.calculate(a_uuid, b_uuid, "push", "one_shot")

      :ok =
        ElixirDB.TestReplicationCheckpoint.seed_matching_checkpoints!(
          a_uuid,
          b_uuid,
          replication_id
        )

      {elapsed_us, :ok} =
        :timer.tc(fn ->
          start_push_job!(a_uuid, b_uuid, server.base_url)
          wait_for_target!(scenario, b_uuid, fixture)
        end)

      requests = Agent.get(counter, &Enum.reverse/1)
      source_repr = representation_sha256(a_uuid, fixture.digest)
      target_repr = representation_sha256(b_uuid, fixture.digest)

      %{
        "scenario" => Atom.to_string(scenario),
        "logical_bytes" => fixture.logical_bytes,
        "encoded_payload_bytes" => fixture.encoded_payload_bytes,
        "http_body_bytes" => Enum.reduce(requests, 0, &(&1["entity_bytes"] + &2)),
        "http_json_body_bytes" => sum_kind(requests, "json"),
        "http_blob_body_bytes" => sum_kind(requests, "blob"),
        "requests" => requests,
        "compress_us" => sum_field(requests, "compress_us"),
        "decompress_us" => sum_field(requests, "decompress_us"),
        "replication_elapsed_us" => elapsed_us,
        "source_representation_sha256" => source_repr,
        "target_representation_sha256" => target_repr,
        "fixture" => fixture.metadata
      }
    after
      _ = TestServer.stop(server)
      _ = Agent.stop(counter)
      _ = DatabaseCatalog.close(a_uuid)
      _ = DatabaseCatalog.close(b_uuid)
      _ = DatabaseCatalog.unregister(a_uuid)
      _ = DatabaseCatalog.unregister(b_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(root, a_path))
      ElixirDB.TempDatabase.cleanup(Path.join(root, b_path))
    end
  end

  defp count_hook(conn, counter) do
    Plug.Conn.register_before_send(conn, fn conn ->
      path = conn.request_path
      blob? = String.contains?(path, "/replication/blobs/")
      json? = not blob? and String.contains?(path, "/replication")

      if json? or blob? do
        req_len = header_int(conn, :req, "content-length")
        resp_len = resp_entity_bytes(conn)
        {compress_us, decompress_us} = codec_round_trip_us(conn, blob?)

        Agent.update(counter, fn acc ->
          [
            %{
              "method" => conn.method,
              "path" => path,
              "payload_kind" => if(blob?, do: "blob", else: "json"),
              "request_entity_bytes" => req_len,
              "response_entity_bytes" => resp_len,
              "entity_bytes" => req_len + resp_len,
              "compress_us" => compress_us,
              "decompress_us" => decompress_us,
              "request_content_encoding" =>
                List.first(Plug.Conn.get_req_header(conn, "content-encoding")),
              "response_content_encoding" =>
                List.first(Plug.Conn.get_resp_header(conn, "content-encoding"))
            }
            | acc
          ]
        end)
      end

      conn
    end)
  end

  defp header_int(conn, :req, name) do
    parse_int_header(Plug.Conn.get_req_header(conn, name))
  end

  defp parse_int_header([value | _]) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ -> 0
    end
  end

  defp parse_int_header(_), do: 0

  defp resp_entity_bytes(%Plug.Conn{resp_body: body}) when is_binary(body), do: byte_size(body)

  defp resp_entity_bytes(%Plug.Conn{resp_body: body}) when is_list(body),
    do: body |> IO.iodata_to_binary() |> byte_size()

  defp resp_entity_bytes(conn),
    do: parse_int_header(Plug.Conn.get_resp_header(conn, "content-length"))

  defp start_push_job!(source_uuid, target_uuid, base_url) do
    case JobManager.put(source_uuid, %{
           "persist" => false,
           "mode" => "one_shot",
           "direction" => "push",
           "enabled" => true,
           "endpoint" => %{
             "kind" => "remote",
             "database_uuid" => target_uuid,
             "base_url" => base_url
           }
         }) do
      {:ok, _} -> :ok
      {:error, error} -> Mix.raise("replication job failed: #{inspect(error)}")
    end
  end

  defp wait_for_target!(_scenario, target_uuid, fixture) do
    wait_until(
      fn ->
        Enum.all?(fixture.doc_ids, fn id ->
          match?({:ok, _}, Documents.get(target_uuid, %{id: id}))
        end)
      end,
      30_000,
      "replicated documents never arrived for #{inspect(fixture.doc_ids)}"
    )
  end

  defp wait_until(fun, timeout_ms, message) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    poll_until(fun, deadline, message)
  end

  defp poll_until(fun, deadline, message) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        Mix.raise(message)
      else
        Process.sleep(10)
        poll_until(fun, deadline, message)
      end
    end
  end

  defp build_fixture(:small_compressible_json, _seed, uuid) do
    body = %{"text" => String.duplicate("compressible json payload ", 8), "n" => 1}

    doc_ids =
      Enum.map(1..40, fn index ->
        id = "small-#{index}"
        {:ok, _} = Documents.put(uuid, %{id: id, body: Map.put(body, "n", index)})
        id
      end)

    %{
      doc_ids: doc_ids,
      digest: nil,
      logical_bytes: json_size(body) * length(doc_ids),
      encoded_payload_bytes: 0,
      metadata: %{"documents" => length(doc_ids), "kind" => "json"}
    }
  end

  defp build_fixture(:large_compressible_json, _seed, uuid) do
    blob = String.duplicate("LARGE-COMPRESSIBLE-BLOCK-", 2_048)
    body = %{"blob" => blob, "marker" => "large"}

    doc_ids =
      Enum.map(1..4, fn index ->
        id = "large-#{index}"
        {:ok, _} = Documents.put(uuid, %{id: id, body: Map.put(body, "n", index)})
        id
      end)

    %{
      doc_ids: doc_ids,
      digest: nil,
      logical_bytes: json_size(body) * length(doc_ids),
      encoded_payload_bytes: 0,
      metadata: %{"documents" => length(doc_ids), "kind" => "json"}
    }
  end

  defp build_fixture(:mixed_revision_chain_json, _seed, uuid) do
    {doc_ids, logical_bytes} =
      Enum.map_reduce(1..8, 0, fn index, acc ->
        id = "chain-#{index}"

        bytes =
          Enum.reduce(1..3, {nil, 0}, fn rev, {if_revision, inner_acc} ->
            body = %{
              "rev" => rev,
              "note" => String.duplicate("revision-chain ", 12),
              "index" => index
            }

            {:ok, %{revision: revision}} =
              Documents.put(uuid, %{id: id, body: body, if_revision: if_revision})

            {revision, inner_acc + json_size(body)}
          end)
          |> elem(1)

        {id, acc + bytes}
      end)

    %{
      doc_ids: doc_ids,
      digest: nil,
      logical_bytes: logical_bytes,
      encoded_payload_bytes: 0,
      metadata: %{"documents" => length(doc_ids), "revisions_each" => 3, "kind" => "json"}
    }
  end

  defp build_fixture(:raw_attachment, seed, uuid) do
    :rand.seed(:exsss, {seed + 3, seed, seed})
    payload = :rand.bytes(65_536)
    put_attachment_fixture(uuid, "raw-doc", "raw", payload)
  end

  defp build_fixture(:zstd_attachment, _seed, uuid) do
    payload = String.duplicate("ZSTD-ATTACHMENT-PAYLOAD-", 8_192)
    put_attachment_fixture(uuid, "zstd-doc", "zstd", payload)
  end

  defp put_attachment_fixture(uuid, doc_id, kind, payload) do
    {:ok, %{blob: digest}} = Attachments.upload_stream(uuid, [payload])
    {:ok, bundle} = DatabaseCatalog.bundle_root(uuid)
    {:ok, stat} = FilesystemStore.stat(bundle, digest)

    {:ok, _} =
      Documents.put(uuid, %{
        id: doc_id,
        body: %{"kind" => kind},
        attachments: %{
          "blob.bin" => %{"blob" => digest, "content_type" => "application/octet-stream"}
        }
      })

    %{
      doc_ids: [doc_id],
      digest: digest,
      logical_bytes: byte_size(payload),
      encoded_payload_bytes: encoded_payload_bytes(bundle, digest, stat),
      metadata: %{
        "kind" => "attachment",
        "encoding" => encoding_name(stat.encoding),
        "logical_bytes" => byte_size(payload)
      }
    }
  end

  defp encoded_payload_bytes(bundle, digest, stat) do
    prefix = String.slice(digest, 0, 2)
    blob = Path.join([bundle, "blobs", prefix, digest <> ".blob"])

    if File.regular?(blob) do
      File.stat!(blob).size - 92
    else
      stat.physical_size
    end
  end

  defp encoding_name(:zstd), do: "zstd"
  defp encoding_name(_encoding), do: "raw"

  defp representation_sha256(_uuid, nil), do: nil

  defp representation_sha256(uuid, digest) do
    {:ok, bundle} = DatabaseCatalog.bundle_root(uuid)
    prefix = String.slice(digest, 0, 2)
    dir = Path.join([bundle, "blobs", prefix])

    case Path.wildcard(Path.join(dir, digest <> ".*")) do
      [path] ->
        path
        |> File.read!()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      _ ->
        nil
    end
  end

  # Times the wire codec by round-tripping the compressed JSON entity that is
  # actually sent, since the in-band codec calls are not observable here.
  defp codec_round_trip_us(_conn, true), do: {0, 0}

  defp codec_round_trip_us(conn, false) do
    with "zstd" <- List.first(Plug.Conn.get_resp_header(conn, "content-encoding")),
         compressed when is_binary(compressed) <- entity_binary(conn.resp_body),
         {decompress_us, json} when is_binary(json) <-
           :timer.tc(fn -> :ezstd.decompress(compressed) end),
         {compress_us, recompressed} when is_binary(recompressed) <-
           :timer.tc(fn -> :ezstd.compress(json, 1) end) do
      {compress_us, decompress_us}
    else
      _ -> {0, 0}
    end
  end

  defp entity_binary(body) when is_binary(body), do: body
  defp entity_binary(body) when is_list(body), do: IO.iodata_to_binary(body)
  defp entity_binary(_body), do: nil

  defp sum_field(requests, field) do
    Enum.reduce(requests, 0, &(Map.fetch!(&1, field) + &2))
  end

  defp sum_kind(requests, kind) do
    requests
    |> Enum.filter(&(&1["payload_kind"] == kind))
    |> Enum.reduce(0, &(&1["entity_bytes"] + &2))
  end

  defp json_size(term) do
    term
    |> JSON.encode_to_iodata!()
    |> IO.iodata_to_binary()
    |> byte_size()
  end

  defp write_report(report, "-") do
    IO.puts(JSON.encode!(report))
  end

  defp write_report(report, path) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, JSON.encode!(report) <> "\n")
  end

  defp print_summary(report, output) do
    IO.puts("replication wire benchmark git=#{report["git_revision"]} output=#{output}")

    Enum.each(report["results"], fn result ->
      IO.puts(
        "#{result["scenario"]} http_body=#{result["http_body_bytes"]} json=#{result["http_json_body_bytes"]} blob=#{result["http_blob_body_bytes"]} elapsed_us=#{result["replication_elapsed_us"]}"
      )
    end)
  end

  defp runtime_metadata do
    %{
      "elixir" => System.version(),
      "otp" => :erlang.system_info(:otp_release) |> to_string(),
      "schedulers_online" => :erlang.system_info(:schedulers_online)
    }
  end

  defp git_revision do
    case System.cmd("git", ["rev-parse", "HEAD"], stderr_to_stdout: true) do
      {revision, 0} -> String.trim(revision)
      _ -> "unknown"
    end
  end

  defp default_output_path do
    timestamp = DateTime.utc_now() |> Calendar.strftime("%Y%m%dT%H%M%SZ")
    Path.join("tmp/replication-wire-bench", "elixirdb-#{timestamp}.json")
  end

  defp unique_suffix,
    do: "#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}"

  defp ensure_test_env! do
    if Mix.env() != :test do
      Mix.raise("replication wire benchmark must run under MIX_ENV=test")
    end
  end

  defp usage do
    """
    mix bench.replication -- --format json --seed 20260812 [--output PATH] [--scenario NAME]
    """
  end
end

ElixirDB.Benchmarks.ReplicationWire.main(System.argv())
