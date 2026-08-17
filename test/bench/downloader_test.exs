defmodule VialKeeper.Bench.DownloaderTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Bench.{Checksums, Downloader, Root, Tmp}

  setup do
    parent = unique_dir("approved")
    repo = unique_dir("repo")
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, approved_parent: parent, repo_root: repo)
    {:ok, context: ctx, parent: parent, repo: repo}
  end

  test "complete download verifies checksum", %{context: ctx} do
    body = "hello-bench"
    md5 = Checksums.md5_iodata(body)
    {url, _} = start_server(%{"/file.bin" => %{body: body}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "file.bin"])

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               md5: md5,
               expected_size: byte_size(body)
             })

    assert File.read!(dest) == body
  end

  test "checksum mismatch leaves the object unready", %{context: ctx} do
    body = "hello-bench"
    {url, _} = start_server(%{"/file.bin" => %{body: body}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "bad.bin"])

    assert {:error, message} =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               md5: "00000000000000000000000000000000"
             })

    assert message =~ "MD5"
    refute File.regular?(dest)
  end

  test "resumes a .part file when the server returns 206", %{context: ctx} do
    body = "abcdefghij"
    {url, _} = start_server(%{"/file.bin" => %{body: body, support_range: true}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "resume.bin"])
    File.mkdir_p!(Path.dirname(dest))
    File.write!(dest <> ".part", "abcde")

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               expected_size: byte_size(body)
             })

    assert File.read!(dest) == body
  end

  test "a 200 response restarts instead of appending", %{context: ctx} do
    body = "abcdefghij"
    {url, _} = start_server(%{"/file.bin" => %{body: body, support_range: false}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "restart.bin"])
    File.mkdir_p!(Path.dirname(dest))
    File.write!(dest <> ".part", "xxxxx")

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               expected_size: byte_size(body)
             })

    assert File.read!(dest) == body
  end

  test "rejects HTML payloads", %{context: ctx} do
    {url, _} =
      start_server(%{
        "/file.bin" => %{body: "<html>nope</html>", content_type: "text/html"}
      })

    {:ok, dest} = Root.resolve(ctx, ["staging", "html.bin"])

    assert {:error, message} =
             Downloader.download(ctx, %{url: url <> "/file.bin", dest: dest})

    assert message =~ "HTML"
  end

  test "accepts checksum-authenticated HTML objects", %{context: ctx} do
    body = "<html><body>valid supplement</body></html>"
    md5 = Checksums.md5_iodata(body)
    {url, _} = start_server(%{"/supplement.html" => %{body: body, content_type: "text/html"}})

    {:ok, dest} = Root.resolve(ctx, ["staging", "supplement.html"])

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/supplement.html",
               dest: dest,
               md5: md5
             })

    assert File.read!(dest) == body
  end

  test "accepts a current S3 ETag when a manifest MD5 is stale", %{context: ctx} do
    body = "current-source-body"
    md5 = Checksums.md5_iodata(body)
    {url, _} = start_server(%{"/file.bin" => %{body: body, etag: ~s("#{md5}")}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "etag.bin"])

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               md5: "00000000000000000000000000000000"
             })

    assert File.read!(dest) == body
  end

  test "refuses destinations outside the benchmark root", %{context: ctx} do
    {url, _} = start_server(%{"/file.bin" => %{body: "x"}})
    outside = Path.join(System.tmp_dir!(), "vk-escape-#{System.unique_integer([:positive])}")

    assert {:error, message} =
             Downloader.download(ctx, %{url: url <> "/file.bin", dest: outside})

    assert message =~ "outside"
    refute File.exists?(outside)
  end

  test "does not retry HTTP 404", %{context: ctx} do
    hits = :atomics.new(1, signed: false)
    {url, _} = start_status_server(404, hits)
    {:ok, dest} = Root.resolve(ctx, ["staging", "missing.bin"])

    assert {:error, message} =
             Downloader.download(ctx, %{url: url <> "/file.bin", dest: dest})

    assert message =~ "HTTP 404"
    assert :atomics.get(hits, 1) == 1
    refute File.regular?(dest)
  end

  test "retries a transient HTTP failure", %{context: ctx} do
    body = "retry-ok"
    {:ok, dest} = Root.resolve(ctx, ["staging", "retry.bin"])
    hits = :atomics.new(1, signed: false)
    {url, _} = start_flaky_server(body, hits)

    assert :ok =
             Downloader.download(ctx, %{
               url: url <> "/file.bin",
               dest: dest,
               expected_size: byte_size(body)
             })

    assert File.read!(dest) == body
    assert :atomics.get(hits, 1) >= 2
  end

  test "an interrupted object stays a .part until a later complete download", %{context: ctx} do
    body = "complete-body"
    {url, _} = start_server(%{"/file.bin" => %{body: body, drop_after: 4}})
    {:ok, dest} = Root.resolve(ctx, ["staging", "partial.bin"])

    assert {:error, _} =
             Downloader.download(
               ctx,
               %{
                 url: url <> "/file.bin",
                 dest: dest,
                 expected_size: byte_size(body)
               },
               max_retries: 1
             )

    refute File.regular?(dest)
  end

  defp start_server(routes) do
    {:ok, pid} =
      Bandit.start_link(
        plug: {VialKeeper.Bench.TestHTTP, [routes: routes]},
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        thousand_island_options: [num_acceptors: 1]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    on_exit(fn -> stop_server(pid) end)
    {"http://127.0.0.1:#{port}", pid}
  end

  defp start_flaky_server(body, hits) do
    start_plug_server({__MODULE__.Flaky, [body: body, hits: hits]})
  end

  defp start_status_server(status, hits) do
    start_plug_server({__MODULE__.Status, [status: status, hits: hits]})
  end

  defp start_plug_server(plug) do
    {:ok, pid} =
      Bandit.start_link(
        plug: plug,
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        thousand_island_options: [num_acceptors: 1]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    on_exit(fn -> stop_server(pid) end)
    {"http://127.0.0.1:#{port}", pid}
  end

  defp stop_server(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :normal)
    :ok
  end

  defp unique_dir(prefix), do: Tmp.dir(prefix)

  defmodule Status do
    def init(opts), do: opts

    def call(conn, opts) do
      hits = Keyword.fetch!(opts, :hits)
      status = Keyword.fetch!(opts, :status)
      _ = :atomics.add_get(hits, 1, 1)
      Plug.Conn.send_resp(conn, status, "missing")
    end
  end

  defmodule Flaky do
    def init(opts), do: opts

    def call(conn, opts) do
      hits = Keyword.fetch!(opts, :hits)
      body = Keyword.fetch!(opts, :body)
      n = :atomics.add_get(hits, 1, 1)

      if n == 1 do
        Plug.Conn.send_resp(conn, 500, "nope")
      else
        Plug.Conn.send_resp(conn, 200, body)
      end
    end
  end
end
