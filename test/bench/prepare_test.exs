defmodule VialKeeper.Bench.PrepareTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Bench.{Checksums, CLI, Downloader, Marker, Prepare, Root, TestHTTP, Tmp}

  setup do
    parent = unique_dir("approved")
    repo = unique_dir("repo")
    root = Path.join(parent, "vk")
    {:ok, ctx} = Root.configure(root, approved_parent: parent, repo_root: repo)
    env = env_opts(parent, repo)
    {:ok, context: ctx, env: env, repo: repo, parent: parent}
  end

  test "prepare trec-covid from a local zip and never writes the corpus into the repo", %{
    context: ctx,
    env: env,
    repo: repo
  } do
    {url, zip_body, md5} = serve_trec_zip()

    spec_override = fn object ->
      Map.merge(object, %{url: url, md5: md5, expected_size: byte_size(zip_body)})
    end

    download = fn context, object, opts ->
      Downloader.download(context, spec_override.(object), opts)
    end

    assert {:ok, result} = Prepare.prepare("trec-covid", Keyword.put(env, :download, download))
    assert result["state"] == "ready"
    assert Marker.present?(result["path"])
    assert Root.descendant?(result["path"], ctx.root)
    assert File.regular?(Path.join(result["path"], "manifest.json"))
    refute corpus_in_repo?(repo)
  end

  test "unsafe root fails before the first HTTP request", %{parent: parent} do
    hits = :atomics.new(1, signed: false)

    download = fn _context, _object, _opts ->
      :atomics.put(hits, 1, 1)
      {:error, "should not download"}
    end

    assert {:error, message} =
             Prepare.prepare("trec-covid",
               approved_parent: parent,
               repo_root: unique_dir("repo"),
               download: download
             )

    assert message =~ "not configured"
    assert :atomics.get(hits, 1) == 0
  end

  test "insufficient free space fails before download", %{parent: parent} do
    hits = :atomics.new(1, signed: false)

    download = fn _context, _object, _opts ->
      :atomics.put(hits, 1, 1)
      {:error, "should not download"}
    end

    repo = unique_dir("repo")
    root = Path.join(parent, "vk-space")
    {:ok, _} = Root.configure(root, approved_parent: parent, repo_root: repo)

    assert {:error, message} =
             Prepare.prepare(
               "trec-covid",
               env_opts(parent, repo)
               |> Keyword.put(:available_bytes_fun, fn _ -> {:ok, 1} end)
               |> Keyword.put(:download, download)
             )

    assert message =~ "insufficient free space"
    assert :atomics.get(hits, 1) == 0
  end

  test "corrupt READY fixture is reported as corrupt", %{env: env, context: ctx} do
    {:ok, dataset} = Root.dataset_path(ctx, "trec-covid", "v1")
    File.mkdir_p!(dataset)
    File.write!(Path.join(dataset, "READY.json"), ~s({"schema_version":1}\n))

    assert {:ok, status} = Prepare.status(env)
    states = Map.new(status["datasets"], &{&1["name"], &1["state"]})
    assert states["trec-covid"] == "corrupt"
  end

  test "prepare pmc smoke from local objects", %{context: ctx, env: env, repo: repo} do
    text = "pmc text body"
    pdf = "%PDF-1.4 tiny"
    jpeg = <<0xFF, 0xD8, 0xFF, 0xD9>>
    extra = "supplement"

    {url, _} =
      start_server(%{
        "/article.txt" => %{body: text},
        "/article.pdf" => %{body: pdf},
        "/figure.jpg" => %{body: jpeg},
        "/supp.bin" => %{body: extra}
      })

    articles = [
      %{
        "pmcid" => "PMC1",
        "version" => 1,
        "license_code" => "CC BY",
        "text_url" => url <> "/article.txt",
        "pdf_url" => url <> "/article.pdf",
        "media_urls" => [url <> "/figure.jpg", url <> "/supp.bin"]
      }
    ]

    assert {:ok, result} =
             Prepare.prepare(
               "pmc",
               env ++ [profile: :smoke, articles: articles]
             )

    assert result["state"] == "ready"
    assert Root.descendant?(result["path"], ctx.root)
    assert File.regular?(Path.join([result["path"], "objects", "PMC1.1", "article.txt"]))
    refute corpus_in_repo?(repo)
  end

  test "prepare open-images standard from a local CSV", %{context: ctx, env: env} do
    assert {:ok, result} = prepare_open_images_csv(env, :standard)
    assert result["state"] == "ready"
    assert Root.descendant?(result["path"], ctx.root)
    assert File.regular?(Path.join([result["path"], "objects", "img-a.jpg"]))
  end

  test "open-images prepare ticks the download progress watchdog", %{env: env} do
    hits = :atomics.new(1, signed: false)

    printer = fn _level, message ->
      if String.contains?(message, "download_objects") do
        :atomics.add_get(hits, 1, 1)
      end
    end

    assert {:ok, result} =
             prepare_open_images_csv(Keyword.put(env, :progress_printer, printer), :standard)

    assert result["state"] == "ready"
    assert :atomics.get(hits, 1) >= 1
  end

  test "open-images 1k preflight is smaller than the 100k budget", %{env: env} do
    twenty = 20 * 1024 * 1024 * 1024
    tight = Keyword.put(env, :available_bytes_fun, fn _ -> {:ok, twenty} end)

    assert {:error, message} =
             Prepare.prepare("open-images", Keyword.put(tight, :profile, :standard))

    assert message =~ "insufficient free space"

    assert {:ok, result} = prepare_open_images_csv(tight, :k1)
    assert result["state"] == "ready"
  end

  test "CLI --help does not continue into status" do
    assert :ok = CLI.run_data(["status", "--help"])
  end

  defp start_server(routes) do
    {:ok, pid} =
      Bandit.start_link(
        plug: {TestHTTP, [routes: routes]},
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        thousand_island_options: [num_acceptors: 1]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :normal) end)
    {"http://127.0.0.1:#{port}", pid}
  end

  defp env_opts(parent, repo) do
    [
      approved_parent: parent,
      repo_root: repo,
      available_bytes_fun: fn _ -> {:ok, 1_000_000_000_000} end
    ]
  end

  defp prepare_open_images_csv(env, profile) do
    jpeg = <<0xFF, 0xD8, 0xFF, 0xD9>>
    md5 = Checksums.md5_iodata(jpeg)
    {url, _} = start_server(%{"/a.jpg" => %{body: jpeg}, "/b.jpg" => %{body: jpeg}})

    csv = """
    ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL,Rotation
    img-a,train,#{url}/a.jpg,https://example.test/a,https://creativecommons.org/licenses/by/2.0/,https://example.test/u,A,Title A,#{byte_size(jpeg)},#{md5},,0
    img-b,train,#{url}/b.jpg,https://example.test/b,https://creativecommons.org/licenses/by/2.0/,https://example.test/u,B,Title B,#{byte_size(jpeg)},#{md5},,0
    """

    path = Path.join(unique_dir("csv"), "info.csv")
    File.write!(path, csv)
    Prepare.prepare("open-images", env ++ [profile: profile, info_csv: path])
  end

  test "status reports missing datasets before prepare", %{env: env, context: ctx} do
    assert {:ok, status} = Prepare.status(env)
    assert status["root"] == ctx.root
    states = Map.new(status["datasets"], &{&1["name"], &1["state"]})
    assert states["trec-covid"] == "missing"
    assert states["pmc"] == "missing"
    assert states["open-images"] == "missing"
  end

  test "clean removes only the named dataset directory", %{context: ctx, env: env} do
    {:ok, dataset} = Root.dataset_path(ctx, "trec-covid", "v1")
    File.mkdir_p!(dataset)
    File.write!(Path.join(dataset, "READY.json"), ~s({"schema_version":1}\n))
    {:ok, other} = Root.dataset_path(ctx, "pmc", "100k-v1")
    File.mkdir_p!(other)
    File.write!(Path.join(other, "keep"), "yes")

    assert :ok = Prepare.clean("trec-covid", env)
    refute File.dir?(dataset)
    assert File.dir?(other)
  end

  test "CLI rejects --root on status" do
    assert_raise Mix.Error, ~r/do not accept --root/, fn ->
      CLI.run_data(["status", "--root", "/tmp/nope"])
    end
  end

  defp serve_trec_zip do
    tmp = unique_dir("zip")
    payload_dir = Path.join(tmp, "payload")
    File.mkdir_p!(Path.join(payload_dir, "trec-covid/qrels"))

    File.write!(Path.join(payload_dir, "trec-covid/corpus.jsonl"), """
    {"_id":"d1","title":"covid","text":"vaccine","metadata":{}}
    """)

    File.write!(Path.join(payload_dir, "trec-covid/queries.jsonl"), """
    {"_id":"q1","text":"covid"}
    """)

    File.write!(Path.join(payload_dir, "trec-covid/qrels/test.tsv"), """
    q1\td1\t2
    """)

    archive = Path.join(tmp, "trec-covid-v1.zip")

    {:ok, _} =
      :zip.create(
        String.to_charlist(archive),
        [
          ~c"trec-covid/corpus.jsonl",
          ~c"trec-covid/queries.jsonl",
          ~c"trec-covid/qrels/test.tsv"
        ],
        cwd: String.to_charlist(payload_dir)
      )

    zip_body = File.read!(archive)
    md5 = Checksums.md5_iodata(zip_body)

    {:ok, pid} =
      Bandit.start_link(
        plug: {TestHTTP, [routes: %{"/trec-covid-v1.zip" => %{body: zip_body}}]},
        scheme: :http,
        ip: {127, 0, 0, 1},
        port: 0,
        thousand_island_options: [num_acceptors: 1]
      )

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :normal) end)
    {"http://127.0.0.1:#{port}/trec-covid-v1.zip", zip_body, md5}
  end

  defp corpus_in_repo?(repo) do
    Path.wildcard(Path.join(repo, "**/*"))
    |> Enum.any?(fn path ->
      name = Path.basename(path)
      name in ["corpus.jsonl", "trec-covid-v1.zip", "manifest.json", "READY.json"]
    end)
  end

  defp unique_dir(prefix), do: Tmp.dir(prefix)
end
