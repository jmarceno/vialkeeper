defmodule VialKeeper.Bench.Pmc do
  @moduledoc "PMC Open Access fixture mapping and deterministic selection."

  alias VialKeeper.Bench.{Checksums, CSV, Gzip, Select}

  @https "https://pmc-oa-opendata.s3.amazonaws.com"

  @spec smoke_manifest(map(), keyword()) :: map()
  def smoke_manifest(spec, opts \\ []) do
    articles =
      Keyword.get(opts, :articles, spec["smoke_articles"])
      |> Enum.map(&normalize_article/1)

    %{
      "dataset" => "pmc",
      "version" => spec["version"],
      "profile" => "smoke",
      "selection_algorithm" => spec["selection_algorithm"],
      "selection_seed" => spec["selection_seed"],
      "articles" => articles,
      "expected_source_bytes" => Enum.reduce(articles, 0, &(&1["expected_bytes"] + &2))
    }
  end

  @spec objects_for(map()) :: [map()]
  def objects_for(%{"articles" => articles}) do
    Enum.flat_map(articles, &article_objects/1)
  end

  @spec document_body(map(), binary()) :: map()
  def document_body(article, text) when is_map(article) and is_binary(text) do
    %{
      "pmcid" => article["pmcid"],
      "version" => article["version"],
      "pmid" => article["pmid"],
      "doi" => article["doi"],
      "title" => article["title"],
      "citation" => article["citation"],
      "license" => article["license_code"],
      "text" => text
    }
  end

  @spec classify_name(binary()) :: binary()
  def classify_name(name) when is_binary(name) do
    down = String.downcase(name)

    if String.starts_with?(down, "mmc") do
      "supplement"
    else
      classify_ext(down)
    end
  end

  defp classify_ext(down) do
    if String.ends_with?(down, ".pdf") do
      "pdf"
    else
      classify_image(down)
    end
  end

  defp classify_image(down) do
    if String.ends_with?(down, ".jpg") or String.ends_with?(down, ".jpeg") or
         String.ends_with?(down, ".png") or String.ends_with?(down, ".gif") do
      "image"
    else
      "supplement"
    end
  end

  @spec parse_object_url(binary()) :: {:ok, map()} | {:error, binary()}
  def parse_object_url(url) when is_binary(url) do
    uri = URI.parse(url)

    case uri.scheme do
      "s3" ->
        key = String.trim_leading(uri.path || "", "/")
        object_from_https(@https <> "/" <> key, query_md5(uri))

      "https" ->
        object_from_https(strip_query(url), query_md5(uri))

      "http" ->
        object_from_https(strip_query(url), query_md5(uri))

      _ ->
        {:error, "unsupported PMC object URL #{url}"}
    end
  end

  defp object_from_https(https, md5) do
    {:ok,
     %{
       "url" => https,
       "md5" => md5,
       "name" => Path.basename(URI.parse(https).path || "")
     }}
  end

  @spec approved_license?(term(), [binary()]) :: boolean()
  def approved_license?(code, prefixes) when is_list(prefixes) do
    is_binary(code) and Enum.any?(prefixes, &String.starts_with?(code, &1))
  end

  @spec rank_key(binary(), binary(), term()) :: binary()
  def rank_key(seed, pmcid, version) do
    label = seed <> ":" <> pmcid <> "." <> to_string(version)
    :crypto.hash(:sha256, label) |> Base.encode16(case: :lower)
  end

  @spec select_from_metadata([map()], map(), pos_integer()) :: [map()]
  def select_from_metadata(rows, spec, count)
      when is_list(rows) and is_map(spec) and is_integer(count) and count > 0 do
    prefixes = spec["approved_license_prefixes"] || ["CC "]

    rows
    |> Enum.filter(&selectable?(&1, prefixes))
    |> Enum.sort_by(&rank_key(spec["selection_seed"], &1["pmcid"], &1["version"]))
    |> Enum.take(count)
  end

  @spec generate_manifest([map()], map(), pos_integer()) :: map()
  def generate_manifest(rows, spec, count)
      when is_list(rows) and is_map(spec) and is_integer(count) and count > 0 do
    articles =
      rows
      |> select_from_metadata(spec, count)
      |> Enum.map(&normalize_article/1)
      |> assign_attachment_budget(spec)

    %{
      "dataset" => "pmc",
      "version" => spec["version"],
      "profile" => "standard",
      "selection_algorithm" => spec["selection_algorithm"],
      "selection_seed" => spec["selection_seed"],
      "inventory_snapshot" => spec["inventory_snapshot"],
      "articles" => articles,
      "expected_source_bytes" => Enum.reduce(articles, 0, &(&1["expected_bytes"] + &2))
    }
  end

  @spec generate_from_jsonl(Path.t(), map(), pos_integer()) :: {:ok, map()} | {:error, binary()}
  def generate_from_jsonl(path, spec, count)
      when is_binary(path) and is_map(spec) and is_integer(count) and count > 0 do
    rows =
      path
      |> File.stream!()
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Enum.reduce_while([], fn line, acc ->
        case JSON.decode(line) do
          {:ok, row} when is_map(row) -> {:cont, [row | acc]}
          _ -> {:halt, {:error, "PMC metadata JSONL contains an invalid row"}}
        end
      end)

    case rows do
      {:error, reason} -> {:error, reason}
      list -> {:ok, generate_manifest(Enum.reverse(list), spec, count)}
    end
  end

  @spec inventory_keys(Path.t()) :: Enumerable.t()
  def inventory_keys(path) when is_binary(path) do
    path
    |> Gzip.stream_lines()
    |> Stream.map(&parse_inventory_key/1)
    |> Stream.reject(&is_nil/1)
  end

  @spec parse_inventory_key(binary()) :: map() | nil
  def parse_inventory_key(line) when is_binary(line) do
    case CSV.parse_line(line) do
      {:ok, [_bucket, key | _]} -> parse_metadata_key(key)
      _ -> nil
    end
  end

  @spec select_inventory_keys(Enumerable.t(), map(), pos_integer()) :: [map()]
  def select_inventory_keys(keys, spec, count) do
    Select.smallest(keys, count, fn key ->
      {rank_key(spec["selection_seed"], key["pmcid"], key["version"]),
       key["pmcid"] <> "." <> to_string(key["version"])}
    end)
  end

  @spec metadata_https(binary()) :: binary()
  def metadata_https(key) when is_binary(key), do: @https <> "/" <> key

  @spec parse_metadata_key(binary()) :: map() | nil
  def parse_metadata_key(key) when is_binary(key) do
    case Regex.run(~r/^metadata\/(PMC\d+)\.(\d+)\.json$/, key) do
      [_, pmcid, version] ->
        %{
          "key" => key,
          "pmcid" => pmcid,
          "version" => String.to_integer(version)
        }

      _ ->
        nil
    end
  end

  @spec assign_attachment_budget([map()], map()) :: [map()]
  def assign_attachment_budget(articles, spec) when is_list(articles) and is_map(spec) do
    budgets = %{
      "pdf" => fetch_budget(spec, "pdf_budget_bytes"),
      "image" => fetch_budget(spec, "image_budget_bytes"),
      "supplement" => fetch_budget(spec, "supplement_budget_bytes")
    }

    {kept, remaining} =
      Enum.map_reduce(articles, budgets, fn article, budgets ->
        take_article_attachments(article, budgets)
      end)

    spill_remaining(kept, remaining)
  end

  defp fetch_budget(spec, key) do
    case Map.fetch(spec, key) do
      {:ok, n} when is_integer(n) and n >= 0 -> n
      _ -> 0
    end
  end

  defp take_article_attachments(article, budgets) do
    {kept, budgets} =
      Enum.map_reduce(article["attachments"] || [], budgets, fn obj, budgets ->
        category = obj["category"] || classify_name(obj["name"] || "")
        {size, estimated?} = object_size(obj, category)
        remaining = Map.get(budgets, category, 0)
        obj = obj |> Map.put("expected_size", size) |> Map.put("size_estimated?", estimated?)
        commit_budgeted_object(obj, category, size, remaining, budgets)
      end)

    kept = Enum.reject(kept, &is_nil/1)

    article =
      article
      |> Map.put("attachments", kept)
      |> Map.put("expected_bytes", article_bytes(article, kept))

    {article, budgets}
  end

  defp commit_budgeted_object(obj, category, size, remaining, budgets) do
    left = remaining - size

    if left < 0 do
      {nil, budgets}
    else
      {obj, Map.put(budgets, category, left)}
    end
  end

  defp object_size(%{"expected_size" => size}, _category)
       when is_integer(size) and size > 0 do
    {size, false}
  end

  defp object_size(_obj, "pdf"), do: {2 * 1024 * 1024, true}
  defp object_size(_obj, "image"), do: {200 * 1024, true}
  defp object_size(_obj, _category), do: {1024 * 1024, true}

  defp spill_remaining(articles, remaining) do
    leftover = Enum.reduce(remaining, 0, fn {_category, bytes}, acc -> acc + bytes end)
    spill_unused(articles, leftover)
  end

  defp spill_unused(articles, leftover) when leftover > 0 do
    {taken, _} =
      articles
      |> unused_attachments()
      |> Enum.reduce({[], leftover}, &take_spilled/2)

    merge_spilled(articles, Enum.reverse(taken))
  end

  defp spill_unused(articles, _leftover), do: articles

  defp unused_attachments(articles) do
    Enum.flat_map(articles, fn article ->
      chosen = MapSet.new(article["attachments"] || [], & &1["url"])

      (article["all_attachments"] || [])
      |> Enum.reject(&MapSet.member?(chosen, &1["url"]))
      |> Enum.map(&Map.put(&1, "owner_pmcid", article["pmcid"]))
    end)
  end

  defp take_spilled(obj, {acc, leftover}) do
    category = obj["category"] || classify_name(obj["name"] || "")
    {size, estimated?} = object_size(obj, category)

    if size <= leftover do
      spilled =
        obj
        |> Map.put("expected_size", size)
        |> Map.put("size_estimated?", estimated?)

      {[spilled | acc], leftover - size}
    else
      {acc, leftover}
    end
  end

  defp merge_spilled(articles, spilled) do
    by_id = Enum.group_by(spilled, & &1["owner_pmcid"])

    Enum.map(articles, fn article ->
      extra =
        by_id
        |> Map.get(article["pmcid"], [])
        |> Enum.map(&Map.delete(&1, "owner_pmcid"))

      attachments = (article["attachments"] || []) ++ extra

      %{
        article
        | "attachments" => attachments,
          "expected_bytes" => article_bytes(article, attachments)
      }
    end)
  end

  defp article_bytes(article, attachments) do
    text = get_in(article, ["text", "expected_size"]) || 0
    meta = get_in(article, ["metadata", "expected_size"]) || 0
    text + meta + Enum.reduce(attachments, 0, &((&1["expected_size"] || 0) + &2))
  end

  defp selectable?(row, prefixes) do
    row["is_pmc_openaccess"] == true and row["is_retracted"] != true and
      is_binary(row["text_url"] || row["xml_url"]) and
      approved_license?(row["license_code"], prefixes)
  end

  defp normalize_article(article) when is_map(article) do
    text = object_or_nil(article["text_url"])
    pdf = object_or_nil(article["pdf_url"])
    metadata = object_or_nil(article["metadata_url"])

    media =
      (article["media_urls"] || [])
      |> Enum.map(&object_or_nil/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.map(fn obj -> Map.put(obj, "category", classify_name(obj["name"])) end)

    attachments =
      [pdf && Map.put(pdf, "category", "pdf") | media]
      |> Enum.reject(&is_nil/1)

    %{
      "pmcid" => article["pmcid"],
      "version" => article["version"],
      "pmid" => article["pmid"],
      "doi" => article["doi"],
      "title" => article["title"],
      "citation" => article["citation"],
      "license_code" => article["license_code"],
      "text" => text,
      "metadata" => metadata,
      "attachments" => attachments,
      "all_attachments" => attachments,
      "expected_bytes" => article_bytes(%{"text" => text, "metadata" => metadata}, attachments)
    }
  end

  defp article_objects(article) do
    [article["text"], article["metadata"] | article["attachments"] || []]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn obj ->
      %{
        url: obj["url"],
        dest_name: dest_name(article, obj),
        md5: obj["md5"],
        expected_size: obj["expected_size"]
      }
    end)
  end

  defp dest_name(article, obj) do
    prefix = article["pmcid"] <> "." <> to_string(article["version"])
    Path.join(prefix, obj["name"] || "object.bin")
  end

  defp object_or_nil(nil), do: nil

  defp object_or_nil(url) when is_binary(url) do
    case parse_object_url(url) do
      {:ok, obj} -> obj
      {:error, _} -> nil
    end
  end

  defp query_md5(%URI{query: query}) when is_binary(query) do
    query
    |> URI.decode_query()
    |> Map.get("md5")
    |> case do
      md5 when is_binary(md5) ->
        case Checksums.canonicalize_md5(md5) do
          {:ok, hex} -> hex
          _ -> md5
        end

      _ ->
        nil
    end
  end

  defp query_md5(_), do: nil

  defp strip_query(url) do
    uri = URI.parse(url)
    URI.to_string(%{uri | query: nil, fragment: nil})
  end
end
