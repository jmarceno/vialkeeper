defmodule VialKeeper.Bench.OpenImages do
  @moduledoc "Open Images V7 selection and attachment mapping."

  alias VialKeeper.Bench.{Checksums, CSV, Select}

  @spec smoke_manifest(map(), keyword()) :: map()
  def smoke_manifest(spec, opts \\ []) do
    images = Keyword.get(opts, :images, spec["smoke_images"]) |> Enum.map(&normalize_image/1)
    manifest_from_images(spec, "smoke", images)
  end

  @spec objects_for(map()) :: [map()]
  def objects_for(%{"images" => images}) do
    Enum.map(images, fn image ->
      %{
        url: image["url"],
        dest_name: image["image_id"] <> ".jpg",
        md5: image["md5"],
        expected_size: image["expected_size"]
      }
    end)
  end

  @spec document_body(map()) :: map()
  def document_body(image) when is_map(image) do
    %{
      "image_id" => image["image_id"],
      "split" => image["split"] || "train",
      "labels" => image["labels"] || [],
      "title" => image["title"] || image["image_id"],
      "source_metadata" => %{
        "url" => image["url"],
        "license" => image["license"]
      }
    }
  end

  @spec rank_key(binary(), binary()) :: binary()
  def rank_key(seed, image_id) when is_binary(seed) and is_binary(image_id) do
    :crypto.hash(:sha256, seed <> ":" <> image_id) |> Base.encode16(case: :lower)
  end

  @spec select_ids(Enumerable.t(), binary(), pos_integer()) :: [binary()]
  def select_ids(ids, seed, count) do
    ids
    |> Select.smallest(count, fn id -> {rank_key(seed, id), id} end)
  end

  @spec parse_info_row(binary(), [binary()]) :: {:ok, map()} | :skip | {:error, binary()}
  def parse_info_row(line, headers) when is_binary(line) and is_list(headers) do
    case parse_csv_line(line) do
      {:ok, fields} ->
        row = headers |> Enum.zip(fields) |> Map.new()
        from_info_row(row)

      :skip ->
        :skip

      error ->
        error
    end
  end

  defp from_info_row(row) do
    id = row["ImageID"]
    url = row["OriginalURL"]

    if usable_info?(id, url) do
      {:ok, info_image(row, id, url)}
    else
      :skip
    end
  end

  defp usable_info?(id, url), do: is_binary(id) and id != "" and is_binary(url) and url != ""

  defp info_image(row, id, url) do
    %{
      "image_id" => id,
      "split" => row["Subset"] || "train",
      "url" => url,
      "expected_size" => parse_optional_int(row["OriginalSize"]),
      "md5" => optional_md5(row["OriginalMD5"]),
      "title" => row["Title"],
      "license" => row["License"],
      "labels" => []
    }
  end

  defp optional_md5(value) do
    case Checksums.canonicalize_md5(value || "") do
      {:ok, hex} -> hex
      _ -> nil
    end
  end

  defp parse_optional_int(value) do
    case Integer.parse(value || "") do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  defp normalize_image(image) when is_map(image) do
    md5 =
      case Checksums.canonicalize_md5(image["md5"] || "") do
        {:ok, hex} -> hex
        _ -> image["md5"]
      end

    %{
      "image_id" => image["image_id"],
      "split" => image["split"] || "train",
      "url" => image["url"],
      "expected_size" => image["expected_size"],
      "md5" => md5,
      "title" => image["title"],
      "license" => image["license"],
      "labels" => image["labels"] || []
    }
  end

  @spec parse_csv_line(binary()) :: {:ok, [binary()]} | :skip | {:error, binary()}
  def parse_csv_line(line) when is_binary(line), do: CSV.parse_line(line)

  @spec generate_from_info_csv(Path.t(), map(), pos_integer()) :: {:ok, map()} | {:error, binary()}
  def generate_from_info_csv(path, spec, count)
      when is_binary(path) and is_map(spec) and is_integer(count) and count > 0 do
    case stream_info_rows(path) do
      {:error, reason} ->
        {:error, reason}

      stream ->
        images =
          stream
          |> Select.smallest(count, fn image ->
            {rank_key(spec["selection_seed"], image["image_id"]), image["image_id"]}
          end)

        {:ok, manifest_from_images(spec, "standard", images)}
    end
  end

  @spec merge_labels([map()], Path.t(), Path.t()) :: [map()]
  def merge_labels(images, labels_path, classes_path)
      when is_list(images) and is_binary(labels_path) and is_binary(classes_path) do
    names = class_names(classes_path)
    wanted = MapSet.new(images, & &1["image_id"])

    labels =
      labels_path
      |> File.stream!()
      |> Stream.with_index()
      |> Enum.reduce(%{}, fn {line, index}, acc ->
        collect_label(line, index, wanted, names, acc)
      end)

    Enum.map(images, fn image ->
      Map.put(image, "labels", Enum.sort(Map.get(labels, image["image_id"], [])))
    end)
  end

  defp manifest_from_images(spec, profile, images) do
    %{
      "dataset" => "open-images",
      "version" => spec["version"],
      "profile" => profile,
      "split" => spec["split"],
      "selection_algorithm" => spec["selection_algorithm"],
      "selection_seed" => spec["selection_seed"],
      "images" => images,
      "expected_source_bytes" => Enum.reduce(images, 0, &((&1["expected_size"] || 0) + &2))
    }
  end

  defp stream_info_rows(path) do
    if File.regular?(path) do
      path
      |> File.stream!()
      |> Stream.with_index()
      |> Stream.transform(nil, fn {line, index}, headers ->
        parse_info_stream_line(line, index, headers)
      end)
    else
      {:error, "Open Images CSV is missing: #{path}"}
    end
  end

  defp parse_info_stream_line(line, 0, nil) do
    case CSV.parse_line(line) do
      {:ok, headers} -> {[], headers}
      :skip -> {[], nil}
      {:error, _} -> {[], nil}
    end
  end

  defp parse_info_stream_line(_line, _index, nil), do: {[], nil}

  defp parse_info_stream_line(line, _index, headers) when is_list(headers) do
    case parse_info_row(line, headers) do
      {:ok, image} -> {[image], headers}
      _ -> {[], headers}
    end
  end

  defp class_names(path) do
    path
    |> File.stream!()
    |> Stream.map(&CSV.parse_line/1)
    |> Enum.reduce(%{}, fn
      {:ok, [id, name | _]}, acc -> Map.put(acc, id, name)
      _, acc -> acc
    end)
  end

  defp collect_label(_line, 0, _wanted, _names, acc) do
    acc
  end

  defp collect_label(line, _index, wanted, names, acc) do
    case CSV.parse_line(line) do
      {:ok, [image_id, _source, label_id | _]} ->
        if MapSet.member?(wanted, image_id) do
          label = Map.get(names, label_id, label_id)
          Map.update(acc, image_id, [label], &[label | &1])
        else
          acc
        end

      _ ->
        acc
    end
  end
end
