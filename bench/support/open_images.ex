defmodule VialKeeper.Bench.OpenImages do
  @moduledoc "Open Images V7 selection and attachment mapping."

  alias VialKeeper.Bench.{Checksums, CSV, Select}

  @cvdf_train_prefix "https://s3.amazonaws.com/open-images-dataset/train/"

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
        "url" => image["original_url"] || image["url"],
        "license" => image["license"]
      }
    }
  end

  @doc """
  Points official train objects at the CVDF S3 copies.

  Flickr `OriginalURL` values in the image-info CSV are not a durable byte
  source; many return HTTP 404. CVDF hosts the same train IDs as JPEG objects.
  Original MD5/size apply to the Flickr originals, not the CVDF files.
  """
  @spec use_cvdf_bytes(map()) :: map()
  def use_cvdf_bytes(%{"images" => images} = manifest) when is_list(images) do
    %{manifest | "images" => Enum.map(images, &cvdf_image/1)}
  end

  defp cvdf_image(image) do
    image
    |> Map.put("original_url", image["original_url"] || image["url"])
    |> Map.put("url", @cvdf_train_prefix <> image["image_id"] <> ".jpg")
    |> Map.put("md5", nil)
    |> Map.put("expected_size", nil)
  end

  @doc "Keeps the first `wanted` images whose JPEG exists under `staging/objects/`."
  @spec keep_downloaded(map(), Path.t(), pos_integer()) :: {:ok, map()} | {:error, binary()}
  def keep_downloaded(%{"images" => images} = manifest, staging, wanted)
      when is_list(images) and is_binary(staging) and is_integer(wanted) and wanted > 0 do
    present =
      Enum.filter(images, fn image ->
        File.regular?(Path.join([staging, "objects", image["image_id"] <> ".jpg"]))
      end)

    required = min(wanted, length(images))
    kept = Enum.take(present, required)

    if length(kept) < required do
      {:error, "open-images downloaded #{length(kept)} images; need #{required}"}
    else
      drop_unselected_objects(staging, images, kept)
      {:ok, %{manifest | "images" => kept}}
    end
  end

  defp drop_unselected_objects(staging, images, kept) do
    keep = MapSet.new(kept, & &1["image_id"])

    Enum.each(images, fn image ->
      id = image["image_id"]

      if id in keep do
        :ok
      else
        _ = File.rm(Path.join([staging, "objects", id <> ".jpg"]))
      end
    end)
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

  @spec generate_from_info_csv(Path.t(), map(), pos_integer(), binary()) ::
          {:ok, map()} | {:error, binary()}
  def generate_from_info_csv(path, spec, count, profile \\ "standard")
      when is_binary(path) and is_map(spec) and is_integer(count) and count > 0 and
             is_binary(profile) do
    case stream_info_rows(path) do
      {:error, reason} ->
        {:error, reason}

      stream ->
        images = Enum.take(stream, count)
        {:ok, manifest_from_images(spec, profile, images)}
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
