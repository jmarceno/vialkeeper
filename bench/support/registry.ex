defmodule VialKeeper.Bench.Registry do
  @moduledoc """
  Pinned dataset identities for data-backed benchmarks.

  The registry stores source URLs, checksums, selection algorithm versions, and
  size estimates. It never embeds corpus bytes, generated manifests, or image
  ID lists.
  """

  @names ["trec-covid", "pmc", "simplewiki", "open-images"]

  @trec_url "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/trec-covid.zip"
  @trec_size 73_876_720
  @trec_md5 "ce62140cb23feb9becf6270d0d1fe6d1"

  @pmc_https "https://pmc-oa-opendata.s3.amazonaws.com"
  @simplewiki_url "https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles-multistream.xml.bz2"

  @open_images_info_url "https://storage.googleapis.com/openimages/2018_04/train/train-images-with-labels-with-rotation.csv"
  @open_images_labels_url "https://storage.googleapis.com/openimages/v7/oidv7-train-annotations-human-imagelabels.csv"
  @open_images_classes_url "https://storage.googleapis.com/openimages/v7/oidv7-class-descriptions-boxable.csv"

  @spec names() :: [binary()]
  def names, do: @names

  @spec known?(binary()) :: boolean()
  def known?(name) when is_binary(name), do: name in @names

  @spec fetch(binary()) :: {:ok, map()} | {:error, binary()}
  def fetch(name) when is_binary(name) do
    case definition(name) do
      nil -> {:error, "unknown dataset #{name}; known datasets: #{Enum.join(@names, ", ")}"}
      spec -> {:ok, spec}
    end
  end

  @spec fetch!(binary()) :: map()
  def fetch!(name) do
    case fetch(name) do
      {:ok, spec} -> spec
      {:error, message} -> raise ArgumentError, message
    end
  end

  @spec profile(keyword() | binary() | atom()) :: {:ok, atom()} | {:error, binary()}
  def profile(opts) when is_list(opts), do: profile(Keyword.get(opts, :profile, :standard))

  def profile(:standard), do: {:ok, :standard}
  def profile(:smoke), do: {:ok, :smoke}
  def profile(:k1), do: {:ok, :k1}
  def profile(:k10), do: {:ok, :k10}
  def profile("standard"), do: {:ok, :standard}
  def profile("smoke"), do: {:ok, :smoke}
  def profile("1k"), do: {:ok, :k1}
  def profile("10k"), do: {:ok, :k10}

  def profile(other),
    do: {:error, "unknown dataset profile #{inspect(other)}; use standard, smoke, 1k, or 10k"}

  @doc "Returns whether `profile` is valid for `name`."
  @spec ensure_profile(binary(), atom()) :: :ok | {:error, binary()}
  def ensure_profile(name, profile) when is_binary(name) and is_atom(profile) do
    if supported_profile?(name, profile) do
      :ok
    else
      {:error, "dataset #{name} does not support profile #{profile}; #{profile_hint(name)}"}
    end
  end

  @spec selection_count(binary(), atom()) :: pos_integer()
  def selection_count("pmc", :standard), do: 100_000
  def selection_count("pmc", :smoke), do: 3
  def selection_count("simplewiki", :standard), do: 100_000
  def selection_count("simplewiki", :smoke), do: 3
  def selection_count("open-images", :standard), do: 100_000
  def selection_count("open-images", :k10), do: 10_000
  def selection_count("open-images", :k1), do: 1_000
  def selection_count("open-images", :smoke), do: 1
  def selection_count("trec-covid", _), do: 171_332

  defp supported_profile?("open-images", profile) when profile in [:standard, :smoke, :k1, :k10],
    do: true

  defp supported_profile?(_name, profile) when profile in [:standard, :smoke], do: true
  defp supported_profile?(_name, _profile), do: false

  defp profile_hint("open-images"), do: "use standard, smoke, 1k, or 10k"
  defp profile_hint(_name), do: "use standard or smoke"

  defp definition("trec-covid") do
    %{
      "name" => "trec-covid",
      "version" => "v1",
      "title" => "TREC-COVID / BEIR full-text search",
      "source_url" => @trec_url,
      "expected_size_bytes" => @trec_size,
      "md5" => @trec_md5,
      "archive_name" => "trec-covid-v1.zip",
      "expected_documents" => 171_332,
      "expected_queries" => 50,
      "estimated_source_bytes" => @trec_size,
      "estimated_working_bytes" => 5 * gib(),
      "source_bytes_estimated?" => false
    }
  end

  defp definition("pmc") do
    %{
      "name" => "pmc",
      "version" => "100k-v1",
      "title" => "PMC Open Access system stress",
      "https_origin" => @pmc_https,
      "inventory_prefix" => "inventory-reports/pmc-oa-opendata/metadata/",
      "selection_algorithm" => "sha256-rank-v1",
      "selection_seed" => "vialkeeper-pmc-100k-v1",
      "attachment_budget_bytes" => 20 * gib(),
      "pdf_budget_bytes" => 10 * gib(),
      "image_budget_bytes" => 5 * gib(),
      "supplement_budget_bytes" => 5 * gib(),
      "approved_license_prefixes" => ["CC "],
      "estimated_source_bytes" => 70 * gib(),
      "estimated_working_bytes" => 80 * gib(),
      "source_bytes_estimated?" => true,
      "smoke_articles" => pmc_smoke_articles()
    }
  end

  defp definition("simplewiki") do
    %{
      "name" => "simplewiki",
      "version" => "v1",
      "title" => "Simple English Wikipedia catalog stress",
      "source_url" => @simplewiki_url,
      "archive_name" => "simplewiki-pages-articles-multistream.xml.bz2",
      "selection_algorithm" => "first-current-main-v1",
      "selection_seed" => "vialkeeper-simplewiki-v1",
      "attachment_count" => 800,
      "attachment_bytes" => 640 * 64 * 1024 + 120 * 1024 * 1024 + 40 * 16 * 1024 * 1024,
      "estimated_source_bytes" => 2 * gib(),
      "estimated_working_bytes" => 4 * gib(),
      "source_bytes_estimated?" => true
    }
  end

  defp definition("open-images") do
    %{
      "name" => "open-images",
      "version" => "v7-100k-v1",
      "title" => "Open Images V7 attachment torture",
      "split" => "train",
      "image_info_url" => @open_images_info_url,
      "labels_url" => @open_images_labels_url,
      "classes_url" => @open_images_classes_url,
      "selection_algorithm" => "sha256-rank-v1",
      "selection_seed" => "vialkeeper-open-images-v7-100k-v1",
      "estimated_source_bytes" => 60 * gib(),
      "estimated_working_bytes" => 70 * gib(),
      "source_bytes_estimated?" => true,
      "smoke_images" => open_images_smoke()
    }
  end

  defp definition(_), do: nil

  defp pmc_smoke_articles do
    [
      %{
        "pmcid" => "PMC12855588",
        "version" => 1,
        "text_url" =>
          @pmc_https <> "/PMC12855588.1/PMC12855588.1.txt?md5=95d78e2bc6318fb728abb7137ad0049e",
        "pdf_url" =>
          @pmc_https <> "/PMC12855588.1/PMC12855588.1.pdf?md5=8546223bd7ec0f01313ec7c4903fb9dc",
        "metadata_url" => @pmc_https <> "/metadata/PMC12855588.1.json",
        "media_urls" => [
          @pmc_https <> "/PMC12855588.1/fx1.jpg?md5=7e7e95a91dedf32760eda1032d632d24",
          @pmc_https <> "/PMC12855588.1/mmc1.pdf?md5=e3b8ac93beae30fc40ae7e01421ab566"
        ]
      },
      %{
        "pmcid" => "PMC12788873",
        "version" => 1,
        "text_url" => @pmc_https <> "/PMC12788873.1/PMC12788873.1.txt",
        "pdf_url" => nil,
        "metadata_url" => @pmc_https <> "/metadata/PMC12788873.1.json",
        "media_urls" => []
      },
      %{
        "pmcid" => "PMC10009416",
        "version" => 1,
        "text_url" => @pmc_https <> "/PMC10009416.1/PMC10009416.1.txt",
        "pdf_url" => @pmc_https <> "/PMC10009416.1/PMC10009416.1.pdf",
        "metadata_url" => @pmc_https <> "/metadata/PMC10009416.1.json",
        "media_urls" => [
          @pmc_https <> "/PMC10009416.1/NPR2-43-85-g001.jpg",
          @pmc_https <> "/PMC10009416.1/NPR2-43-85-s001.xlsx"
        ]
      }
    ]
  end

  defp open_images_smoke do
    [
      %{
        "image_id" => "000060e3121c7305",
        "split" => "train",
        "url" => "https://c1.staticflickr.com/5/4129/5215831864_46f356962f_o.jpg",
        "expected_size" => 211_079,
        "md5" => "0Sad+xMj2ttXM1U8meEJ0A==",
        "title" => "28 Nov 2010 Our new house.",
        "labels" => []
      }
    ]
  end

  defp gib, do: 1024 * 1024 * 1024
end
