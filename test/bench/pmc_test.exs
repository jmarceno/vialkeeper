defmodule VialKeeper.Bench.PmcTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.{Pmc, Registry}

  test "converts S3 object URLs and keeps the MD5 query value" do
    url =
      "s3://pmc-oa-opendata/PMC12855588.1/PMC12855588.1.pdf?md5=8546223bd7ec0f01313ec7c4903fb9dc"

    assert {:ok, obj} = Pmc.parse_object_url(url)
    assert obj["url"] =~ "https://pmc-oa-opendata.s3.amazonaws.com/PMC12855588.1/"
    assert obj["md5"] == "8546223bd7ec0f01313ec7c4903fb9dc"
    assert obj["name"] == "PMC12855588.1.pdf"
  end

  test "maps an article into a VialKeeper document and attachment names" do
    spec = Registry.fetch!("pmc")
    manifest = Pmc.smoke_manifest(spec)
    [article | _] = manifest["articles"]

    body = Pmc.document_body(article, "full text")
    assert body["pmcid"] == "PMC12855588"
    assert body["text"] == "full text"

    names = Enum.map(article["attachments"], & &1["name"])
    assert "PMC12855588.1.pdf" in names
    assert Enum.any?(names, &(Pmc.classify_name(&1) == "image"))
    assert Enum.any?(names, &(Pmc.classify_name(&1) == "supplement"))
  end

  test "selection is deterministic for the same rows" do
    spec = Registry.fetch!("pmc")

    rows =
      for n <- 1..10 do
        %{
          "pmcid" => "PMC#{n}",
          "version" => 1,
          "is_pmc_openaccess" => true,
          "is_retracted" => false,
          "license_code" => "CC BY",
          "text_url" => "https://example.test/#{n}.txt"
        }
      end

    first = Pmc.select_from_metadata(rows, spec, 3)
    second = Pmc.select_from_metadata(Enum.reverse(rows), spec, 3)
    assert Enum.map(first, & &1["pmcid"]) == Enum.map(second, & &1["pmcid"])
  end

  test "OA flag alone does not approve a missing license" do
    spec = Registry.fetch!("pmc")

    rows = [
      %{
        "pmcid" => "PMC1",
        "version" => 1,
        "is_pmc_openaccess" => true,
        "is_retracted" => false,
        "license_code" => nil,
        "text_url" => "https://example.test/1.txt"
      }
    ]

    assert Pmc.select_from_metadata(rows, spec, 1) == []
  end

  test "generate_from_jsonl is deterministic and maps attachments" do
    spec = Registry.fetch!("pmc")
    dir = Path.join(System.tmp_dir!(), "vk-pmc-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "metadata.jsonl")

    File.write!(path, """
    {"pmcid":"PMC1","version":1,"is_pmc_openaccess":true,"is_retracted":false,"license_code":"CC BY","text_url":"https://example.test/1.txt","pdf_url":"https://example.test/1.pdf","media_urls":["https://example.test/1.jpg"]}
    {"pmcid":"PMC2","version":1,"is_pmc_openaccess":true,"is_retracted":false,"license_code":"CC BY","text_url":"https://example.test/2.txt"}
    {"pmcid":"PMC3","version":1,"is_pmc_openaccess":true,"is_retracted":false,"license_code":"CC BY-NC","text_url":"https://example.test/3.txt"}
    """)

    {:ok, first} = Pmc.generate_from_jsonl(path, spec, 2)
    {:ok, second} = Pmc.generate_from_jsonl(path, spec, 2)
    assert Enum.map(first["articles"], & &1["pmcid"]) == Enum.map(second["articles"], & &1["pmcid"])
    assert match?([_, _], first["articles"])

    article = Enum.find(first["articles"], &(&1["pmcid"] == "PMC1")) || hd(first["articles"])
    body = Pmc.document_body(article, "text")
    assert body["pmcid"]
    assert body["text"] == "text"
    assert is_list(article["attachments"])
  end

  test "leftover attachment budget spills into unused objects" do
    spec = %{
      "pdf_budget_bytes" => 0,
      "image_budget_bytes" => 1_000,
      "supplement_budget_bytes" => 0
    }

    image = %{"url" => "img", "name" => "a.jpg", "category" => "image", "expected_size" => 100}
    pdf = %{"url" => "pdf", "name" => "a.pdf", "category" => "pdf", "expected_size" => 800}

    [out] =
      Pmc.assign_attachment_budget(
        [
          %{
            "pmcid" => "PMC1",
            "version" => 1,
            "text" => nil,
            "metadata" => nil,
            "attachments" => [image, pdf],
            "all_attachments" => [image, pdf]
          }
        ],
        spec
      )

    assert Enum.map(out["attachments"], & &1["url"]) == ["img", "pdf"]
  end
end
