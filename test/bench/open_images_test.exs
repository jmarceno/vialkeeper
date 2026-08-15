defmodule VialKeeper.Bench.OpenImagesTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.{Checksums, OpenImages, Registry}

  @jpeg <<0xFF, 0xD8, 0xFF, 0xD9>>

  test "canonicalizes base64 OriginalMD5 values" do
    {:ok, hex} = Checksums.canonicalize_md5("0Sad+xMj2ttXM1U8meEJ0A==")
    assert Regex.match?(~r/^[0-9a-f]{32}$/, hex)
  end

  test "parses official image-information columns" do
    header =
      "ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL,Rotation"

    {:ok, headers} = OpenImages.parse_csv_line(header)

    line =
      ~s(000060e3121c7305,train,https://example.test/a.jpg,https://flickr.test/a,https://creativecommons.org/licenses/by/2.0/,https://flickr.test/u,"David","28 Nov 2010 Our new house.",211079,0Sad+xMj2ttXM1U8meEJ0A==,https://example.test/t.jpg,0)

    assert {:ok, image} = OpenImages.parse_info_row(line, headers)
    assert image["image_id"] == "000060e3121c7305"
    assert image["url"] == "https://example.test/a.jpg"
    assert image["expected_size"] == 211_079
    assert image["md5"]
  end

  test "selection ranking is stable" do
    spec = Registry.fetch!("open-images")
    ids = ["b", "a", "c", "d"]
    first = OpenImages.select_ids(ids, spec["selection_seed"], 2)
    second = OpenImages.select_ids(Enum.reverse(ids), spec["selection_seed"], 2)
    assert first == second
  end

  test "maps a row into a VialKeeper document" do
    image = %{
      "image_id" => "abc",
      "split" => "train",
      "url" => "https://example.test/a.jpg",
      "title" => "House",
      "labels" => ["Building"]
    }

    body = OpenImages.document_body(image)
    assert body["image_id"] == "abc"
    assert body["labels"] == ["Building"]
    assert body["source_metadata"]["url"] == "https://example.test/a.jpg"
  end

  test "smoke manifest records pinned URL, size, and MD5" do
    spec = Registry.fetch!("open-images")

    manifest =
      OpenImages.smoke_manifest(spec,
        images: [
          %{
            "image_id" => "tiny",
            "url" => "https://example.test/tiny.jpg",
            "expected_size" => byte_size(@jpeg),
            "md5" => Checksums.md5_iodata(@jpeg)
          }
        ]
      )

    [image] = manifest["images"]
    assert image["url"] == "https://example.test/tiny.jpg"
    assert image["expected_size"] == byte_size(@jpeg)
    assert image["md5"] == Checksums.md5_iodata(@jpeg)
  end

  test "generate_from_info_csv selects a stable subset" do
    spec = Registry.fetch!("open-images")
    dir = Path.join(System.tmp_dir!(), "vk-oi-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    path = Path.join(dir, "info.csv")
    md5 = Checksums.md5_iodata(@jpeg)

    File.write!(path, """
    ImageID,Subset,OriginalURL,OriginalLandingURL,License,AuthorProfileURL,Author,Title,OriginalSize,OriginalMD5,Thumbnail300KURL,Rotation
    zzz,train,https://example.test/z.jpg,https://example.test/z,https://creativecommons.org/licenses/by/2.0/,https://example.test/u,Z,Z,4,#{md5},,0
    aaa,train,https://example.test/a.jpg,https://example.test/a,https://creativecommons.org/licenses/by/2.0/,https://example.test/u,A,A,4,#{md5},,0
    mmm,train,https://example.test/m.jpg,https://example.test/m,https://creativecommons.org/licenses/by/2.0/,https://example.test/u,M,M,4,#{md5},,0
    """)

    {:ok, first} = OpenImages.generate_from_info_csv(path, spec, 2)
    {:ok, second} = OpenImages.generate_from_info_csv(path, spec, 2)

    assert Enum.map(first["images"], & &1["image_id"]) ==
             Enum.map(second["images"], & &1["image_id"])

    assert match?([_, _], first["images"])
  end
end
