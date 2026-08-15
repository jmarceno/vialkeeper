defmodule VialKeeper.Bench.AttachmentStreamTest do
  use ExUnit.Case, async: false

  alias VialKeeper.Attachments
  alias VialKeeper.Bench.{IO, OpenImages, Pmc}
  alias VialKeeper.Documents
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.View.Manager

  @jpeg <<0xFF, 0xD8, 0xFF, 0xD9>>

  setup do
    relative = "bench-attach-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert :ok = Manager.await_resumed(uuid)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid}
  end

  test "PMC and Open Images adapters stream attachments through upload_stream", %{uuid: uuid} do
    dir = Path.join(System.tmp_dir!(), "vk-attach-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    pdf_path = Path.join(dir, "paper.pdf")
    jpeg_path = Path.join(dir, "image.jpg")
    File.write!(pdf_path, "%PDF-1.4 fixture")
    File.write!(jpeg_path, @jpeg)

    article = %{
      "pmcid" => "PMC1",
      "version" => 1,
      "license_code" => "CC BY",
      "title" => "Fixture",
      "text" => nil
    }

    {:ok, %{blob: pdf_digest}} = Attachments.upload_stream(uuid, IO.file_chunks(pdf_path))
    {:ok, %{blob: jpeg_digest}} = Attachments.upload_stream(uuid, IO.file_chunks(jpeg_path))

    pmc_body = Pmc.document_body(article, "full text")

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "PMC1",
               "body" => pmc_body,
               "attachments" => %{
                 "paper.pdf" => %{"blob" => pdf_digest, "content_type" => "application/pdf"},
                 "figure.jpg" => %{"blob" => jpeg_digest, "content_type" => "image/jpeg"}
               }
             })

    image = %{
      "image_id" => "img-1",
      "split" => "train",
      "url" => "https://example.test/img-1.jpg",
      "title" => "House",
      "labels" => ["Building"]
    }

    assert {:ok, _} =
             Documents.put(uuid, %{
               "id" => "img-1",
               "body" => OpenImages.document_body(image),
               "attachments" => %{
                 "image.jpg" => %{"blob" => jpeg_digest, "content_type" => "image/jpeg"}
               }
             })

    assert {:ok, stream} =
             Attachments.open_stream(uuid, %{
               "id" => "PMC1",
               "revision" => nil,
               "name" => "paper.pdf"
             })

    assert IO.consume_stream(stream.body) == byte_size("%PDF-1.4 fixture")
  end
end
