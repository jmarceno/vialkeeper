defmodule ElixirDB.Contract.AttachmentManifestTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Attachments.Manifest
  alias ElixirDB.Domain.Revision
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Storage.SQLite.Revisions

  @digest String.duplicate("a", 64)
  @other_digest String.duplicate("b", 64)

  test "accepts unicode and filesystem-special attachment names" do
    assert {:ok, manifest} =
             Manifest.normalize(%{
               "café/文档.svg" => %{
                 "digest" => @digest,
                 "length" => 10,
                 "content_type" => "image/svg+xml"
               }
             })

    assert Map.has_key?(manifest, "café/文档.svg")
  end

  test "rejects NUL and control characters in attachment names" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Manifest.normalize(%{
               "bad\0name" => %{
                 "digest" => @digest,
                 "length" => 1,
                 "content_type" => "text/plain"
               }
             })
  end

  test "rejects invalid digests and client-authoritative lengths" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Manifest.normalize_references(%{
               "file.txt" => %{"blob" => "NOT-A-DIGEST", "content_type" => "text/plain"}
             })

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Manifest.normalize_references(%{
               "file.txt" => %{"blob" => @digest, "content_type" => "text/plain", "length" => 1}
             })
  end

  test "canonical ordering is independent of input map order" do
    left = %{
      "z-last" => %{"digest" => @digest, "length" => 1, "content_type" => "text/plain"},
      "a-first" => %{"digest" => @other_digest, "length" => 2, "content_type" => "text/plain"}
    }

    right = %{
      "a-first" => %{"digest" => @other_digest, "length" => 2, "content_type" => "text/plain"},
      "z-last" => %{"digest" => @digest, "length" => 1, "content_type" => "text/plain"}
    }

    assert {:ok, left_canonical} = Manifest.canonical_for_hash(left)
    assert {:ok, right_canonical} = Manifest.canonical_for_hash(right)
    assert left_canonical == right_canonical
  end

  test "revision id changes when attachment metadata changes" do
    history_id = "11111111-1111-4111-8111-111111111111"

    base = %{
      document_id: "doc",
      history_id: history_id,
      parent_revision: nil,
      deleted: false,
      body: %{"x" => 1}
    }

    {:ok, empty_id} = Id.calculate(Map.put(base, :attachments, %{}))

    {:ok, named_id} =
      Id.calculate(
        Map.put(base, :attachments, %{
          "diagram.svg" => %{
            digest: @digest,
            length: 10,
            content_type: "image/svg+xml"
          }
        })
      )

    {:ok, renamed_id} =
      Id.calculate(
        Map.put(base, :attachments, %{
          "renamed.svg" => %{
            digest: @digest,
            length: 10,
            content_type: "image/svg+xml"
          }
        })
      )

    {:ok, length_id} =
      Id.calculate(
        Map.put(base, :attachments, %{
          "diagram.svg" => %{
            digest: @digest,
            length: 11,
            content_type: "image/svg+xml"
          }
        })
      )

    {:ok, type_id} =
      Id.calculate(
        Map.put(base, :attachments, %{
          "diagram.svg" => %{
            digest: @digest,
            length: 10,
            content_type: "application/octet-stream"
          }
        })
      )

    assert empty_id != named_id
    assert named_id != renamed_id
    assert named_id != length_id
    assert named_id != type_id
  end

  test "revision id changes when attachment digest changes" do
    history_id = "11111111-1111-4111-8111-111111111111"

    base = %{
      document_id: "doc",
      history_id: history_id,
      parent_revision: nil,
      deleted: false,
      body: %{"x" => 1}
    }

    attachment = fn digest ->
      %{
        "diagram.svg" => %{
          digest: digest,
          length: 10,
          content_type: "image/svg+xml"
        }
      }
    end

    {:ok, first} = Id.calculate(Map.put(base, :attachments, attachment.(@digest)))
    {:ok, second} = Id.calculate(Map.put(base, :attachments, attachment.(@other_digest)))

    assert first != second
  end

  test "resolve_inheritance: create omitted => empty manifest" do
    assert {:ok, %{}} = Manifest.resolve_inheritance(:create, :omitted, nil)
    assert {:ok, %{}} = Manifest.resolve_inheritance(:create, :omitted, %{"keep" => parent_entry()})
  end

  test "resolve_inheritance: update omitted => inherit parent manifest" do
    parent = %{
      "diagram.svg" => parent_entry()
    }

    assert {:ok, inherited} = Manifest.resolve_inheritance(:update, :omitted, parent)
    assert inherited == parent

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Manifest.resolve_inheritance(:update, :omitted, nil)
  end

  test "resolve_inheritance: explicit empty map clears manifest" do
    parent = %{"diagram.svg" => parent_entry()}

    assert {:ok, %{}} = Manifest.resolve_inheritance(:update, %{}, parent)
    assert {:ok, %{}} = Manifest.resolve_inheritance(:create, %{}, nil)
  end

  test "resolve_inheritance: supplied map is complete replacement" do
    parent = %{
      "old.svg" => parent_entry()
    }

    replacement = %{
      "new.svg" => %{
        "digest" => @other_digest,
        "length" => 5,
        "content_type" => "image/png"
      }
    }

    assert {:ok, replaced} = Manifest.resolve_inheritance(:update, replacement, parent)
    assert Map.keys(replaced) == ["new.svg"]
    refute Map.has_key?(replaced, "old.svg")
  end

  test "resolve_inheritance: conflict resolution omitted => inherit chosen parent" do
    chosen_parent = %{
      "diagram.svg" => parent_entry()
    }

    assert {:ok, inherited} =
             Manifest.resolve_inheritance(:resolve_conflict, :omitted, chosen_parent)

    assert inherited == chosen_parent

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             Manifest.resolve_inheritance(:resolve_conflict, :omitted, nil)
  end

  test "Revisions.same?/2 compares attachment manifests" do
    base = %Revision{
      document_id: "doc",
      history_id: "hist",
      revision_id: "1-abc",
      generation: 1,
      parent_revision: nil,
      digest: "d",
      deleted: false,
      body: %{"x" => 1},
      attachments: %{
        "file.txt" => %{
          digest: @digest,
          length: 4,
          content_type: "text/plain"
        }
      }
    }

    identical = %{base | attachments: base.attachments}

    different_digest = %{
      base
      | attachments: %{
          "file.txt" => %{
            digest: @other_digest,
            length: 4,
            content_type: "text/plain"
          }
        }
    }

    different_name =
      %{
        base
        | attachments: %{
            "renamed.txt" => Map.fetch!(base.attachments, "file.txt")
          }
      }

    assert Revisions.same?(base, identical)
    refute Revisions.same?(base, different_digest)
    refute Revisions.same?(base, different_name)
  end

  test "empty manifests hash for ordinary revisions and tombstones" do
    history_id = "11111111-1111-4111-8111-111111111111"

    {:ok, live_id} =
      Id.calculate("doc", history_id, nil, false, %{"ok" => true}, %{})

    {:ok, tombstone_id} =
      Id.calculate("doc", history_id, live_id, true, nil, %{})

    {:ok, live_payload} =
      Canonical.encode(%{
        "version" => 1,
        "document_id" => "doc",
        "history_id" => history_id,
        "parent_revision" => nil,
        "deleted" => false,
        "body" => %{"ok" => true},
        "attachments" => %{}
      })

    {:ok, tombstone_payload} =
      Canonical.encode(%{
        "version" => 1,
        "document_id" => "doc",
        "history_id" => history_id,
        "parent_revision" => live_id,
        "deleted" => true,
        "body" => nil,
        "attachments" => %{}
      })

    assert live_payload =~ ~s("attachments":{})
    assert tombstone_payload =~ ~s("attachments":{})
    assert tombstone_id != live_id
  end

  test "tombstones reject non-empty attachment manifests" do
    attrs = %{
      document_id: "doc",
      history_id: "11111111-1111-4111-8111-111111111111",
      revision_id: "1-abc",
      generation: 1,
      parent_revision: nil,
      digest: "d",
      deleted: true,
      body: nil,
      attachments: %{
        "file.txt" => %{
          digest: @digest,
          length: 4,
          content_type: "text/plain"
        }
      }
    }

    assert {:error, %ElixirDB.Error{code: :invalid_request}} = Revision.new(attrs)
  end

  test "physical codec fields are not part of revision hash payload" do
    history_id = "11111111-1111-4111-8111-111111111111"

    attachments = %{
      "file.bin" => %{
        digest: @digest,
        length: 4,
        content_type: "application/octet-stream"
      }
    }

    {:ok, _} = Id.calculate("doc", history_id, nil, false, %{"x" => 1}, attachments)

    {:ok, canonical} = Manifest.canonical_for_hash(attachments)
    refute Map.has_key?(canonical["file.bin"], "codec")
    refute Map.has_key?(canonical["file.bin"], "compressed")
  end

  defp parent_entry do
    %{
      digest: @digest,
      length: 10,
      content_type: "image/svg+xml"
    }
  end
end
