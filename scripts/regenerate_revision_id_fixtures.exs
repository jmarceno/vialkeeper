# Regenerates priv/fixtures/revision_ids/vectors.json for attachment-aware revision hashing.

alias VialKeeper.JSON.Canonical
alias VialKeeper.Revisions.Id

fixtures_path = Path.expand("priv/fixtures/revision_ids/vectors.json", File.cwd!())
fixtures = fixtures_path |> File.read!() |> JSON.decode!()

updated =
  Enum.map(fixtures, fn fixture ->
    case fixture do
      %{"expect_error" => _} ->
        fixture

      %{"history_id" => history_id} = fixture ->
        attachments = Map.get(fixture, "attachments", %{})

        {:ok, revision_id} =
          Id.calculate(
            fixture["document_id"],
            history_id,
            fixture["parent_revision"],
            fixture["deleted"],
            fixture["body"],
            attachments
          )

        {:ok, canonical} =
          Canonical.encode(%{
            "version" => 1,
            "document_id" => fixture["document_id"],
            "history_id" => history_id,
            "parent_revision" => fixture["parent_revision"],
            "deleted" => fixture["deleted"],
            "body" => fixture["body"],
            "attachments" => attachments
          })

        fixture
        |> Map.put("expected_revision_id", revision_id)
        |> Map.put("canonical_payload", canonical)
        |> Map.put("attachments", attachments)
    end
  end)

File.write!(fixtures_path, JSON.encode!(updated) <> "\n")
IO.puts("Wrote #{byte_size(JSON.encode!(updated))} bytes to #{fixtures_path}")
