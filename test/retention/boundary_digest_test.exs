defmodule ElixirDB.Retention.BoundaryDigestTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Domain.{BoundaryPage, RetentionBoundary}
  alias ElixirDB.JSON.Canonical

  test "digest encodes sorted retired branch roots" do
    boundary =
      boundary!(%{
        document_id: "doc-1",
        history_id: "hist-1",
        minimum_retained_generation: 2,
        retired: false,
        retired_branch_roots: ["root-z", "root-a"]
      })

    digest = BoundaryPage.digest_for([boundary])

    expected_payload = [
      %{
        "document_id" => "doc-1",
        "history_id" => "hist-1",
        "minimum_retained_generation" => 2,
        "retired" => false,
        "retired_branch_roots" => ["root-a", "root-z"]
      }
    ]

    canonical = Canonical.encode!(expected_payload)

    assert digest ==
             :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)
  end

  test "retired boundary encodes null minimum generation" do
    boundary =
      boundary!(%{
        document_id: "doc-2",
        history_id: "hist-2",
        minimum_retained_generation: nil,
        retired: true,
        retired_branch_roots: []
      })

    digest = BoundaryPage.digest_for([boundary])

    expected_payload = [
      %{
        "document_id" => "doc-2",
        "history_id" => "hist-2",
        "minimum_retained_generation" => nil,
        "retired" => true,
        "retired_branch_roots" => []
      }
    ]

    canonical = Canonical.encode!(expected_payload)

    assert digest ==
             :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)
  end

  defp boundary!(attrs) do
    {:ok, boundary} = RetentionBoundary.new(attrs)
    boundary
  end
end
