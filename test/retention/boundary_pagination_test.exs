defmodule VialKeeper.Retention.BoundaryPaginationTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Retention.Service

  test "boundary pages cover every record exactly once" do
    boundaries =
      for n <- 1..7 do
        %{
          source_database_uuid: "source",
          boundary: %{document_id: "doc-#{n}", history_id: "history-#{n}"}
        }
      end

    served = collect_pages(boundaries, nil, [])

    assert Enum.map(served, & &1.boundary.document_id) ==
             Enum.map(boundaries, & &1.boundary.document_id)
  end

  defp collect_pages(boundaries, cursor, acc) do
    assert {:ok, page, next} = Service.paginate_boundaries(boundaries, cursor, 3)
    acc = acc ++ page

    case next do
      nil -> acc
      next -> collect_pages(boundaries, next, acc)
    end
  end
end
