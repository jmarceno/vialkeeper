defmodule ElixirDB.Federation.OrderingContractTest do
  @moduledoc "Covers stable federation ordering across source completion order."

  use ExUnit.Case, async: true

  alias ElixirDB.Federation.Executor

  @source "123e4567-e89b-12d3-a456-426614174000"
  @other_source "123e4567-e89b-12d3-a456-426614174001"
  @third_source "123e4567-e89b-12d3-a456-426614174002"

  test "global tie-breaking is independent of source completion order" do
    parent = self()

    fetcher = fn source_uuid, _request, _deadline ->
      send(parent, {:started, source_uuid, self()})

      receive do
        {:release, ^source_uuid} ->
          {:ok, page([document("same", 1)], 1)}
      end
    end

    task =
      Task.async(fn ->
        Executor.run(
          %{
            databases: [@third_source, @other_source, @source],
            query: %{sort: [%{path: "/score", direction: "asc"}], limit: 3}
          },
          source_fetcher: fetcher,
          max_candidates: 3,
          max_concurrent_sources: 3
        )
      end)

    started =
      for _ <- 1..3 do
        assert_receive {:started, source_uuid, pid}, 1_000
        {source_uuid, pid}
      end

    Enum.each([@source, @other_source, @third_source], fn source_uuid ->
      {^source_uuid, pid} = Enum.find(started, fn {uuid, _pid} -> uuid == source_uuid end)
      send(pid, {:release, source_uuid})
    end)

    assert {:ok, result} = Task.await(task, 2_000)

    assert Enum.map(result.documents, & &1.source_database_uuid) == [
             @source,
             @other_source,
             @third_source
           ]
  end

  test "without a sort, global order is source UUID then document ID" do
    fetcher = fn source_uuid, _request, _deadline ->
      documents =
        case source_uuid do
          @source -> [document("a", 1), document("z", 1)]
          @other_source -> [document("b", 1)]
          @third_source -> [document("c", 1)]
        end

      {:ok, page(documents, 1)}
    end

    assert {:ok, result} =
             Executor.run(
               %{databases: [@third_source, @other_source, @source], query: %{limit: 4}},
               source_fetcher: fetcher,
               max_candidates: 6
             )

    assert Enum.map(result.documents, &{&1.source_database_uuid, &1.id}) == [
             {@source, "a"},
             {@source, "z"},
             {@other_source, "b"},
             {@third_source, "c"}
           ]
  end

  test "fetches an exhausted page buffer before choosing another source head" do
    parent = self()

    fetcher = fn source_uuid, request, _deadline ->
      send(parent, {:page_request, source_uuid, request.bookmark})

      case {source_uuid, request.bookmark} do
        {@source, nil} ->
          {:ok, page([document("a-1", 1)], 1, true, "source-next")}

        {@source, "source-next"} ->
          {:ok, page([document("a-2", 2)], 1)}

        {@other_source, nil} ->
          {:ok, page([document("b-1", 3)], 1)}
      end
    end

    assert {:ok, result} =
             Executor.run(
               %{
                 databases: [@source, @other_source],
                 query: %{sort: [%{path: "/score", direction: "asc"}], limit: 2}
               },
               source_fetcher: fetcher,
               max_candidates: 4
             )

    assert Enum.map(result.documents, & &1.id) == ["a-1", "a-2"]

    initial_requests =
      for _ <- 1..2 do
        assert_receive {:page_request, source_uuid, nil}
        source_uuid
      end

    assert Enum.sort(initial_requests) == Enum.sort([@source, @other_source])
    assert_receive {:page_request, @source, "source-next"}
  end

  defp page(documents, sequence), do: %{documents: documents, sequence: sequence, has_more: false}

  defp page(documents, sequence, has_more, bookmark),
    do: %{documents: documents, sequence: sequence, has_more: has_more, bookmark: bookmark}

  defp document(id, score), do: %{id: id, revision: "revision-" <> id, body: %{"score" => score}}
end
