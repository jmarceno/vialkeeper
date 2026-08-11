defmodule ElixirDB.Federation.ExecutorTest do
  use ExUnit.Case, async: true

  alias ElixirDB.Error
  alias ElixirDB.Federation.Executor

  @source "123e4567-e89b-12d3-a456-426614174000"
  @other_source "123e4567-e89b-12d3-a456-426614174001"
  @third_source "123e4567-e89b-12d3-a456-426614174002"

  test "merges source pages by sort values, source UUID, and document ID" do
    parent = self()

    fetcher = fn source_uuid, request, _deadline ->
      send(parent, {:fetched, source_uuid, request})

      documents =
        case source_uuid do
          @source -> [document("a", 1), document("z", 2)]
          @other_source -> [document("b", 1), document("a", 2)]
        end

      {:ok, page(documents, 7)}
    end

    request = %{
      databases: [@source, @other_source],
      query: %{selector: %{}, sort: [%{path: "/score", direction: "asc"}], limit: 4}
    }

    assert {:ok, result} = Executor.run(request, source_fetcher: fetcher, max_candidates: 8)

    assert_receive {:fetched, @source, source_request}, 1_000
    refute Map.has_key?(source_request, :fields)
    assert_receive {:fetched, @other_source, other_request}, 1_000
    refute Map.has_key?(other_request, :fields)

    assert Enum.map(result.documents, & &1.id) == ["a", "b", "z", "a"]

    assert Enum.map(result.documents, & &1.source_database_uuid) == [
             @source,
             @other_source,
             @source,
             @other_source
           ]

    assert result.bookmark == nil
  end

  test "continuation refetches source pages, preserves the query, and advances each source" do
    parent = self()

    fetcher = fn source_uuid, request, _deadline ->
      send(parent, {:page_request, source_uuid, request})

      if is_nil(request.bookmark) do
        {:ok,
         page(
           [
             document(
               if(source_uuid == @source, do: "a", else: "b"),
               if(source_uuid == @source, do: 1, else: 2)
             )
           ],
           9,
           true,
           "next-" <> source_uuid
         )}
      else
        {:ok,
         page(
           [
             document(
               if(source_uuid == @source, do: "c", else: "d"),
               if(source_uuid == @source, do: 3, else: 4)
             )
           ],
           9
         )}
      end
    end

    request = %{
      databases: [@source, @other_source],
      query: %{
        selector: %{"/type" => "note"},
        sort: [%{path: "/score", direction: "asc"}],
        fields: ["/score"],
        limit: 2
      }
    }

    assert {:ok, first} = Executor.run(request, source_fetcher: fetcher, max_candidates: 4)
    assert is_binary(first.bookmark)

    first_requests =
      for _ <- 1..3 do
        assert_receive {:page_request, source_uuid, source_request}, 1_000
        {source_uuid, source_request}
      end

    assert Enum.sort(Enum.map(first_requests, &elem(&1, 0))) ==
             Enum.sort([@source, @other_source, @source])

    assert Enum.sort(Enum.map(first_requests, fn {_source_uuid, request} -> request.bookmark end)) ==
             Enum.sort([nil, nil, "next-" <> @source])

    {_, first_request} = Enum.find(first_requests, fn {_, request} -> is_nil(request.bookmark) end)

    assert first_request.selector == %{"/type" => "note"}
    assert first_request.sort == [%{path: "/score", direction: "asc"}]
    assert first_request.limit == 2
    assert first_request.bookmark == nil
    refute Map.has_key?(first_request, :fields)

    assert {:ok, second} =
             Executor.run(
               put_in(request, [:query, :bookmark], first.bookmark),
               source_fetcher: fetcher,
               max_candidates: 4
             )

    assert Enum.map(second.documents, & &1.id) == ["c", "d"]
    assert Enum.all?(second.documents, &Map.has_key?(&1, :fields))
    assert second.bookmark == nil

    second_requests =
      for _ <- 1..2 do
        assert_receive {:page_request, source_uuid, source_request}, 1_000
        {source_uuid, source_request}
      end

    assert Enum.all?(second_requests, fn {_source_uuid, source_request} ->
             source_request.bookmark == nil
           end)

    continuation_requests =
      for _ <- 1..2 do
        assert_receive {:page_request, source_uuid, source_request}, 1_000
        {source_uuid, source_request}
      end

    assert Enum.sort(Enum.map(continuation_requests, &elem(&1, 0))) ==
             Enum.sort([@source, @other_source])

    assert Enum.sort(
             Enum.map(continuation_requests, fn {_, source_request} -> source_request.bookmark end)
           ) ==
             Enum.sort(["next-" <> @source, "next-" <> @other_source])
  end

  test "rejects a continuation when a source sequence changed" do
    parent = self()

    fetcher = fn source_uuid, request, _deadline ->
      send(parent, {:sequence_request, source_uuid, self(), request.bookmark})

      receive do
        {:use_sequence, sequence} ->
          {:ok, page([document("doc-" <> String.last(source_uuid), 1)], sequence, true, "next")}
      end
    end

    request = %{databases: [@source, @other_source], query: %{limit: 1}}

    first_task =
      Task.async(fn -> Executor.run(request, source_fetcher: fetcher, max_candidates: 4) end)

    first_sources =
      for _ <- 1..2 do
        assert_receive {:sequence_request, source_uuid, pid, nil}, 1_000
        {source_uuid, pid}
      end

    Enum.each(first_sources, fn {_source_uuid, pid} -> send(pid, {:use_sequence, 5}) end)
    assert {:ok, first} = Task.await(first_task, 2_000)

    second_task =
      Task.async(fn ->
        Executor.run(
          put_in(request, [:query, :bookmark], first.bookmark),
          source_fetcher: fetcher,
          max_candidates: 4
        )
      end)

    second_sources =
      for _ <- 1..2 do
        assert_receive {:sequence_request, source_uuid, pid, nil}, 1_000
        {source_uuid, pid}
      end

    Enum.each(second_sources, fn {_source_uuid, pid} -> send(pid, {:use_sequence, 6}) end)

    assert {:error, %Error{code: :bookmark_stale}} =
             Task.await(second_task, 2_000)
  end

  test "limits concurrent source pages with explicit release barriers" do
    parent = self()

    fetcher = fn source_uuid, _request, _deadline ->
      send(parent, {:started, source_uuid, self()})

      receive do
        {:release, ^source_uuid} -> {:ok, page([document(source_uuid, 1)], 1)}
      end
    end

    task =
      Task.async(fn ->
        Executor.run(
          %{databases: [@source, @other_source, @third_source], query: %{limit: 3}},
          source_fetcher: fetcher,
          max_candidates: 3,
          max_concurrent_sources: 2
        )
      end)

    started =
      for _ <- 1..2 do
        assert_receive {:started, source_uuid, pid}, 1_000
        {source_uuid, pid}
      end

    assert Enum.sort(Enum.map(started, &elem(&1, 0))) == Enum.sort([@source, @other_source])

    Enum.each(started, fn {source_uuid, pid} -> send(pid, {:release, source_uuid}) end)
    assert_receive {:started, @third_source, third_pid}, 1_000
    send(third_pid, {:release, @third_source})

    assert {:ok, %{documents: [_, _, _]}} = Task.await(task, 2_000)
  end

  test "cancels remaining source tasks and preserves the first typed source error" do
    parent = self()

    fetcher = fn
      @source, _request, _deadline ->
        send(parent, {:failing_source, self()})

        receive do
          :fail -> {:error, Error.database_unavailable("source is unavailable")}
        end

      @other_source, _request, _deadline ->
        send(parent, {:blocked_source, self()})

        receive do
          :never -> {:ok, page([document("never", 1)], 1)}
        end
    end

    task =
      Task.async(fn ->
        Executor.run(
          %{databases: [@source, @other_source], query: %{limit: 1}},
          source_fetcher: fetcher,
          max_candidates: 2
        )
      end)

    assert_receive {:failing_source, failing_pid}, 1_000
    assert_receive {:blocked_source, blocked_pid}, 1_000
    blocked_monitor = Process.monitor(blocked_pid)
    send(failing_pid, :fail)

    assert {:error, %Error{code: :database_unavailable}} = Task.await(task, 2_000)
    assert_receive {:DOWN, ^blocked_monitor, :process, ^blocked_pid, _reason}, 1_000
  end

  test "returns a resource limit when another source page would exceed the candidate budget" do
    parent = self()

    fetcher = fn source_uuid, request, _deadline ->
      send(parent, {:budget_page, source_uuid, request})
      {:ok, page([document(source_uuid, 1)], 3, true, "next-" <> source_uuid)}
    end

    assert {:error, %Error{code: :resource_limit}} =
             Executor.run(
               %{databases: [@source, @other_source], query: %{limit: 3}},
               source_fetcher: fetcher,
               max_candidates: 2
             )

    assert_receive {:budget_page, @source, %{limit: 1}}, 1_000
    assert_receive {:budget_page, @other_source, %{limit: 1}}, 1_000
  end

  defp page(documents, sequence, has_more \\ false, bookmark \\ nil) do
    %{documents: documents, sequence: sequence, has_more: has_more, bookmark: bookmark}
  end

  defp document(id, score), do: %{id: id, revision: "revision-" <> id, body: %{"score" => score}}
end
