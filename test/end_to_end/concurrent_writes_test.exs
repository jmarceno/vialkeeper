defmodule VialKeeper.EndToEnd.ConcurrentWritesTest do
  @moduledoc """
  Many concurrent writers through the writer pool while a follower tails the
  changes feed.

  Writes commit out of order, so the feed must hide a committed row until
  every lower sequence finished. The follower must still see every row of the
  final changes table exactly once and in increasing order, the final winners
  must match a per-document replay of the acknowledged writes, and every
  acknowledged write must already be visible when its call returns.

  On Turso the writer slots commit concurrently and conflict per row; a
  conflict is retried inside the writer pool and must never reach a client
  unless its retries run out.
  """
  use ExUnit.Case, async: false

  alias VialKeeper.Runtime.{DatabaseCatalog, WriterPool}
  alias VialKeeper.Storage.SQLite.Connection
  alias VialKeeper.View.Manager

  @writers 16
  @operations_per_writer 200
  @document_count 300
  @page 100

  setup context do
    previous_limits = Application.get_env(:vial_keeper, :host_limits)
    previous_writers = Application.get_env(:vial_keeper, :sqlite_max_writers)

    limits =
      (previous_limits || [])
      |> Keyword.put(:writer_pool_size, Map.get(context, :pool_size, 4))
      |> Keyword.put(:write_queue_limit, 128)

    Application.put_env(:vial_keeper, :host_limits, limits)
    Application.put_env(:vial_keeper, :sqlite_max_writers, 4)

    relative = "concurrent-writes-#{System.unique_integer([:positive])}.vialkeeper"
    absolute = Path.join(VialKeeper.Config.database_root(), relative)
    VialKeeper.TempDatabase.cleanup(absolute)

    assert {:ok, identity} = DatabaseCatalog.create(relative)
    uuid = identity.database_uuid
    assert {:ok, _} = DatabaseCatalog.open(uuid)
    assert :ok = Manager.await_resumed(uuid)

    on_exit(fn ->
      Application.put_env(:vial_keeper, :host_limits, previous_limits)

      if previous_writers,
        do: Application.put_env(:vial_keeper, :sqlite_max_writers, previous_writers),
        else: Application.delete_env(:vial_keeper, :sqlite_max_writers)

      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      VialKeeper.TempDatabase.cleanup(absolute)
    end)

    {:ok, uuid: uuid, sqlite: VialKeeper.TempDatabase.artifact_path(absolute)}
  end

  @tag timeout: 300_000
  test "a follower sees every committed row once, in order, while writers race", context do
    race(context, @writers, @document_count)
  end

  @tag :turso_engine
  @tag pool_size: 16
  @tag timeout: 300_000
  test "sixteen concurrent Turso writer slots serve 32 clients", context do
    race(context, 32, @document_count)
  end

  @tag :turso_engine
  @tag pool_size: 16
  @tag timeout: 300_000
  test "sixteen Turso writer slots on ten hot documents", context do
    race(context, 16, 10)
  end

  defp race(%{uuid: uuid, sqlite: sqlite}, writers, documents) do
    assert WriterPool.enabled?(uuid)
    conflicts = count_conflicts(uuid)

    follower = Task.async(fn -> follow(uuid, 0, []) end)

    acknowledged =
      1..writers
      |> Enum.map(fn writer -> Task.async(fn -> write_randomly(uuid, writer, documents) end) end)
      |> Enum.flat_map(&Task.await(&1, 240_000))

    send(follower.pid, :writers_done)
    followed = Task.await(follower, 60_000)

    # Retried conflicts are counted; none ran out of retries.
    %{retry: retried, exhausted: exhausted} = Agent.get(conflicts, & &1)
    assert is_integer(retried)
    assert exhausted == 0

    assert acknowledged != []
    followed_sequences = Enum.map(followed, & &1.sequence)
    assert followed_sequences == Enum.sort(Enum.uniq(followed_sequences))
    assert followed_sequences == stored_change_sequences(sqlite)

    acknowledged
    |> Enum.group_by(& &1.id)
    |> Enum.each(fn {id, writes} ->
      last = Enum.max_by(writes, & &1.sequence)
      assert winner(uuid, id) == {last.revision, last.deleted}
    end)
  end

  defp count_conflicts(uuid) do
    {:ok, agent} = Agent.start_link(fn -> %{retry: 0, exhausted: 0} end)
    handler = {__MODULE__, uuid}

    :ok =
      :telemetry.attach(
        handler,
        [:vial_keeper, :writer, :write_conflict],
        &__MODULE__.handle_conflict/4,
        %{uuid: uuid, agent: agent}
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    agent
  end

  @doc false
  def handle_conflict(_event, %{count: count}, %{database_uuid: uuid, outcome: outcome}, %{
        uuid: uuid,
        agent: agent
      }),
      do: Agent.update(agent, &Map.update!(&1, outcome, fn total -> total + count end))

  def handle_conflict(_event, _measurements, _metadata, _config), do: :ok

  defp write_randomly(uuid, writer, documents) do
    :rand.seed(:exsss, {writer, 17, 31})
    Process.put(:document_count, documents)

    Enum.flat_map(1..@operations_per_writer, fn _step ->
      uuid
      |> write_once(:rand.uniform(10))
      |> acknowledge(uuid)
    end)
  end

  defp write_once(uuid, roll) when roll <= 6 do
    id = random_id()
    request = %{document_id: id, body: %{"value" => :rand.uniform(1_000)}}
    {[{id, false}], command(uuid, {:command, :put, with_current(uuid, request)})}
  end

  defp write_once(uuid, roll) when roll <= 8 do
    id = random_id()

    case current(uuid, id) do
      {:live, revision} ->
        request = %{document_id: id, if_revision: revision}
        {[{id, true}], command(uuid, {:command, :delete, request})}

      _deleted_or_missing ->
        {[], :skipped}
    end
  end

  defp write_once(uuid, _roll) do
    ids = Enum.uniq(for _ <- 1..:rand.uniform(5), do: random_id())

    operations =
      Enum.map(ids, fn id ->
        uuid
        |> with_current(%{document_id: id, body: %{"bulk" => :rand.uniform(1_000)}})
        |> Map.put(:operation, :put)
      end)

    {Enum.map(ids, &{&1, false}), command(uuid, {:command, :bulk_write, %{operations: operations}})}
  end

  defp acknowledge({_targets, :skipped}, _uuid), do: []

  defp acknowledge({_targets, {:error, %VialKeeper.Error{code: code}}}, _uuid) do
    refute code == :write_conflict, "a write conflict reached the client"
    []
  end

  defp acknowledge({[{id, deleted}], {:ok, %{revision: revision, sequence: sequence}}}, uuid) do
    assert_visible(uuid, sequence)
    [%{id: id, deleted: deleted, revision: revision, sequence: sequence}]
  end

  defp acknowledge({targets, {:ok, results}}, uuid) when is_list(results) do
    assert length(targets) == length(results)

    Enum.zip_with(targets, results, fn {id, deleted}, result ->
      assert_visible(uuid, result.sequence)
      %{id: id, deleted: deleted, revision: result.revision, sequence: result.sequence}
    end)
  end

  # A successful write response implies the changes feed already includes it.
  defp assert_visible(uuid, sequence) do
    assert {:ok, %{current_sequence: visible}} = command(uuid, {:command, :identity, %{}})
    assert visible >= sequence
  end

  defp with_current(uuid, request) do
    case current(uuid, request.document_id) do
      {_state, revision} -> Map.put(request, :if_revision, revision)
      :missing -> request
    end
  end

  defp current(uuid, id) do
    case command(uuid, {:command, :get_document, %{document_id: id}}) do
      {:ok, %{revision: revision}} ->
        {:live, revision}

      {:error, %VialKeeper.Error{code: :document_not_found, details: %{winning_revision: rev}}} ->
        {:deleted, rev}

      {:error, %VialKeeper.Error{code: :document_not_found}} ->
        :missing
    end
  end

  defp winner(uuid, id) do
    case current(uuid, id) do
      {:live, revision} -> {revision, false}
      {:deleted, revision} -> {revision, true}
    end
  end

  defp follow(uuid, since, seen) do
    {:ok, page} = command(uuid, {:command, :read_changes, %{since: since, limit: @page}})
    rows = page.results
    seen = Enum.reverse(rows, seen)

    cond do
      rows != [] ->
        follow(uuid, page.last_sequence, seen)

      writers_done?() ->
        drain(uuid, since, seen)

      true ->
        follow(uuid, since, seen)
    end
  end

  # After the writers finished, every write is visible; one more empty page
  # proves the follower is caught up.
  defp drain(uuid, since, seen) do
    {:ok, page} = command(uuid, {:command, :read_changes, %{since: since, limit: @page}})

    case page.results do
      [] -> Enum.reverse(seen)
      rows -> drain(uuid, page.last_sequence, Enum.reverse(rows, seen))
    end
  end

  defp writers_done? do
    receive do
      :writers_done -> true
    after
      0 -> false
    end
  end

  defp stored_change_sequences(sqlite) do
    {:ok, conn} =
      Connection.open(sqlite, mode: [:readonly], driver: VialKeeper.TestBackend.driver())

    try do
      {:ok, rows} = Connection.query(conn, "SELECT sequence FROM changes ORDER BY sequence")
      Enum.map(rows, &hd/1)
    after
      Connection.close(conn)
    end
  end

  defp random_id, do: "doc-#{:rand.uniform(Process.get(:document_count, @document_count))}"

  defp command(uuid, command), do: DatabaseCatalog.command(uuid, command, 30_000)
end
