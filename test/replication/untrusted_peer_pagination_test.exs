defmodule VialKeeper.Replication.UntrustedPeerPaginationTest do
  @moduledoc """
  A misbehaving source must fail replication instead of looping forever:
  repeated pagination cursors and change batches that cannot advance the
  checkpoint are rejected.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias VialKeeper.Replication
  alias VialKeeper.Replication.LocalEndpoint
  alias VialKeeper.Runtime.DatabaseCatalog
  alias VialKeeper.TempDatabase

  defmodule RogueEndpoint do
    @moduledoc false
    alias VialKeeper.Replication.Endpoint

    # Delegates every Endpoint callback to LocalEndpoint, except the one named
    # in `rogue`, which returns a misbehaving page.
    defstruct [:inner, :rogue, :database_uuid]

    overridden = [read_boundary_pages: 2, read_changes: 2]

    for {name, arity} <- Endpoint.behaviour_info(:callbacks),
        {name, arity} not in overridden do
      args = Macro.generate_arguments(arity - 1, __MODULE__)

      def unquote(name)(%__MODULE__{inner: inner}, unquote_splicing(args)),
        do: LocalEndpoint.unquote(name)(inner, unquote_splicing(args))
    end

    # Serves the real first page but always points at the same next cursor.
    def read_boundary_pages(%__MODULE__{rogue: :boundary_cursor, inner: inner}, request) do
      with {:ok, page} <- LocalEndpoint.read_boundary_pages(inner, %{request | cursor: nil}) do
        {:ok, put_next_page(page, request.cursor || "stuck")}
      end
    end

    def read_boundary_pages(%__MODULE__{inner: inner}, request),
      do: LocalEndpoint.read_boundary_pages(inner, request)

    defp put_next_page(page, cursor) when is_struct(page), do: %{page | next_page: cursor}
    defp put_next_page(page, cursor), do: Map.put(page, "next_page", cursor)

    def read_changes(%__MODULE__{rogue: :stale_changes}, _request) do
      {:ok, %{results: [%{sequence: 0, document_id: "stale", leaf_revisions: []}]}}
    end

    def read_changes(%__MODULE__{inner: inner}, request),
      do: LocalEndpoint.read_changes(inner, request)
  end

  setup do
    prefix = "rogue-peer-#{System.unique_integer([:positive])}"
    root = VialKeeper.Config.database_root()
    paths = [prefix <> "-a.vialkeeper", prefix <> "-b.vialkeeper"]
    Enum.each(paths, &TempDatabase.cleanup(Path.join(root, &1)))

    [{:ok, a}, {:ok, b}] = Enum.map(paths, &DatabaseCatalog.create/1)

    on_exit(fn ->
      for {identity, path} <- Enum.zip([a, b], paths) do
        _ = DatabaseCatalog.close(identity.database_uuid)
        _ = DatabaseCatalog.unregister(identity.database_uuid)
        TempDatabase.cleanup(Path.join(root, path))
      end
    end)

    {:ok, source} = LocalEndpoint.new(a.database_uuid)
    {:ok, target} = LocalEndpoint.new(b.database_uuid)
    {:ok, a: a, source: source, target: target}
  end

  defp rogue(source, mode),
    do: %RogueEndpoint{inner: source, rogue: mode, database_uuid: source.database_uuid}

  test "a repeated boundary page cursor fails instead of looping", %{
    source: source,
    target: target
  } do
    task =
      Task.async(fn ->
        Replication.one_shot_endpoints(rogue(source, :boundary_cursor), target, %{})
      end)

    assert {:error, %VialKeeper.Error{}} = Task.await(task, 10_000)
  end

  test "changes that cannot advance the checkpoint fail instead of looping", %{
    a: a,
    source: source,
    target: target
  } do
    assert {:ok, _} = Replication.one_shot_endpoints(source, target, %{})
    assert {:ok, _} = VialKeeper.Documents.put(a.database_uuid, %{id: "next", body: %{}})

    task =
      Task.async(fn ->
        Replication.one_shot_endpoints(rogue(source, :stale_changes), target, %{})
      end)

    assert {:error, %VialKeeper.Error{}} = Task.await(task, 10_000)
  end
end
