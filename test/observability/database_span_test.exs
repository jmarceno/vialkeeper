defmodule ElixirDB.Observability.DatabaseOpenSpanTest do
  @moduledoc """
  Assert the `elixir_db.database.open` span is emitted with
  `outcome: :ok` on a successful open and `outcome: :rejected` (status UNSET,
  not ERROR) on a rejected open.
  """

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Eventual
  alias ElixirDB.Observability.TestExporter
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    # The catalog creates the file under database_root; clean up THERE (a
    # leftover from a previous run would fail create with "database file
    # already exists").
    rel = "obs-open-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    [uuid: uuid, rel: rel]
  end

  test "open emits a span with outcome: :ok", %{uuid: uuid} do
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("elixir_db.database.open")
          |> Enum.find(fn s -> TestExporter.span_attr(s, :"db.uuid") == uuid end)
        end,
        message: "expected an open span for #{uuid}"
      )

    assert TestExporter.span_attr(span, :"db.uuid") == uuid
    assert TestExporter.span_attr(span, :outcome) == :ok
  end

  test "a rejected open records outcome: :rejected, status UNSET not ERROR" do
    # Open a non-registered uuid to trigger a rejection.
    assert {:error, %ElixirDB.Error{}} = DatabaseCatalog.open(ElixirDB.UUID.v4())

    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("elixir_db.database.open")
          |> Enum.find(fn s -> TestExporter.span_attr(s, :outcome) == :rejected end)
        end,
        message: "expected a rejected open span"
      )

    # Rejected opens must NOT set span status to ERROR (expected outcome).
    # The status is read from the recorded span view's :status field — not
    # the attributes map, where it never lives.
    assert TestExporter.status_code(span) == :unset,
           "rejected open must keep status UNSET, got #{inspect(span[:status])}"
  end
end
