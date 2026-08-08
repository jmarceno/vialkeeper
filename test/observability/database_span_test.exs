defmodule ElixirDB.Observability.DatabaseOpenSpanTest do
  @moduledoc """
  Plan §7.2: assert the `elixir_db.database.open` span is emitted with
  `outcome: :ok` on a successful open and `outcome: :rejected` (status UNSET,
  not ERROR) on a rejected open.
  """

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Observability.{OtelCase, TestExporter}
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

    OtelCase.flush()

    spans = TestExporter.spans_named("elixir_db.database.open")
    # Filter to the span for THIS uuid (other DBs may be opened by the catalog).
    span = Enum.find(spans, fn s -> TestExporter.span_attr(s, :"db.uuid") == uuid end)

    assert span != nil,
           "expected an open span for #{uuid}, got uuids: #{inspect(Enum.map(spans, &TestExporter.span_attr(&1, :"db.uuid")))}"

    assert TestExporter.span_attr(span, :"db.uuid") == uuid
    assert TestExporter.span_attr(span, :outcome) == :ok
  end

  test "a rejected open records outcome: :rejected, status UNSET not ERROR" do
    # Open a non-registered uuid to trigger a rejection.
    assert {:error, %ElixirDB.Error{}} = DatabaseCatalog.open(ElixirDB.UUID.v4())

    OtelCase.flush()

    spans = TestExporter.spans_named("elixir_db.database.open")
    rejected = Enum.filter(spans, fn s -> TestExporter.span_attr(s, :outcome) == :rejected end)
    assert [_ | _] = rejected

    span = List.last(rejected)

    # Rejected opens must NOT set span status to ERROR (expected outcome).
    # The status is read from the recorded span view's :status field — not
    # the attributes map, where it never lives.
    assert TestExporter.status_code(span) == :unset,
           "rejected open must keep status UNSET, got #{inspect(span[:status])}"
  end
end
