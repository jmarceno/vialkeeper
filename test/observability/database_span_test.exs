defmodule VialKeeper.Observability.DatabaseOpenSpanTest do
  @moduledoc """
  Assert the `vial_keeper.database.open` span is emitted with
  `outcome: :ok` on a successful open and `outcome: :rejected` (status UNSET,
  not ERROR) on a rejected open.
  """

  use VialKeeper.Observability.OtelCase, async: false

  @moduletag :integration

  alias VialKeeper.Eventual
  alias VialKeeper.Observability.TestExporter
  alias VialKeeper.Runtime.DatabaseCatalog

  setup do
    # The catalog creates the file under database_root; clean up THERE (a
    # leftover from a previous run would fail create with "database file
    # already exists").
    rel = "obs-open-#{System.unique_integer([:positive])}.vialkeeper"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = VialKeeper.Config.database_root()
      VialKeeper.TempDatabase.cleanup(Path.join(root, rel))
    end)

    [uuid: uuid, rel: rel]
  end

  test "open emits a span with outcome: :ok", %{uuid: uuid} do
    assert {:ok, _} = DatabaseCatalog.open(uuid)

    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("vial_keeper.database.open")
          |> Enum.find(fn s -> TestExporter.span_attr(s, :"db.uuid") == uuid end)
        end,
        message: "expected an open span for #{uuid}"
      )

    assert TestExporter.span_attr(span, :"db.uuid") == uuid
    assert TestExporter.span_attr(span, :outcome) == :ok
  end

  test "a rejected open records outcome: :rejected, status UNSET not ERROR" do
    # Open a non-registered uuid to trigger a rejection.
    assert {:error, %VialKeeper.Error{}} = DatabaseCatalog.open(VialKeeper.UUID.v4())

    span =
      Eventual.eventually(
        fn ->
          TestExporter.spans_named("vial_keeper.database.open")
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
