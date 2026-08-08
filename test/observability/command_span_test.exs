defmodule ElixirDB.Observability.CommandSpanTest do
  @moduledoc """
  Plan §7.2: assert the `elixir_db.database.command` span is emitted with the
  correct `command.type`; that a `:revision_conflict` put sets `error.code`
  and keeps status UNSET; that an injected `:internal_error` sets status
  ERROR; and parentage (no HTTP → root span).
  """

  use ElixirDB.Observability.OtelCase, async: false

  alias ElixirDB.Documents
  alias ElixirDB.MapAccess
  alias ElixirDB.Observability.Instrumentation.Database
  alias ElixirDB.Observability.TestExporter
  alias ElixirDB.Runtime.DatabaseCatalog

  setup do
    # The catalog creates the file under database_root; clean up THERE (a
    # leftover from a previous run would fail create with "database file
    # already exists").
    rel = "obs-cmd-#{System.unique_integer([:positive])}.elixirdb"
    {:ok, %{database_uuid: uuid}} = DatabaseCatalog.create(rel)

    on_exit(fn ->
      _ = DatabaseCatalog.close(uuid)
      _ = DatabaseCatalog.unregister(uuid)
      root = ElixirDB.Config.database_root()
      ElixirDB.TempDatabase.cleanup(Path.join(root, rel))
    end)

    [uuid: uuid]
  end

  test "a put emits a command span with command.type: :put", %{uuid: uuid} do
    assert {:ok, _} = Documents.put(uuid, %{id: "doc-1", body: %{"value" => 42}})

    spans = TestExporter.spans_named("elixir_db.database.command")

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"db.uuid") == uuid and
          TestExporter.span_attr(s, :"command.type") == :put
      end)

    assert span != nil,
           "no :put command span for #{uuid}; " <>
             "got: #{inspect(Enum.map(spans, &{TestExporter.span_attr(&1, :"db.uuid"), TestExporter.span_attr(&1, :"command.type")}))}"
  end

  test "a get emits a command span with command.type: :get", %{uuid: uuid} do
    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Documents.get(uuid, %{id: "nope"})

    spans = TestExporter.spans_named("elixir_db.database.command")

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"db.uuid") == uuid and
          TestExporter.span_attr(s, :"command.type") == :get
      end)

    assert span != nil, "no :get command span for #{uuid}"
  end

  test "command spans from direct (non-HTTP) calls are roots", %{uuid: uuid} do
    assert {:ok, _} = Documents.put(uuid, %{id: "doc-root", body: %{"value" => 1}})

    spans = TestExporter.spans_named("elixir_db.database.command")

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"db.uuid") == uuid and
          TestExporter.span_attr(s, :"command.type") == :put
      end)

    assert span != nil
    assert span[:parent_span_id] == :undefined
  end

  test "a revision_conflict put records error.code and keeps status UNSET", %{uuid: uuid} do
    assert {:ok, rev1} = put_revision(uuid, "doc-c", nil, %{"v" => 1})
    assert {:ok, rev2} = put_revision(uuid, "doc-c", rev1, %{"v" => 2})
    assert {:ok, _rev3} = put_revision(uuid, "doc-c", rev2, %{"v" => 3})

    # Stale-parent retry of the already-committed rev2 body: TX-006 requires a
    # revision_conflict (not a replay) once a later revision changed state.
    assert {:error, %ElixirDB.Error{code: :revision_conflict}} =
             Documents.put(uuid, %{id: "doc-c", if_revision: rev1, body: %{"v" => 2}})

    spans = TestExporter.spans_named("elixir_db.database.command")

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"db.uuid") == uuid and
          TestExporter.span_attr(s, :"error.code") == :revision_conflict
      end)

    assert span != nil,
           "no command span with error.code :revision_conflict; got error codes: " <>
             "#{inspect(Enum.map(spans, &TestExporter.span_attr(&1, :"error.code")))}"

    # §6.5: expected domain errors keep status UNSET.
    assert TestExporter.status_code(span) == :unset,
           "revision_conflict must keep status UNSET, got #{inspect(span[:status])}"
  end

  test "an injected :internal_error sets span status ERROR", %{uuid: uuid} do
    error = ElixirDB.Error.internal_error("injected for the error-policy test")

    assert {:error, ^error} =
             Database.command(
               uuid,
               {:command, :put, %{}},
               fn -> {:error, error} end
             )

    spans = TestExporter.spans_named("elixir_db.database.command")

    span =
      Enum.find(spans, fn s ->
        TestExporter.span_attr(s, :"db.uuid") == uuid and
          TestExporter.span_attr(s, :"error.code") == :internal_error
      end)

    assert span != nil, "no command span with error.code :internal_error"

    # §6.5: only :internal_error marks the span ERROR.
    assert TestExporter.status_code(span) == :error,
           "internal_error must set status ERROR, got #{inspect(span[:status])}"
  end

  # Puts a document revision and returns its revision string.
  defp put_revision(uuid, id, if_revision, body) do
    request = %{id: id, body: body}
    request = if if_revision, do: Map.put(request, :if_revision, if_revision), else: request

    case Documents.put(uuid, request) do
      {:ok, result} when is_map(result) ->
        {:ok, MapAccess.get(result, :revision)}

      other ->
        other
    end
  end
end
