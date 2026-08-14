defmodule ElixirDB.Observability.CommandSpanTest do
  @moduledoc """
  Assert the `elixir_db.database.command` span is emitted with the
  correct `command.type`; that a `:revision_conflict` put sets `error.code`
  and keeps status UNSET; that an injected `:internal_error` sets status
  ERROR; and parentage (no HTTP → root span).
  """

  use ElixirDB.Observability.OtelCase, async: false

  @moduletag :integration

  alias ElixirDB.Documents
  alias ElixirDB.Eventual
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

    span = command_span(uuid, &(TestExporter.span_attr(&1, :"command.type") == :put))

    assert TestExporter.span_attr(span, :"command.type") == :put
  end

  test "a get emits a command span with command.type: :get", %{uuid: uuid} do
    assert {:error, %ElixirDB.Error{code: :document_not_found}} =
             Documents.get(uuid, %{id: "nope"})

    span = command_span(uuid, &(TestExporter.span_attr(&1, :"command.type") == :get))

    assert TestExporter.span_attr(span, :"command.type") == :get
  end

  test "command spans from direct (non-HTTP) calls are roots", %{uuid: uuid} do
    assert {:ok, _} = Documents.put(uuid, %{id: "doc-root", body: %{"value" => 1}})

    span = command_span(uuid, &(TestExporter.span_attr(&1, :"command.type") == :put))

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

    span =
      command_span(
        uuid,
        &(TestExporter.span_attr(&1, :"error.code") == :revision_conflict)
      )

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

    span =
      command_span(
        uuid,
        &(TestExporter.span_attr(&1, :"error.code") == :internal_error)
      )

    # §6.5: only :internal_error marks the span ERROR.
    assert TestExporter.status_code(span) == :error,
           "internal_error must set status ERROR, got #{inspect(span[:status])}"
  end

  defp command_span(uuid, predicate) when is_function(predicate, 1) do
    # FLAKE: `Eventual.eventually` polls until a matching span appears, but under
    # full-suite load a span from a *different* test with the same uuid (leftover) can be
    # found first, or the expected span can be flushed late. This helper masks most races
    # but the "get"/"put" tests still flirt with the async exporter. Prefer a deterministic
    # exporter flush (or fresh uuid per test) so `Enum.find` only ever sees this test's span.
    Eventual.eventually(
      fn ->
        TestExporter.spans_named("elixir_db.database.command")
        |> Enum.find(fn span ->
          TestExporter.span_attr(span, :"db.uuid") == uuid and predicate.(span)
        end)
      end,
      message: "command span was not exported for #{uuid}"
    )
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
