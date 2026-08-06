defmodule ElixirDB.Storage.AdapterCase do
  @moduledoc """
  Shared storage-adapter conformance case template.

  Use with:

      use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  Each test receives a fresh temporary database handle under `:adapter` and
  the absolute path under `:path`. The suite owns storage-neutral expectations;
  SQLite-only probes remain in dedicated tests.
  """

  use ExUnit.CaseTemplate

  using opts do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      use ExUnit.Case, async: false

      @adapter unquote(adapter)

      setup context do
        ElixirDB.Storage.AdapterCase.open_temp_adapter(@adapter, context)
      end

      defp wire(document_id, revision_id, parent, deleted, body) do
        ElixirDB.Storage.AdapterCase.wire_revision(document_id, revision_id, parent, deleted, body)
      end
    end
  end

  @doc """
  Opens a temporary adapter instance and registers cleanup for the test process.
  """
  @spec open_temp_adapter(module(), map()) :: {:ok, map()}
  def open_temp_adapter(adapter_mod, _context) when is_atom(adapter_mod) do
    {:ok, path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-adapter")
    {:ok, adapter} = adapter_mod.create(path, %{})

    ExUnit.Callbacks.on_exit(fn ->
      _ = safe_close(adapter_mod, adapter)
      ElixirDB.TempDatabase.cleanup(path)
    end)

    {:ok, adapter: adapter, path: path, adapter_module: adapter_mod}
  end

  @doc """
  Builds a wire revision map for import/replication chain fixtures.
  """
  @spec wire_revision(binary(), binary(), binary() | nil, boolean(), map() | nil) :: map()
  def wire_revision(document_id, revision_id, parent, deleted, body) do
    {:ok, generation} = ElixirDB.Revisions.Id.generation(revision_id)

    %{
      document_id: document_id,
      revision_id: revision_id,
      generation: generation,
      parent_revision: parent,
      deleted: deleted,
      body: body
    }
  end

  defp safe_close(adapter_mod, adapter) do
    try do
      adapter_mod.close(adapter)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end
end
