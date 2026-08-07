defmodule ElixirDB.Storage.AdapterCase do
  @moduledoc """
  Shared storage-adapter conformance case template.

  Use with:

      use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  Each test receives a fresh temporary database handle under `:adapter` and
  the absolute path under `:path`. The suite owns storage-neutral expectations;
  SQLite-only probes remain in dedicated tests.
  """

  alias ElixirDB.Revisions.Id
  alias ElixirDB.Revisions.Wire

  use ExUnit.CaseTemplate

  using opts do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      use ExUnit.Case, async: false
      alias ElixirDB.Storage.AdapterCase, as: AdapterCaseModule

      @adapter unquote(adapter)

      setup context do
        AdapterCaseModule.open_temp_adapter(@adapter, context)
      end

      defp wire(document_id, revision_id, parent, deleted, body) do
        AdapterCaseModule.wire_revision(document_id, revision_id, parent, deleted, body)
      end
    end
  end

  @doc """
  Opens a temporary adapter instance and registers cleanup for the test process.
  """
  @spec open_temp_adapter(module(), map()) :: {:ok, keyword()}
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
    {:ok, generation} = Id.generation(revision_id)

    Wire.new(document_id, revision_id, generation, parent, deleted, body)
  end

  defp safe_close(adapter_mod, adapter) do
    adapter_mod.close(adapter)
  rescue
    [ArgumentError, ErlangError, RuntimeError, UndefinedFunctionError] -> :ok
  catch
    _, _ -> :ok
  end
end
