defmodule ElixirDB.Storage.AdapterCase do
  @moduledoc """
  Shared storage-adapter conformance case template.

  Use with:

      use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.Memory.Adapter

  Or for a physical SQLite suite:

      use ElixirDB.Storage.AdapterCase, adapter: ElixirDB.Storage.SQLite.Adapter

  Each test receives a fresh temporary database handle under `:adapter` and
  the absolute path under `:path`. Shared contract suites own storage-neutral
  expectations; SQLite-only probes remain under `test/physical/sqlite/`.
  """

  alias ElixirDB.RevisionFixtures
  alias ElixirDB.Revisions.Id
  alias ElixirDB.Revisions.Wire

  use ExUnit.CaseTemplate

  using opts do
    adapter = Keyword.get(opts, :adapter)
    adapters = Keyword.get(opts, :adapters)

    quote do
      use ExUnit.Case, async: true
      alias ElixirDB.Storage.AdapterCase, as: AdapterCaseModule

      @adapter unquote(adapter)
      @adapters unquote(adapters)

      setup context do
        AdapterCaseModule.open_temp_adapter(
          @adapter || Map.fetch!(context, :adapter_module),
          context
        )
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
    {:ok, bundle_path} = ElixirDB.TempDatabase.create(prefix: "elixirdb-adapter")
    path = adapter_path(adapter_mod, bundle_path)
    {:ok, adapter} = adapter_mod.create(path, %{})

    ExUnit.Callbacks.on_exit(fn ->
      _ = safe_close(adapter_mod, adapter)
      ElixirDB.TempDatabase.cleanup(bundle_path)
    end)

    {:ok, adapter: adapter, path: path, bundle_path: bundle_path, adapter_module: adapter_mod}
  end

  @doc """
  Closes `adapter` and reopens it from `path` (SQLite file or Memory `memory.backend`).
  """
  @spec reopen!(module(), term(), binary()) :: term()
  def reopen!(adapter_mod, adapter, path) when is_atom(adapter_mod) and is_binary(path) do
    case adapter_mod.close(adapter) do
      :ok -> :ok
      {:error, reason} -> raise "close failed: #{inspect(reason)}"
    end

    case adapter_mod.open(path) do
      {:ok, reopened} -> reopened
      {:error, reason} -> raise "open failed: #{inspect(reason)}"
    end
  end

  @doc """
  Absolute artifact path for a temporary adapter under `bundle_path`.
  """
  @spec adapter_path(module(), binary()) :: binary()
  def adapter_path(ElixirDB.Storage.SQLite.Adapter, bundle_path),
    do: ElixirDB.TempDatabase.sqlite_path(bundle_path)

  def adapter_path(_adapter_mod, bundle_path), do: bundle_path

  @doc """
  Builds a wire revision map for import/replication chain fixtures.
  """
  @spec wire_revision(binary(), binary(), binary() | nil, boolean(), map() | nil, binary() | nil) ::
          map()
  def wire_revision(document_id, revision_id, parent, deleted, body, history_id \\ nil) do
    history_id = history_id || RevisionFixtures.shared_history_id()
    {:ok, generation} = Id.generation(revision_id)

    Wire.new(document_id, history_id, revision_id, generation, parent, deleted, body)
  end

  defp safe_close(adapter_mod, adapter) do
    adapter_mod.close(adapter)
  rescue
    [ArgumentError, ErlangError, RuntimeError, UndefinedFunctionError] -> :ok
  catch
    _, _ -> :ok
  end
end
