defmodule ElixirDB.Storage.SQLite.IndexCandidates do
  @moduledoc """
  SQLite index definition and candidate-retrieval port.

  Candidate rows are normalized maps. Physical names stay inside opaque
  `:backend_meta` when present.
  """
  @behaviour ElixirDB.Storage.Ports.IndexCandidates

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Ports.Errors
  alias ElixirDB.Storage.SQLite.{Adapter, Context, Indexes}

  @impl true
  def list_indexes(%BackendContext{} = context) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Adapter.list_indexes(adapter) do
        {:ok, indexes} -> {:ok, Enum.map(indexes, &public_index/1)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def create_index(%BackendContext{} = context, definition) when is_map(definition) do
    with {:ok, adapter} <- Context.unwrap(context) do
      case Adapter.create_index(adapter, definition) do
        {:ok, index} -> {:ok, public_index(index)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    end
  end

  @impl true
  def delete_index(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.delete_index(adapter, index_id))
    end
  end

  @impl true
  def rebuild_index(%BackendContext{} = context, index_id) when is_binary(index_id) do
    with {:ok, adapter} <- Context.unwrap(context) do
      Errors.wrap(Adapter.rebuild_index(adapter, index_id))
    end
  end

  @impl true
  def lookup_candidates(%BackendContext{} = context, request) when is_map(request) do
    full_text_candidates(context, request)
  end

  @impl true
  def full_text_candidates(%BackendContext{} = context, request) when is_map(request) do
    text = MapAccess.get(request, :text) || MapAccess.get(request, :query)
    mode = MapAccess.get(request, :mode, "all")

    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, indexes} <- Adapter.list_indexes(adapter),
         {:ok, metadata} <- find_index(indexes, MapAccess.get(request, :index_id)),
         true <- is_binary(text) do
      case Indexes.search(adapter.conn, metadata, text, mode) do
        {:ok, rows} -> {:ok, Enum.map(rows, &public_candidate/1)}
        {:error, reason} -> {:error, Errors.normalize(reason)}
      end
    else
      false ->
        {:error, ElixirDB.Error.invalid_request("index candidate text is required")}

      {:error, reason} ->
        {:error, Errors.normalize(reason)}
    end
  end

  @impl true
  def range_scan_candidates(%BackendContext{} = context, request) when is_map(request) do
    with {:ok, adapter} <- Context.unwrap(context),
         {:ok, indexes} <- Adapter.list_indexes(adapter),
         {:ok, metadata} <- find_index(indexes, MapAccess.get(request, :index_id)) do
      # Structured range scans are executed by QueryRunner today; expose an empty
      # candidate page when the index exists so shared executors can post-filter.
      _ = {adapter, metadata, request}
      {:ok, []}
    end
  end

  defp find_index(indexes, index_id) when is_binary(index_id) do
    case Enum.find(
           indexes,
           &(MapAccess.get(&1, :id) == index_id or MapAccess.get(&1, "id") == index_id)
         ) do
      nil -> {:error, ElixirDB.Error.index_not_found("index not found")}
      metadata -> {:ok, metadata}
    end
  end

  defp find_index(_, _), do: {:error, ElixirDB.Error.invalid_request("index_id is required")}

  defp public_index(index) when is_map(index) do
    physical = MapAccess.get(index, :physical_name) || MapAccess.get(index, "physical_name")

    index
    |> Map.drop([:physical_name, "physical_name"])
    |> Map.put(:backend_meta, %{physical_name: physical})
  end

  defp public_candidate(row) when is_map(row) do
    doc_key = Map.get(row, :doc_key)

    row
    |> Map.drop([:doc_key])
    |> Map.put(:backend_meta, %{doc_key: doc_key})
  end
end
