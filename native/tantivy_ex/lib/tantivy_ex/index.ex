defmodule TantivyEx.Index do
  @moduledoc """
  Index management for TantivyEx.

  An index is a collection of segments that contain documents and their associated
  search indices.
  """

  alias TantivyEx.{Native, Schema}

  @type t :: reference()

  @doc """
  Creates a new index in the specified directory.

  The directory will be created if it doesn't exist. The index metadata
  and segment files will be stored in this directory.

  ## Parameters

  - `path`: The filesystem path where the index should be created
  - `schema`: The schema defining the structure of documents

  ## Examples

      iex> schema = TantivyEx.Schema.new()
      iex> schema = TantivyEx.Schema.add_text_field(schema, "title", :text_stored)
      iex> {:ok, index} = TantivyEx.Index.create_in_dir("/tmp/my_index", schema)
      iex> is_reference(index)
      true
  """
  @spec create_in_dir(String.t(), Schema.t()) :: {:ok, t()} | {:error, String.t()}
  def create_in_dir(path, schema) do
    case Native.index_create_in_dir(path, schema) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to create index: #{inspect(e)}"}
  end

  @doc """
  Creates a new index in RAM.

  This creates an in-memory index that doesn't persist to disk.
  Useful for testing or temporary indices.

  ## Parameters

  - `schema`: The schema defining the structure of documents

  ## Examples

      iex> schema = TantivyEx.Schema.new()
      iex> schema = TantivyEx.Schema.add_text_field(schema, "title", :text_stored)
      iex> {:ok, index} = TantivyEx.Index.create_in_ram(schema)
      iex> is_reference(index)
      true
  """
  @spec create_in_ram(Schema.t()) :: {:ok, t()} | {:error, String.t()}
  def create_in_ram(schema) do
    case Native.index_create_in_ram(schema) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to create RAM index: #{inspect(e)}"}
  end

  @doc """
  Opens an existing index at the specified path.

  This function attempts to open an index that already exists at the given path.
  If the index doesn't exist, it will return an error.

  ## Parameters

  - `path`: The filesystem path where the existing index is located

  ## Examples

      iex> {:ok, index} = TantivyEx.Index.open("/tmp/existing_index")
      iex> is_reference(index)
      true
  """
  @spec open(String.t()) :: {:ok, t()} | {:error, String.t()}
  def open(path) do
    case Native.index_open_in_dir(path) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to open index: #{inspect(e)}"}
  end

  @doc """
  Creates a new index at the given path using a no-sync directory.

  Identical to `create_in_dir/2`, except file writes are flushed to the page
  cache but never fsynced: commits become visible to readers immediately,
  while durability is deferred until `sync_all/1` is called on the path.
  This suits derived, rebuildable index caches where a torn index can be
  rebuilt from an authoritative store.
  """
  @spec create_in_dir_nosync(String.t(), Schema.t()) :: {:ok, t()} | {:error, String.t()}
  def create_in_dir_nosync(path, schema) do
    case Native.index_create_in_dir_nosync(path, schema) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to create index: #{inspect(e)}"}
  end

  @doc """
  Opens an existing index at the specified path using a no-sync directory.

  Reads behave exactly like `open/1` (memory-mapped segment files); writes
  (refresh commits) skip fsync and are durable only after `sync_all/1`.
  """
  @spec open_nosync(String.t()) :: {:ok, t()} | {:error, String.t()}
  def open_nosync(path) do
    case Native.index_open_in_dir_nosync(path) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to open index: #{inspect(e)}"}
  end

  @doc """
  Fsyncs every file under the index directory and the directory itself.

  Restores full crash durability for an index created or opened with the
  no-sync directory. Returns `:ok` if the path does not exist.
  """
  @spec sync_all(String.t()) :: :ok | {:error, String.t()}
  def sync_all(path) when is_binary(path) do
    case Native.index_sync_all(path) do
      {:error, reason} -> {:error, reason}
      result -> result
    end
  rescue
    e -> {:error, "Failed to sync index: #{inspect(e)}"}
  end

  @doc """
  Opens an existing index at the specified path, or creates it if it doesn't exist.

  This is the recommended function for most use cases as it handles both
  opening existing indices and creating new ones with a single API call.
  The directory will be created if it doesn't exist.

  ## Parameters

  - `path`: The filesystem path where the index should be opened/created
  - `schema`: The schema defining the structure of documents (used only when creating)

  ## Examples

      iex> schema = TantivyEx.Schema.new()
      iex> schema = TantivyEx.Schema.add_text_field(schema, "title", :text_stored)
      iex> {:ok, index} = TantivyEx.Index.open_or_create("/tmp/my_index", schema)
      iex> is_reference(index)
      true

      # Subsequent calls will open the existing index
      iex> {:ok, same_index} = TantivyEx.Index.open_or_create("/tmp/my_index", schema)
      iex> is_reference(same_index)
      true
  """
  @spec open_or_create(String.t(), Schema.t()) :: {:ok, t()} | {:error, String.t()}
  def open_or_create(path, schema) do
    case Native.index_open_or_create_in_dir(path, schema) do
      {:error, reason} -> {:error, reason}
      index -> {:ok, index}
    end
  rescue
    e -> {:error, "Failed to open or create index: #{inspect(e)}"}
  end
end
