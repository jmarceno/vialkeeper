defmodule ElixirDB.Storage.Results do
  @moduledoc """
  Storage-neutral result structs for owner replies (Plan §6.4 / gap F6).

  HTTP still maps these to plain JSON envelopes via `to_public/1`.
  """

  defmodule GetDocument do
    @moduledoc false
    @enforce_keys [:id, :revision, :deleted, :body, :sequence]
    defstruct [:id, :revision, :deleted, :body, :sequence, :conflicts]

    @type t :: %__MODULE__{
            id: binary(),
            revision: binary(),
            deleted: boolean(),
            body: map() | nil,
            sequence: non_neg_integer(),
            conflicts: [map()] | nil
          }
  end

  defmodule PutDocument do
    @moduledoc false
    @enforce_keys [:revision, :sequence, :replayed]
    defstruct [:id, :revision, :deleted, :body, :sequence, :replayed]

    @type t :: %__MODULE__{
            id: binary() | nil,
            revision: binary(),
            deleted: boolean() | nil,
            body: map() | nil,
            sequence: non_neg_integer(),
            replayed: boolean()
          }
  end

  defmodule ReadChanges do
    @moduledoc false
    @enforce_keys [:results, :last_sequence, :has_more]
    defstruct [:results, :last_sequence, :has_more]

    @type t :: %__MODULE__{
            results: [map()],
            last_sequence: non_neg_integer(),
            has_more: boolean()
          }
  end

  def ok(value), do: {:ok, value}
  def error(%ElixirDB.Error{} = error), do: {:error, error}

  @doc """
  Builds a GetDocument result from a shaped document map (adapter `to_result`).
  """
  def get_document(map) when is_map(map) do
    %GetDocument{
      id: map[:id] || map["id"],
      revision: map[:revision] || map["revision"],
      deleted: map[:deleted] || map["deleted"] || false,
      body: map[:body] || map["body"],
      sequence: map[:sequence] || map["sequence"] || 0,
      conflicts: map[:conflicts] || map["conflicts"]
    }
  end

  @doc """
  Builds a PutDocument result from a mutation reply map.
  """
  def put_document(map) when is_map(map) do
    %PutDocument{
      id: map[:id] || map["id"],
      revision: map[:revision] || map["revision"],
      deleted: Map.get(map, :deleted, Map.get(map, "deleted")),
      body: Map.get(map, :body, Map.get(map, "body")),
      sequence: map[:sequence] || map["sequence"] || 0,
      replayed: map[:replayed] || map["replayed"] || false
    }
  end

  @doc """
  Builds a ReadChanges result from an adapter changes batch.
  """
  def read_changes(map) when is_map(map) do
    %ReadChanges{
      results: map[:results] || map["results"] || [],
      last_sequence: map[:last_sequence] || map["last_sequence"] || 0,
      has_more: map[:has_more] || map["has_more"] || false
    }
  end

  @doc """
  Converts result structs (and nested maps/lists) into JSON-friendly plain maps.
  """
  def to_public(%GetDocument{} = result) do
    base = %{
      "id" => result.id,
      "revision" => result.revision,
      "deleted" => result.deleted,
      "body" => result.body,
      "sequence" => result.sequence
    }

    if is_list(result.conflicts),
      do: Map.put(base, "conflicts", to_public(result.conflicts)),
      else: base
  end

  def to_public(%PutDocument{} = result) do
    %{
      "id" => result.id,
      "revision" => result.revision,
      "deleted" => result.deleted,
      "body" => result.body,
      "sequence" => result.sequence,
      "replayed" => result.replayed
    }
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new()
  end

  def to_public(%ReadChanges{} = result) do
    %{
      "results" => to_public(result.results),
      "last_sequence" => result.last_sequence,
      "has_more" => result.has_more
    }
  end

  def to_public(list) when is_list(list), do: Enum.map(list, &to_public/1)

  def to_public(map) when is_map(map) and not is_struct(map) do
    Map.new(map, fn {key, value} -> {public_key(key), to_public(value)} end)
  end

  def to_public(%_{} = struct), do: struct |> Map.from_struct() |> to_public()
  def to_public(other), do: other

  defp public_key(key) when is_atom(key), do: Atom.to_string(key)
  defp public_key(key), do: key
end
