defmodule VialKeeper.Storage.Results do
  @moduledoc """
  Storage-neutral result structs for owner replies.

  HTTP still maps these to plain JSON envelopes via `to_public/1`.
  The nested structs are internal result envelopes and are documented through
  this parent module.
  """

  alias VialKeeper.Domain.Revision
  alias VialKeeper.MapAccess
  alias VialKeeper.Revisions.Winner

  defmodule GetDocument do
    @moduledoc false
    @enforce_keys [:id, :revision, :deleted, :body, :sequence]
    defstruct [:id, :revision, :deleted, :body, :sequence, :conflicts, :attachments]

    @type t :: %__MODULE__{
            id: binary(),
            revision: binary(),
            deleted: boolean(),
            body: map() | nil,
            sequence: non_neg_integer(),
            conflicts: [map()] | nil,
            attachments: map() | nil
          }
  end

  defmodule PutDocument do
    @moduledoc false
    @enforce_keys [:revision, :sequence, :replayed]
    defstruct [:id, :revision, :deleted, :body, :sequence, :replayed, :attachments]

    @type t :: %__MODULE__{
            id: binary() | nil,
            revision: binary(),
            deleted: boolean() | nil,
            body: map() | nil,
            sequence: non_neg_integer(),
            replayed: boolean(),
            attachments: map() | nil
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
  def error(%VialKeeper.Error{} = error), do: {:error, error}

  @doc "Builds the shared logical document result map from document and revision facts."
  @spec document_map(map(), Revision.t(), [Revision.t()]) :: map()
  def document_map(document, %Revision{} = revision, leaves)
      when is_map(document) and is_list(leaves) do
    result = %{
      id: MapAccess.get(document, :document_id),
      revision: revision.revision_id,
      deleted: revision.deleted,
      body: revision.body,
      sequence: MapAccess.get(document, :update_sequence),
      attachments: revision.attachments || %{}
    }

    if leaves == [],
      do: result,
      else: Map.put(result, :conflicts, Winner.conflicts(leaves, revision))
  end

  @doc """
  Builds a GetDocument result from a shaped document map (adapter `to_result`).
  """
  def get_document(map) when is_map(map) do
    %GetDocument{
      id: MapAccess.get(map, :id),
      revision: MapAccess.get(map, :revision),
      deleted: MapAccess.get(map, :deleted, false),
      body: MapAccess.get(map, :body),
      sequence: MapAccess.get(map, :sequence, 0),
      conflicts: MapAccess.get(map, :conflicts),
      attachments: MapAccess.get(map, :attachments)
    }
  end

  @doc """
  Builds a PutDocument result from a mutation reply map.
  """
  def put_document(map) when is_map(map) do
    %PutDocument{
      id: MapAccess.get(map, :id),
      revision: MapAccess.get(map, :revision),
      deleted: MapAccess.get(map, :deleted),
      body: MapAccess.get(map, :body),
      sequence: MapAccess.get(map, :sequence, 0),
      replayed: MapAccess.get(map, :replayed, false),
      attachments: MapAccess.get(map, :attachments)
    }
  end

  @doc """
  Builds a ReadChanges result from an adapter changes batch.
  """
  def read_changes(map) when is_map(map) do
    %ReadChanges{
      results: MapAccess.get(map, :results, []),
      last_sequence: MapAccess.get(map, :last_sequence, 0),
      has_more: MapAccess.get(map, :has_more, false)
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
      "sequence" => result.sequence,
      "attachments" => public_attachments(result.attachments)
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
      "replayed" => result.replayed,
      "attachments" => public_attachments(result.attachments)
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

  defp public_attachments(nil), do: %{}

  defp public_attachments(attachments) when is_map(attachments) do
    Map.new(attachments, fn {name, entry} ->
      {to_string(name), public_attachment_entry(entry)}
    end)
  end

  defp public_attachment_entry(entry) when is_map(entry) do
    digest = MapAccess.get(entry, :digest) || MapAccess.get(entry, :blob)
    length = MapAccess.get(entry, :length)
    content_type = MapAccess.get(entry, :content_type)

    %{
      "blob" => digest,
      "length" => length,
      "content_type" => content_type
    }
  end

  defp public_attachment_entry(other), do: other

  defp public_key(key) when is_atom(key), do: Atom.to_string(key)
  defp public_key(key), do: key
end
