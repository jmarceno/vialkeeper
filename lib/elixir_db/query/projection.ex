defmodule ElixirDB.Query.Projection do
  alias ElixirDB.JSON.Pointer
  @moduledoc "Storage-neutral query result projection."

  @spec project(map(), map()) :: map()
  def project(document, request) when is_map(document) and is_map(request) do
    case get(request, :fields) do
      nil ->
        document(document.id, document.revision, document.body)

      fields ->
        %{
          id: document.id,
          revision: document.revision,
          fields:
            Map.new(
              Enum.flat_map(fields, fn path ->
                case Pointer.get(document.body, path) do
                  {:ok, value} -> [{path, value}]
                  _ -> []
                end
              end)
            )
        }
    end
  end

  @type document :: %{
          required(:id) => binary(),
          required(:revision) => binary(),
          required(:body) => map()
        }

  @doc "Builds the storage-neutral document shape consumed by query execution."
  @spec document(binary(), binary(), map()) :: document()
  def document(id, revision, body), do: %{id: id, revision: revision, body: body}

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
