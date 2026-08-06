defmodule ElixirDB.Query.Projection do
  @moduledoc "Storage-neutral query result projection."

  @spec project(map(), map()) :: map()
  def project(document, request) when is_map(document) and is_map(request) do
    case get(request, :fields) do
      nil ->
        %{id: document.id, revision: document.revision, body: document.body}

      fields ->
        %{
          id: document.id,
          revision: document.revision,
          fields:
            Map.new(
              Enum.flat_map(fields, fn path ->
                case ElixirDB.JSON.Pointer.get(document.body, path) do
                  {:ok, value} -> [{path, value}]
                  _ -> []
                end
              end)
            )
        }
    end
  end

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
