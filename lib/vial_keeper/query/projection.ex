defmodule VialKeeper.Query.Projection do
  alias VialKeeper.JSON.Pointer
  @moduledoc "Storage-neutral query result projection."

  @type compiled_field :: {binary(), [binary()]}

  @spec compile_fields(nil | list()) ::
          {:ok, nil | [compiled_field()]} | {:error, VialKeeper.Error.t()}
  def compile_fields(nil), do: {:ok, nil}

  def compile_fields(fields) when is_list(fields) do
    Enum.reduce_while(fields, {:ok, []}, fn path, {:ok, acc} ->
      case Pointer.parse(path) do
        {:ok, tokens} -> {:cont, {:ok, [{path, tokens} | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, fields} -> {:ok, Enum.reverse(fields)}
      {:error, _} = error -> error
    end)
  end

  def compile_fields(_fields),
    do: {:error, VialKeeper.Error.invalid_request("fields must be an array")}

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

  @doc "Projects a document using pointers compiled once for the query batch."
  @spec project_compiled(map(), nil | [compiled_field()]) :: map()
  def project_compiled(document, nil), do: document(document.id, document.revision, document.body)

  def project_compiled(document, fields) when is_list(fields) do
    %{
      id: document.id,
      revision: document.revision,
      fields:
        Map.new(
          Enum.flat_map(fields, fn {path, tokens} ->
            case Pointer.get_tokens(document.body, tokens) do
              {:ok, value} -> [{path, value}]
              _ -> []
            end
          end)
        )
    }
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
