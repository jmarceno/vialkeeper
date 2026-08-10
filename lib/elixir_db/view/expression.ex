defmodule ElixirDB.View.Expression do
  @moduledoc "Evaluates declarative view path/literal expressions against document bodies."

  alias ElixirDB.JSON.Pointer
  alias ElixirDB.View.Number

  @type t :: %{required(:path) => binary()} | %{required(:literal) => term()}

  @spec evaluate(t(), map()) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  def evaluate(%{"path" => path}, document) when is_binary(path),
    do: evaluate(%{path: path}, document)

  def evaluate(%{path: path}, document) when is_binary(path) do
    with {:ok, tokens} <- Pointer.parse(path) do
      case tokens do
        [] -> {:ok, document}
        _ -> resolve_pointer(document, path)
      end
    end
  end

  def evaluate(%{"literal" => literal}, _document), do: {:ok, literal}
  def evaluate(%{literal: literal}, _document), do: {:ok, literal}

  def evaluate(_, _),
    do: {:error, ElixirDB.Error.invalid_request("view expression is invalid")}

  @spec normalize(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def normalize(expression) when is_map(expression) do
    expression =
      Map.new(expression, fn {key, value} ->
        {to_string(key), value}
      end)

    keys = Map.keys(expression)

    cond do
      keys == ["path"] ->
        normalize_path(expression["path"])

      keys == ["literal"] ->
        normalize_literal(expression["literal"])

      true ->
        {:error,
         ElixirDB.Error.invalid_request(
           "view expression must contain exactly one of path or literal"
         )}
    end
  end

  def normalize(_),
    do: {:error, ElixirDB.Error.invalid_request("view expression must be an object")}

  defp normalize_path(path) when is_binary(path) and path != "" do
    case Pointer.parse(path) do
      {:ok, _} -> {:ok, %{"path" => path}}
      {:error, _} = error -> error
    end
  end

  defp normalize_path(_),
    do: {:error, ElixirDB.Error.invalid_request("view expression path is invalid")}

  defp normalize_literal(nil), do: {:ok, %{"literal" => nil}}
  defp normalize_literal(value) when is_boolean(value), do: {:ok, %{"literal" => value}}

  defp normalize_literal(value) when is_number(value) do
    if Number.finite?(value),
      do: {:ok, %{"literal" => Number.normalize_zero(value)}},
      else:
        {:error, ElixirDB.Error.invalid_request("view expression literal must be a finite number")}
  end

  defp normalize_literal(value) when is_binary(value) do
    if String.valid?(value),
      do: {:ok, %{"literal" => value}},
      else: {:error, ElixirDB.Error.invalid_request("view expression literal must be valid UTF-8")}
  end

  defp normalize_literal(_),
    do: {:error, ElixirDB.Error.invalid_request("view expression literal must be a JSON scalar")}

  defp resolve_pointer(document, path) do
    case Pointer.get(document, path) do
      {:ok, value} -> {:ok, value}
      :missing -> {:ok, nil}
      {:error, _} = error -> error
    end
  end
end
