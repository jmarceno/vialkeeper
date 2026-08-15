defmodule VialKeeper.View.Expression do
  @moduledoc "Evaluates declarative view path/literal expressions against document bodies."

  alias VialKeeper.JSON.Pointer
  alias VialKeeper.View.Number

  @type t :: %{required(:path) => binary()} | %{required(:literal) => term()}
  @type compiled :: {:path, [binary()]} | {:literal, term()}

  @spec compile(t()) :: {:ok, compiled()} | {:error, VialKeeper.Error.t()}
  def compile(%{"path" => path}) when is_binary(path), do: compile(%{path: path})

  def compile(%{path: path}) when is_binary(path) do
    case Pointer.parse(path) do
      {:ok, tokens} -> {:ok, {:path, tokens}}
      {:error, _} = error -> error
    end
  end

  def compile(%{"literal" => literal}), do: {:ok, {:literal, literal}}
  def compile(%{literal: literal}), do: {:ok, {:literal, literal}}

  def compile(_),
    do: {:error, VialKeeper.Error.invalid_request("view expression is invalid")}

  @spec compile_many([t()]) :: {:ok, [compiled()]} | {:error, VialKeeper.Error.t()}
  def compile_many(expressions) when is_list(expressions),
    do: compile_many(expressions, [])

  defp compile_many([], acc), do: {:ok, Enum.reverse(acc)}

  defp compile_many([expression | rest], acc) do
    case compile(expression) do
      {:ok, compiled} -> compile_many(rest, [compiled | acc])
      {:error, _} = error -> error
    end
  end

  @spec compile_optional(t() | nil) :: {:ok, compiled() | nil} | {:error, VialKeeper.Error.t()}
  def compile_optional(nil), do: {:ok, nil}
  def compile_optional(expression), do: compile(expression)

  @spec evaluate(t() | compiled(), map()) ::
          {:ok, term()} | :missing | {:error, VialKeeper.Error.t()}
  def evaluate({:path, []}, document), do: {:ok, document}

  def evaluate({:path, tokens}, document) when is_list(tokens),
    do: Pointer.get_tokens(document, tokens)

  def evaluate({:literal, literal}, _document), do: {:ok, literal}

  def evaluate(%{"path" => path}, document) when is_binary(path),
    do: evaluate(%{path: path}, document)

  def evaluate(%{path: path}, document) when is_binary(path) do
    case compile(%{path: path}) do
      {:ok, compiled} -> evaluate(compiled, document)
      {:error, _} = error -> error
    end
  end

  def evaluate(%{"literal" => literal}, _document), do: {:ok, literal}
  def evaluate(%{literal: literal}, _document), do: {:ok, literal}

  def evaluate(_, _),
    do: {:error, VialKeeper.Error.invalid_request("view expression is invalid")}

  @spec normalize(map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
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
         VialKeeper.Error.invalid_request(
           "view expression must contain exactly one of path or literal"
         )}
    end
  end

  def normalize(_),
    do: {:error, VialKeeper.Error.invalid_request("view expression must be an object")}

  defp normalize_path(path) when is_binary(path) do
    case Pointer.parse(path) do
      {:ok, _} -> {:ok, %{"path" => path}}
      {:error, _} = error -> error
    end
  end

  defp normalize_path(_),
    do: {:error, VialKeeper.Error.invalid_request("view expression path is invalid")}

  defp normalize_literal(nil), do: {:ok, %{"literal" => nil}}
  defp normalize_literal(value) when is_boolean(value), do: {:ok, %{"literal" => value}}

  defp normalize_literal(value) when is_number(value) do
    if Number.finite?(value),
      do: {:ok, %{"literal" => Number.normalize_zero(value)}},
      else:
        {:error,
         VialKeeper.Error.invalid_request("view expression literal must be a finite number")}
  end

  defp normalize_literal(value) when is_binary(value) do
    if String.valid?(value),
      do: {:ok, %{"literal" => value}},
      else:
        {:error, VialKeeper.Error.invalid_request("view expression literal must be valid UTF-8")}
  end

  defp normalize_literal(_),
    do: {:error, VialKeeper.Error.invalid_request("view expression literal must be a JSON scalar")}
end
