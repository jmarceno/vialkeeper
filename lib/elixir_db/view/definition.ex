defmodule ElixirDB.View.Definition do
  @moduledoc "Strict normalize/validate/digest for declarative view definitions."

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.Normalizer
  alias ElixirDB.View.{Expression, Reducer}

  @known_fields ~w(name selector key value reducer)
  @max_name_bytes 128
  @max_key_expressions 8

  @type t :: %{
          required(:name) => binary(),
          required(:selector) => map(),
          required(:predicate) => term(),
          required(:key) => [map()],
          optional(:value) => map() | nil,
          required(:reducer) => Reducer.reducer(),
          required(:definition_json) => binary(),
          required(:definition_digest) => binary()
        }

  @spec normalize(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def normalize(definition) when is_map(definition) do
    definition = stringify_keys(definition)

    with :ok <- known_fields(definition),
         {:ok, name} <- normalize_name(definition["name"]),
         {:ok, selector, predicate} <- normalize_selector(definition),
         {:ok, key} <- normalize_key(definition["key"]),
         {:ok, reducer} <- Reducer.normalize_reducer(definition["reducer"]),
         {:ok, value} <- normalize_value(definition["value"], reducer),
         :ok <- validate_value_requirement(reducer, value),
         canonical <- canonical_definition(name, selector, key, value, reducer),
         {:ok, definition_json} <- Canonical.encode(canonical),
         definition_digest <-
           :crypto.hash(:sha256, definition_json) |> Base.encode16(case: :lower) do
      {:ok,
       %{
         name: name,
         selector: selector,
         predicate: predicate,
         key: key,
         value: value,
         reducer: reducer,
         definition_json: definition_json,
         definition_digest: definition_digest
       }}
    end
  end

  def normalize(_),
    do: {:error, ElixirDB.Error.invalid_request("view definition must be an object")}

  @spec digest(t() | map()) :: binary()
  def digest(%{definition_digest: digest}) when is_binary(digest), do: digest

  def digest(definition) when is_map(definition) do
    case normalize(definition) do
      {:ok, normalized} -> normalized.definition_digest
      {:error, _} -> nil
    end
  end

  defp known_fields(definition) do
    if Enum.all?(Map.keys(definition), &(&1 in @known_fields)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("view definition contains an unknown field")}
  end

  defp normalize_name(name) when is_binary(name) and name != "" do
    if byte_size(name) <= @max_name_bytes and String.valid?(name),
      do: {:ok, name},
      else: {:error, ElixirDB.Error.invalid_request("view name is invalid")}
  end

  defp normalize_name(_),
    do: {:error, ElixirDB.Error.invalid_request("view name is required")}

  defp normalize_selector(definition) do
    disallowed =
      Enum.filter(~w(sort fields limit bookmark index search), &Map.has_key?(definition, &1))

    if disallowed != [] do
      {:error, ElixirDB.Error.invalid_request("view selector cannot include query controls")}
    else
      selector = Map.get(definition, "selector", %{})

      with {:ok, normalized} <- Normalizer.normalize(%{"selector" => selector}) do
        {:ok, normalized.selector, normalized.predicate}
      end
    end
  end

  defp normalize_key(key) when is_list(key) and key != [] and length(key) <= @max_key_expressions do
    Enum.reduce_while(key, {:ok, []}, fn expression, {:ok, acc} ->
      case Expression.normalize(expression) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, expressions} -> {:ok, Enum.reverse(expressions)}
      error -> error
    end)
  end

  defp normalize_key([]),
    do: {:error, ElixirDB.Error.invalid_request("view key must be a non-empty array")}

  defp normalize_key(_),
    do: {:error, ElixirDB.Error.invalid_request("view key must be a non-empty array")}

  defp normalize_value(nil, _reducer), do: {:ok, nil}

  defp normalize_value(expression, _reducer) when is_map(expression),
    do: Expression.normalize(expression)

  defp normalize_value(_value, _reducer),
    do: {:error, ElixirDB.Error.invalid_request("view value expression is invalid")}

  defp validate_value_requirement(nil, _value), do: :ok
  defp validate_value_requirement(:_count, _value), do: :ok
  defp validate_value_requirement(_reducer, value) when not is_nil(value), do: :ok

  defp validate_value_requirement(_reducer, _value),
    do: {:error, ElixirDB.Error.invalid_request("view value is required for this reducer")}

  defp canonical_definition(name, selector, key, value, reducer) do
    %{
      "name" => name,
      "selector" => selector,
      "key" => key,
      "value" => value,
      "reducer" => reducer_name(reducer)
    }
  end

  defp reducer_name(nil), do: nil
  defp reducer_name(reducer) when is_atom(reducer), do: Atom.to_string(reducer)

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end
end
