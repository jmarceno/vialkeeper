defmodule ElixirDB.Federation.Normalizer do
  @moduledoc "Normalizes bounded, source-isolated federation requests."
  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.Normalizer

  @keys ~w(databases query)a
  def normalize(request, opts \\ [])

  def normalize(request, opts) when is_map(request) do
    with :ok <- Normalizer.validate_key_collisions(request),
         :ok <- known(request, @keys),
         {:ok, sources} <- sources(request, opts),
         {:ok, query} <- normalize_query(Map.get(request, :query, Map.get(request, "query")), opts),
         {:ok, json} <-
           Canonical.encode(%{"databases" => sources, "query" => canonical_query(query)}) do
      {:ok, %{databases: sources, query: query, fingerprint: digest(json)}}
    end
  end

  def normalize(_, _opts),
    do: {:error, ElixirDB.Error.invalid_request("federation request must be an object")}

  defp known(map, keys) do
    string_keys = Enum.map(keys, &Atom.to_string/1)

    if Enum.all?(Map.keys(map), &(&1 in keys or &1 in string_keys)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("federation request contains an unknown field")}
  end

  defp sources(request, opts) do
    value = Map.get(request, :databases, Map.get(request, "databases"))

    max =
      (Application.get_env(:elixir_db, :federation, []) || [])
      |> Keyword.get(:max_sources, 16)

    cond do
      not is_list(value) or value == [] ->
        {:error, ElixirDB.Error.invalid_request("databases must be a non-empty array")}

      length(value) > Keyword.get(opts, :max_sources, max) ->
        {:error, ElixirDB.Error.resource_limit("too many federation sources")}

      Enum.any?(value, &(not is_binary(&1) or not uuid?(&1))) ->
        {:error, ElixirDB.Error.invalid_request("databases must contain UUID strings")}

      true ->
        normalized = Enum.map(value, &String.downcase/1)

        if length(Enum.uniq(normalized)) != length(normalized) do
          {:error, ElixirDB.Error.invalid_request("databases must not contain duplicates")}
        else
          {:ok, normalized}
        end
    end
  end

  defp normalize_query(query, opts) when is_map(query) do
    with :ok <- reject_unsupported_query_features(query),
         :ok <- known(query, [:selector, :fields, :sort, :limit, :bookmark]),
         {:ok, normalized} <- Normalizer.normalize(query),
         normalized <- put_default_limit(normalized),
         :ok <- validate_limit(normalized.limit, opts) do
      {:ok, normalized}
    else
      {:error, _} = error ->
        error
    end
  end

  defp normalize_query(_, _opts),
    do: {:error, ElixirDB.Error.invalid_request("query must be an object")}

  defp reject_unsupported_query_features(query) do
    cond do
      Map.has_key?(query, :search) or Map.has_key?(query, "search") ->
        {:error,
         ElixirDB.Error.invalid_request("federation query does not support search or index")}

      Map.has_key?(query, :index) or Map.has_key?(query, "index") ->
        {:error,
         ElixirDB.Error.invalid_request("federation query does not support search or index")}

      true ->
        :ok
    end
  end

  defp validate_limit(limit, opts) when is_integer(limit) and limit > 0 do
    if limit <=
         Keyword.get(
           opts,
           :max_query_results,
           ElixirDB.Config.host_limits()[:max_query_results] || 500
         ),
       do: :ok,
       else:
         {:error,
          ElixirDB.Error.resource_limit("federation query limit exceeds the configured limit")}
  end

  defp validate_limit(_, _opts),
    do:
      {:error, ElixirDB.Error.resource_limit("federation query limit exceeds the configured limit")}

  defp put_default_limit(%{limit: nil} = query), do: %{query | limit: 50}
  defp put_default_limit(query), do: query

  defp canonical_query(query) do
    query
    |> query_map()
    |> Map.delete(:predicate)
    |> Map.delete(:fingerprint)
    |> Map.delete(:bookmark)
    |> stringify_terms()
  end

  defp query_map(%_{} = query), do: Map.from_struct(query)
  defp query_map(query), do: query

  defp stringify_terms(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), stringify_terms(child)} end)

  defp stringify_terms(value) when is_list(value), do: Enum.map(value, &stringify_terms/1)
  defp stringify_terms(value), do: value

  defp digest(json), do: :crypto.hash(:sha256, json) |> Base.encode16(case: :lower)

  defp uuid?(value),
    do:
      Regex.match?(
        ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/,
        value
      )
end
