defmodule ElixirDB.DerivedView.Definition do
  @moduledoc "Validates and canonicalizes materialized federated view definitions."

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.Normalizer
  alias ElixirDB.View.Definition, as: ViewDefinition
  alias ElixirDB.View.Reducer

  @known_fields ~w(version name sources map reduce group_level options enabled)
  @option_fields ~w(max_concurrent_sources batch_documents retry_base_delay_ms retry_max_delay_ms)
  @default_options %{
    "max_concurrent_sources" => 4,
    "batch_documents" => 100,
    "retry_base_delay_ms" => 500,
    "retry_max_delay_ms" => 30_000
  }

  @type t :: %{
          required(:version) => 1,
          required(:name) => binary(),
          required(:sources) => [binary()],
          required(:selector) => map(),
          required(:predicate) => term(),
          required(:key) => [map()],
          required(:value) => map() | nil,
          required(:reducer) => atom() | nil,
          required(:group_level) => non_neg_integer() | nil,
          required(:options) => map(),
          required(:options_json) => binary(),
          required(:definition_json) => binary(),
          required(:definition_digest) => binary(),
          required(:enabled) => boolean()
        }

  @spec normalize(map(), keyword()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def normalize(definition, opts \\ [])

  def normalize(definition, opts) when is_map(definition) and is_list(opts) do
    with :ok <- Normalizer.validate_key_collisions(definition),
         definition <- stringify_keys(definition),
         :ok <- known_fields(definition),
         :ok <- validate_version(Map.get(definition, "version", 1)),
         {:ok, name} <- normalize_name(Map.get(definition, "name")),
         {:ok, sources} <- normalize_sources(Map.get(definition, "sources"), opts),
         {:ok, map_definition} <- normalize_map_definition(Map.get(definition, "map"), name),
         {:ok, reducer} <- normalize_reducer(Map.get(definition, "reduce")),
         {:ok, group_level} <-
           normalize_group_level(definition, reducer, length(map_definition.key)),
         {:ok, options} <- normalize_options(Map.get(definition, "options", %{}), length(sources)),
         {:ok, enabled} <- normalize_enabled(Map.get(definition, "enabled", true)),
         canonical <-
           canonical_definition(name, sources, map_definition, reducer, group_level, options),
         {:ok, definition_json} <- Canonical.encode(canonical),
         {:ok, options_json} <- Canonical.encode(options) do
      {:ok,
       %{
         version: 1,
         name: name,
         sources: sources,
         selector: map_definition.selector,
         predicate: map_definition.predicate,
         key: map_definition.key,
         value: map_definition.value,
         reducer: reducer,
         group_level: group_level,
         options: options,
         options_json: options_json,
         definition_json: definition_json,
         definition_digest: digest_json(definition_json),
         enabled: enabled
       }}
    end
  end

  def normalize(_definition, _opts),
    do: {:error, ElixirDB.Error.invalid_request("materialized view definition must be an object")}

  @spec digest(t() | map()) :: binary() | nil
  def digest(%{definition_digest: digest}) when is_binary(digest), do: digest

  def digest(definition) when is_map(definition) do
    case normalize(definition) do
      {:ok, normalized} -> normalized.definition_digest
      {:error, _} -> nil
    end
  end

  def digest(_), do: nil

  @doc "Builds the durable initial state passed to a derived SQLite bundle."
  @spec initial_metadata(t(), binary()) :: map()
  def initial_metadata(%{} = definition, materialization_id) when is_binary(materialization_id) do
    %{
      materialization_id: materialization_id,
      name: definition.name,
      definition_json: definition.definition_json,
      definition_digest: definition.definition_digest,
      enabled: definition.enabled,
      status: if(definition.enabled, do: :rebuilding, else: :disabled),
      options_json: definition.options_json,
      sources: definition.sources
    }
  end

  defp known_fields(definition) do
    if Enum.all?(Map.keys(definition), &(&1 in @known_fields)),
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.invalid_request("materialized view definition contains an unknown field")}
  end

  defp validate_version(1), do: :ok

  defp validate_version(_),
    do: {:error, ElixirDB.Error.invalid_request("materialized view version is unsupported")}

  defp normalize_name(name) when is_binary(name) and name != "" do
    if String.valid?(name) and byte_size(name) <= 128,
      do: {:ok, name},
      else: {:error, ElixirDB.Error.invalid_request("materialized view name is invalid")}
  end

  defp normalize_name(_),
    do: {:error, ElixirDB.Error.invalid_request("materialized view name is required")}

  defp normalize_sources(sources, opts) when is_list(sources) and sources != [] do
    max_sources =
      Keyword.get(
        opts,
        :max_sources,
        ElixirDB.Config.host_limits()[:max_materialized_view_sources] || 32
      )

    with :ok <- validate_source_count(sources, max_sources),
         :ok <- validate_source_values(sources),
         :ok <- validate_source_duplicates(sources),
         {:ok, normalized} <- normalize_source_values(sources),
         :ok <- validate_self_dependency(normalized, opts) do
      {:ok, normalized}
    end
  end

  defp normalize_sources(_, _opts),
    do:
      {:error,
       ElixirDB.Error.invalid_request("materialized view sources must be a non-empty array")}

  defp normalize_map_definition(map_definition, name) when is_map(map_definition) do
    map_definition = stringify_keys(map_definition)

    if Enum.all?(Map.keys(map_definition), &(&1 in ~w(selector key value))) do
      map_definition
      |> Map.put("name", name)
      |> ViewDefinition.normalize()
      |> case do
        {:ok, normalized} ->
          {:ok,
           %{
             selector: normalized.selector,
             predicate: normalized.predicate,
             key: normalized.key,
             value: normalized.value
           }}

        {:error, _} = error ->
          error
      end
    else
      {:error, ElixirDB.Error.invalid_request("materialized view map contains an unknown field")}
    end
  end

  defp normalize_map_definition(_, _),
    do: {:error, ElixirDB.Error.invalid_request("materialized view map must be an object")}

  defp normalize_reducer(nil), do: {:ok, nil}
  defp normalize_reducer(value), do: Reducer.normalize_reducer(value)

  defp normalize_group_level(definition, nil, _key_length) do
    if Map.has_key?(definition, "group_level"),
      do: {:error, ElixirDB.Error.invalid_request("group_level is only valid with a reducer")},
      else: {:ok, nil}
  end

  defp normalize_group_level(definition, _reducer, key_length) do
    case Map.fetch(definition, "group_level") do
      {:ok, level} when is_integer(level) and level >= 0 and level <= key_length ->
        {:ok, level}

      {:ok, _} ->
        {:error, ElixirDB.Error.invalid_request("group_level must be within the map key length")}

      :error ->
        {:error, ElixirDB.Error.invalid_request("group_level is required with a reducer")}
    end
  end

  defp normalize_options(options, source_count) when is_map(options) do
    options = stringify_keys(options)

    if Enum.all?(Map.keys(options), &(&1 in @option_fields)) do
      defaults =
        Map.put(
          @default_options,
          "max_concurrent_sources",
          min(@default_options["max_concurrent_sources"], source_count)
        )

      normalized = Map.merge(defaults, options)
      limits = ElixirDB.Config.host_limits()
      max_concurrent_limit = limits[:max_materialized_view_concurrent_sources] || 16
      batch_limit = limits[:max_materialized_view_batch_documents] || 500
      retry_limit = limits[:max_materialized_view_retry_delay_ms] || 300_000

      with :ok <- positive_option(normalized["max_concurrent_sources"], "max_concurrent_sources"),
           :ok <- positive_option(normalized["batch_documents"], "batch_documents"),
           :ok <- positive_option(normalized["retry_base_delay_ms"], "retry_base_delay_ms"),
           :ok <- positive_option(normalized["retry_max_delay_ms"], "retry_max_delay_ms"),
           :ok <-
             at_most(normalized["max_concurrent_sources"], source_count, "max_concurrent_sources"),
           :ok <-
             at_most(
               normalized["max_concurrent_sources"],
               max_concurrent_limit,
               "max_concurrent_sources"
             ),
           :ok <- at_most(normalized["batch_documents"], batch_limit, "batch_documents"),
           :ok <- at_most(normalized["retry_base_delay_ms"], retry_limit, "retry_base_delay_ms"),
           :ok <- at_most(normalized["retry_max_delay_ms"], retry_limit, "retry_max_delay_ms"),
           :ok <- validate_retry_order(normalized) do
        {:ok, normalized}
      end
    else
      {:error, ElixirDB.Error.invalid_request("materialized view options contain an unknown field")}
    end
  end

  defp normalize_options(_, _),
    do: {:error, ElixirDB.Error.invalid_request("materialized view options must be an object")}

  defp validate_source_count(sources, max_sources) when length(sources) <= max_sources, do: :ok

  defp validate_source_count(_sources, _max_sources),
    do: {:error, ElixirDB.Error.resource_limit("materialized view has too many sources")}

  defp validate_source_values(sources) do
    if Enum.any?(sources, &(not is_binary(&1) or not uuid?(&1))) do
      {:error,
       ElixirDB.Error.invalid_request("materialized view sources must contain UUID strings")}
    else
      :ok
    end
  end

  defp validate_source_duplicates(sources) do
    if length(Enum.uniq_by(sources, &String.downcase/1)) == length(sources) do
      :ok
    else
      {:error,
       ElixirDB.Error.invalid_request("materialized view sources must not contain duplicates")}
    end
  end

  defp normalize_source_values(sources), do: {:ok, Enum.map(sources, &String.downcase/1)}

  defp validate_self_dependency(sources, opts) do
    case Keyword.get(opts, :derived_uuid) do
      nil ->
        :ok

      derived_uuid when is_binary(derived_uuid) ->
        if String.downcase(derived_uuid) in sources do
          {:error, ElixirDB.Error.invalid_request("materialized view cannot depend on itself")}
        else
          :ok
        end

      _ ->
        :ok
    end
  end

  defp validate_retry_order(options) do
    if options["retry_base_delay_ms"] <= options["retry_max_delay_ms"] do
      :ok
    else
      {:error,
       ElixirDB.Error.invalid_request("retry_base_delay_ms must be at most retry_max_delay_ms")}
    end
  end

  defp normalize_enabled(value) when is_boolean(value), do: {:ok, value}

  defp normalize_enabled(_),
    do: {:error, ElixirDB.Error.invalid_request("materialized view enabled must be boolean")}

  defp positive_option(value, _field) when is_integer(value) and value > 0, do: :ok

  defp positive_option(_value, field),
    do: {:error, ElixirDB.Error.invalid_request("materialized view #{field} must be positive")}

  defp at_most(value, maximum, _field) when value <= maximum, do: :ok

  defp at_most(_value, _maximum, field),
    do: {:error, ElixirDB.Error.resource_limit("materialized view #{field} exceeds the host limit")}

  defp canonical_definition(name, sources, map_definition, reducer, group_level, options) do
    %{
      "version" => 1,
      "name" => name,
      "sources" => sources,
      "map" => %{
        "selector" => map_definition.selector,
        "key" => map_definition.key,
        "value" => map_definition.value
      },
      "reduce" => if(reducer, do: Atom.to_string(reducer), else: nil),
      "group_level" => group_level,
      "options" => options
    }
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp uuid?(value) do
    Regex.match?(
      ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/,
      value
    )
  end

  defp digest_json(json), do: :crypto.hash(:sha256, json) |> Base.encode16(case: :lower)
end
