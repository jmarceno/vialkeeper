defmodule ElixirDB.Query.Plan do
  @moduledoc """
  Storage-neutral candidate-plan contract.

  A plan describes logical candidate sources, never SQL, physical SQLite
  identifiers, rowids, processes, or compiled execution objects.
  """

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Query.Regex, as: QueryRegex

  @kinds [:full_text, :single, :union, :bounded_scan]
  @pagination [:indexed, :union, :bounded_scan, :full_text]
  @runtime_keys ~w(compiled sql physical physical_name rowid doc_key pid adapter_metadata)

  @enforce_keys [:kind, :scans, :selected_indexes, :digest, :sort_compatible?, :pagination]
  defstruct [:kind, :scans, :selected_indexes, :digest, :sort_compatible?, :pagination]

  @type index_binding :: %{
          required(:index_id) => binary(),
          required(:definition_digest) => binary()
        }
  @type scan :: %{
          optional(:index_id) => binary(),
          optional(binary()) => term()
        }

  @type t :: %__MODULE__{
          kind: :full_text | :single | :union | :bounded_scan,
          scans: [scan()],
          selected_indexes: [index_binding()],
          digest: binary(),
          sort_compatible?: boolean(),
          pagination: :indexed | :union | :bounded_scan | :full_text
        }

  @spec new(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def new(attrs) when is_map(attrs) do
    with :ok <- validate_no_key_collisions(attrs),
         kind <- get(attrs, :kind),
         scans <- value_or_default(get(attrs, :scans), []),
         selected_indexes <- value_or_default(get(attrs, :selected_indexes), []),
         sort_compatible? <- get(attrs, :sort_compatible?),
         pagination <- get(attrs, :pagination),
         :ok <- validate_kind(kind),
         {:ok, scans} <- sanitize_scans(scans),
         {:ok, bindings} <- normalize_bindings(selected_indexes),
         :ok <- validate_sort_compatible(sort_compatible?),
         {:ok, pagination} <- normalize_pagination(pagination, kind),
         :ok <- validate_shape(kind, scans, bindings, pagination),
         {:ok, digest} <- calculate_digest(kind, scans, bindings, sort_compatible?, pagination) do
      {:ok,
       %__MODULE__{
         kind: kind,
         scans: scans,
         selected_indexes: bindings,
         digest: digest,
         sort_compatible?: sort_compatible?,
         pagination: pagination
       }}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("query plan must be an object")}

  @doc "Alias for `new/1`."
  @spec build(map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def build(attrs), do: new(attrs)

  @spec canonical(t()) :: map()
  def canonical(%__MODULE__{} = plan) do
    %{
      "kind" => Atom.to_string(plan.kind),
      "scans" => sanitize(plan.scans),
      "selected_indexes" => sanitize(plan.selected_indexes),
      "sort_compatible" => plan.sort_compatible?,
      "pagination" => Atom.to_string(plan.pagination)
    }
  end

  @doc "Return the canonical digest input without the digest itself."
  @spec canonical_data(t()) :: map()
  def canonical_data(plan), do: canonical(plan)

  @spec digest(t()) :: binary()
  def digest(%__MODULE__{digest: digest}), do: digest

  @spec serialize(t()) :: map()
  def serialize(%__MODULE__{} = plan) do
    canonical(plan)
    |> Map.put("digest", plan.digest)
  end

  @doc "Alias for `serialize/1`, intended for explain responses."
  @spec explain(t()) :: map()
  def explain(plan), do: serialize(plan)

  @doc "Return the ordered logical index bindings used by a plan."
  @spec index_bindings(t()) :: [index_binding()]
  def index_bindings(%__MODULE__{selected_indexes: selected_indexes}), do: selected_indexes

  @doc """
  Builds the stable Wave 1 transitional digest for one runner-selected source.

  This representation intentionally contains only the candidate kind and the
  logical index binding. Wave 3 may replace it with the complete owner-side
  planner digest without changing the bookmark checksum format.
  """
  @spec transitional_digest(:full_text | :single | :bounded_scan, index_binding() | []) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def transitional_digest(:bounded_scan, []),
    do:
      new(%{
        kind: :bounded_scan,
        scans: [],
        selected_indexes: [],
        sort_compatible?: false,
        pagination: :bounded_scan
      })
      |> digest_result()

  def transitional_digest(kind, binding) when kind in [:single, :full_text] and is_map(binding) do
    scan = %{"index_id" => binding.index_id, "type" => Atom.to_string(kind)}

    normalized_binding = %{
      index_id: binding.index_id,
      definition_digest: binding.definition_digest
    }

    new(%{
      kind: kind,
      scans: [scan],
      selected_indexes: [normalized_binding],
      sort_compatible?: false,
      pagination: pagination_for(kind)
    })
    |> digest_result()
  end

  def transitional_digest(_kind, _binding),
    do: {:error, ElixirDB.Error.invalid_request("query plan transitional binding is invalid")}

  defp validate_kind(kind) when kind in @kinds, do: :ok
  defp validate_kind(_), do: {:error, ElixirDB.Error.invalid_request("query plan kind is invalid")}

  defp validate_sort_compatible(value) when is_boolean(value), do: :ok

  defp validate_sort_compatible(_),
    do: {:error, ElixirDB.Error.invalid_request("query plan sort_compatible? must be boolean")}

  defp normalize_pagination(nil, :single), do: {:ok, :indexed}
  defp normalize_pagination(nil, :union), do: {:ok, :union}
  defp normalize_pagination(nil, :bounded_scan), do: {:ok, :bounded_scan}
  defp normalize_pagination(nil, :full_text), do: {:ok, :full_text}

  defp normalize_pagination(value, kind) when value in @pagination do
    if value == pagination_for(kind), do: {:ok, value}, else: invalid_pagination()
  end

  defp normalize_pagination(value, kind) when is_binary(value) do
    case pagination_atom(value) do
      {:ok, atom} ->
        if atom == pagination_for(kind), do: {:ok, atom}, else: invalid_pagination()

      :error ->
        invalid_pagination()
    end
  end

  defp normalize_pagination(_, _), do: invalid_pagination()

  defp pagination_atom("indexed"), do: {:ok, :indexed}
  defp pagination_atom("union"), do: {:ok, :union}
  defp pagination_atom("bounded_scan"), do: {:ok, :bounded_scan}
  defp pagination_atom("full_text"), do: {:ok, :full_text}
  defp pagination_atom(_), do: :error

  defp invalid_pagination,
    do: {:error, ElixirDB.Error.invalid_request("query plan pagination is invalid")}

  defp normalize_bindings(bindings) when is_list(bindings) do
    Enum.reduce_while(bindings, {:ok, [], MapSet.new()}, fn binding, {:ok, acc, ids} ->
      case normalize_binding(binding) do
        {:ok, normalized} ->
          append_binding(normalized, acc, ids)

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, values, _ids} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp normalize_bindings(_),
    do: {:error, ElixirDB.Error.invalid_request("query plan selected_indexes must be an array")}

  defp append_binding(normalized, acc, ids) do
    if MapSet.member?(ids, normalized.index_id) do
      {:halt, {:error, ElixirDB.Error.invalid_request("query plan index bindings must be unique")}}
    else
      {:cont, {:ok, [normalized | acc], MapSet.put(ids, normalized.index_id)}}
    end
  end

  defp normalize_binding(binding) when is_map(binding) do
    with :ok <- validate_no_key_collisions(binding),
         index_id <- get(binding, :index_id),
         definition_digest <- get(binding, :definition_digest),
         true <-
           is_binary(index_id) and index_id != "" and is_binary(definition_digest) and
             valid_digest?(definition_digest),
         true <-
           Enum.all?(
             Map.keys(binding),
             &(&1 in [:index_id, :definition_digest, "index_id", "definition_digest"])
           ) do
      {:ok, %{index_id: index_id, definition_digest: definition_digest}}
    else
      {:error, _} = error -> error
      _ -> {:error, ElixirDB.Error.invalid_request("query plan index binding is invalid")}
    end
  end

  defp normalize_binding(_),
    do: {:error, ElixirDB.Error.invalid_request("query plan index binding must be an object")}

  defp calculate_digest(kind, scans, bindings, sort_compatible?, pagination) do
    data = %{
      "kind" => Atom.to_string(kind),
      "scans" => sanitize(scans),
      "selected_indexes" => sanitize(bindings),
      "sort_compatible" => sort_compatible?,
      "pagination" => Atom.to_string(pagination)
    }

    with {:ok, encoded} <- Canonical.encode(data) do
      {:ok, :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)}
    end
  end

  defp sanitize(%QueryRegex{} = regex), do: %{"source" => QueryRegex.source(regex)}

  defp sanitize(value) when is_map(value) do
    value
    |> Enum.reject(fn {key, _value} -> safe_key(key) in @runtime_keys end)
    |> Map.new(fn {key, child} -> {safe_key(key), sanitize(child)} end)
  end

  defp sanitize(value) when is_list(value), do: Enum.map(value, &sanitize/1)
  defp sanitize(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.map(&sanitize/1)
  defp sanitize(nil), do: nil
  defp sanitize(value) when is_boolean(value), do: value
  defp sanitize(value) when is_atom(value), do: Atom.to_string(value)
  defp sanitize(value), do: value

  defp get(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))

  defp value_or_default(nil, default), do: default
  defp value_or_default(value, _default), do: value

  defp pagination_for(:single), do: :indexed
  defp pagination_for(:union), do: :union
  defp pagination_for(:bounded_scan), do: :bounded_scan
  defp pagination_for(:full_text), do: :full_text

  defp digest_result({:ok, plan}), do: {:ok, plan.digest}
  defp digest_result(error), do: error

  defp sanitize_scans(scans) when is_list(scans) do
    Enum.reduce_while(scans, {:ok, []}, fn scan, {:ok, acc} ->
      case sanitize_value(scan) do
        {:ok, sanitized} when is_map(sanitized) ->
          {:cont, {:ok, [sanitized | acc]}}

        {:ok, _} ->
          {:halt, {:error, ElixirDB.Error.invalid_request("query plan scans must be objects")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp sanitize_scans(_),
    do: {:error, ElixirDB.Error.invalid_request("query plan scans must be an array")}

  defp sanitize_value(%QueryRegex{} = regex), do: {:ok, %{"source" => QueryRegex.source(regex)}}

  defp sanitize_value(value) when is_map(value) do
    Enum.reduce_while(value, {:ok, %{}, MapSet.new()}, &sanitize_map_entry/2)
    |> then(fn
      {:ok, sanitized, _keys} -> {:ok, sanitized}
      error -> error
    end)
  end

  defp sanitize_value(value) when is_list(value) do
    Enum.reduce_while(value, {:ok, []}, fn child, {:ok, acc} ->
      case sanitize_value(child) do
        {:ok, child} -> {:cont, {:ok, [child | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp sanitize_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> sanitize_value()
  end

  defp sanitize_value(value) when is_nil(value) or is_boolean(value) or is_number(value),
    do: {:ok, value}

  defp sanitize_value(value) when is_binary(value), do: {:ok, value}
  defp sanitize_value(value) when is_atom(value), do: {:ok, Atom.to_string(value)}

  defp sanitize_value(_value),
    do: {:error, ElixirDB.Error.invalid_request("query plan contains an invalid value")}

  defp sanitize_map_entry({key, child}, {:ok, acc, keys}) do
    with {:ok, string_key} <- stringify_key(key) do
      sanitize_map_key(string_key, child, acc, keys)
    end
  end

  defp sanitize_map_key(string_key, _child, acc, keys) when string_key in @runtime_keys do
    if MapSet.member?(keys, string_key) do
      {:halt, {:error, ElixirDB.Error.invalid_request("query plan keys must be unique")}}
    else
      {:cont, {:ok, acc, MapSet.put(keys, string_key)}}
    end
  end

  defp sanitize_map_key(string_key, child, acc, keys) do
    if MapSet.member?(keys, string_key) do
      {:halt, {:error, ElixirDB.Error.invalid_request("query plan keys must be unique")}}
    else
      case sanitize_value(child) do
        {:ok, child} ->
          {:cont, {:ok, Map.put(acc, string_key, child), MapSet.put(keys, string_key)}}

        {:error, _} = error ->
          {:halt, error}
      end
    end
  end

  defp stringify_key(key) when is_binary(key), do: {:ok, key}
  defp stringify_key(key) when is_atom(key), do: {:ok, Atom.to_string(key)}

  defp stringify_key(_key),
    do: {:error, ElixirDB.Error.invalid_request("query plan key is invalid")}

  defp safe_key(key) when is_binary(key), do: key
  defp safe_key(key) when is_atom(key), do: Atom.to_string(key)
  defp safe_key(_key), do: ""

  defp valid_digest?(value) do
    is_binary(value) and Regex.match?(~r/^[0-9a-f]{64}$/, value) and
      value != String.duplicate("0", 64)
  end

  defp validate_no_key_collisions(value) when is_map(value) do
    case sanitize_value(value) do
      {:ok, _} -> :ok
      {:error, _} = error -> error
    end
  end

  defp validate_shape(:single, [scan], [binding], :indexed) do
    if scan_index_id(scan) == binding.index_id and scan_type([scan]) != "full_text",
      do: :ok,
      else: invalid_shape("single plans require one matching scan and binding")
  end

  defp validate_shape(:union, scans, bindings, :union)
       when is_list(scans) and is_list(bindings) do
    if length_at_least_two?(scans) and same_length?(scans, bindings) and
         matching_scan_bindings?(scans, bindings),
       do: :ok,
       else: invalid_shape("union plans require matching scan and binding pairs")
  end

  defp validate_shape(:bounded_scan, scans, [], :bounded_scan) do
    if scans == [] or Enum.all?(scans, &is_nil(scan_index_id(&1))),
      do: :ok,
      else: invalid_shape("bounded scans cannot contain index bindings")
  end

  defp validate_shape(:bounded_scan, _scans, _bindings, _pagination),
    do: invalid_shape("bounded scans require empty bindings and bounded pagination")

  defp validate_shape(:full_text, [scan], [binding], :full_text) do
    if scan_index_id(scan) == binding.index_id and scan_type([scan]) in [nil, "full_text"],
      do: :ok,
      else: invalid_shape("full-text plans require one matching scan and binding")
  end

  defp validate_shape(_kind, _scans, _bindings, _pagination),
    do: invalid_shape("query plan shape does not match its kind")

  defp matching_scan_bindings?(scans, bindings) do
    Enum.zip(scans, bindings)
    |> Enum.all?(fn {scan, binding} -> scan_index_id(scan) == binding.index_id end)
  end

  defp scan_index_id(scan), do: Map.get(scan, "index_id")

  defp scan_type(scans) when is_list(scans) do
    case List.first(scans) do
      nil -> nil
      scan -> Map.get(scan, "type")
    end
  end

  defp length_at_least_two?([_, _ | _]), do: true
  defp length_at_least_two?(_), do: false

  defp same_length?([], []), do: true
  defp same_length?([_ | left], [_ | right]), do: same_length?(left, right)
  defp same_length?(_left, _right), do: false

  defp invalid_shape(message), do: {:error, ElixirDB.Error.invalid_request(message)}
end
