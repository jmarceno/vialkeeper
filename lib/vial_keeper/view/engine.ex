defmodule VialKeeper.View.Engine do
  @moduledoc """
  Shared local-view semantics for definition CAS, rebuild generations, query
  planning, bookmarks, grouping, and result shaping.

  Physical row/range persistence stays behind storage ports.
  """

  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.MapAccess
  alias VialKeeper.View.{BookmarkCodec, Definition, KeyCodec, Reducer}

  @default_query_limit 100
  @default_query_max_limit 500
  @query_fields ~w(view_id consistency key start_key end_key inclusive_end group_level limit bookmark)

  @type query_plan :: %{
          reducer: term(),
          key_length: non_neg_integer(),
          group_level: non_neg_integer(),
          start_sort: binary() | nil,
          end_sort: binary() | nil,
          inclusive_end: boolean(),
          bookmark: binary() | nil,
          bookmark_after: binary() | nil
        }

  @doc "Validates that a view query only contains known fields."
  @spec validate_query_fields(map()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_query_fields(request) when is_map(request) do
    allowed = @query_fields ++ Enum.map(@query_fields, &String.to_atom/1)

    if Enum.all?(Map.keys(request), &(&1 in allowed)),
      do: :ok,
      else: {:error, VialKeeper.Error.invalid_request("view query contains an unknown field")}
  end

  @doc "Extracts a required view_id from a query request."
  @spec query_view_id(map()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def query_view_id(request) when is_map(request) do
    case fetch_query_option(request, :view_id) do
      {:ok, view_id} when is_binary(view_id) and view_id != "" -> {:ok, view_id}
      _ -> {:error, VialKeeper.Error.invalid_request("view query requires a view_id")}
    end
  end

  @doc "Normalizes the query page limit against host config."
  @spec normalize_query_limit(map(), map()) :: {:ok, pos_integer()} | {:error, VialKeeper.Error.t()}
  def normalize_query_limit(request, config) when is_map(request) and is_map(config) do
    maximum = get_in(config, ["queries", "max_limit"]) || @default_query_max_limit

    case fetch_query_option(request, :limit) do
      :missing -> {:ok, @default_query_limit}
      {:ok, value} -> clamp_query_limit(value, maximum)
    end
  end

  defp clamp_query_limit(value, maximum)
       when is_integer(value) and value > 0 and value <= maximum,
       do: {:ok, value}

  defp clamp_query_limit(value, maximum) when is_integer(value) and value > maximum,
    do: {:error, VialKeeper.Error.resource_limit("view query limit exceeds the configured maximum")}

  defp clamp_query_limit(_value, _maximum),
    do: {:error, VialKeeper.Error.invalid_request("view query limit must be a positive integer")}

  @doc "Validates query options such as consistency, key bounds, and bookmarks."
  @spec validate_query_options(map()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_query_options(request) when is_map(request) do
    with :ok <-
           validate_query_option(
             request,
             :consistency,
             &(&1 in ["stale_ok", "update_after", "consistent"]),
             "view consistency is invalid"
           ),
         :ok <-
           validate_query_option(request, :key, &is_list/1, "view query key must be an array"),
         :ok <- validate_exact_key(request),
         :ok <-
           validate_query_option(
             request,
             :start_key,
             &(is_list(&1) or is_nil(&1)),
             "view query start_key must be an array"
           ),
         :ok <-
           validate_query_option(
             request,
             :end_key,
             &(is_list(&1) or is_nil(&1)),
             "view query end_key must be an array"
           ),
         :ok <-
           validate_query_option(
             request,
             :inclusive_end,
             &is_boolean/1,
             "view query inclusive_end must be a boolean"
           ),
         :ok <-
           validate_query_option(
             request,
             :group_level,
             &(is_integer(&1) and &1 >= 0),
             "view query group_level must be a non-negative integer"
           ) do
      validate_query_option(
        request,
        :bookmark,
        &(is_binary(&1) or is_nil(&1)),
        "view query bookmark must be a string"
      )
    end
  end

  @doc "Builds a backend-neutral view query plan."
  @spec build_query_plan(map(), map(), map()) ::
          {:ok, query_plan()} | {:error, VialKeeper.Error.t()}
  def build_query_plan(request, definition, state)
      when is_map(request) and is_map(definition) and is_map(state) do
    key_length = length(definition.key)
    group_level = MapAccess.get(request, :group_level)
    bookmark = MapAccess.get(request, :bookmark)
    group_level = normalize_group_level(group_level, key_length, definition.reducer)
    exact_key = MapAccess.get(request, :key)
    start_key = exact_key || MapAccess.get(request, :start_key)
    end_key = exact_key || MapAccess.get(request, :end_key)
    inclusive_end = MapAccess.get(request, :inclusive_end, true) == true

    with {:ok, start_sort} <- encode_bound(start_key),
         {:ok, end_sort} <- encode_bound(end_key),
         {:ok, bookmark_plan} <- decode_bookmark(bookmark, definition, state) do
      {:ok,
       %{
         reducer: definition.reducer,
         key_length: key_length,
         group_level: group_level,
         start_sort: bookmark_plan[:start_sort] || start_sort,
         end_sort: end_sort,
         inclusive_end: inclusive_end,
         bookmark: bookmark,
         bookmark_after: bookmark_plan[:after]
       }}
    end
  end

  @doc "Formats fetched rows into the public query envelope."
  @spec format_query_result([map()], boolean() | nil, map(), map(), query_plan(), pos_integer()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def format_query_result(rows, fetch_has_more, definition, state, plan, limit)
      when is_list(rows) and is_map(definition) and is_map(state) and is_map(plan) and
             is_integer(limit) do
    mapped_rows =
      Enum.map(rows, fn row ->
        %{key: row.key, value: row.value, id: row.id, key_sort: row.key_sort}
      end)

    with {:ok, results} <- grouped_results(mapped_rows, definition, plan) do
      {page, group_has_more?} = paginate_results(results, definition.reducer, limit)
      has_more? = group_has_more? || fetch_has_more == true

      {:ok,
       %{
         results: Enum.map(page, &public_result/1),
         bookmark: query_bookmark(page, has_more?, definition, state),
         indexed_through: state.indexed_through,
         definition_digest: definition.definition_digest
       }}
    end
  end

  @doc "Decides whether a view batch should apply, no-op, or conflict."
  @spec validate_batch_cas(map(), integer(), integer()) ::
          {:ok, :apply | :idempotent} | {:error, VialKeeper.Error.t()}
  def validate_batch_cas(state, expected, through)
      when is_map(state) and is_integer(expected) and is_integer(through) do
    cond do
      state.indexed_through >= through ->
        {:ok, :idempotent}

      state.indexed_through != expected ->
        {:error, VialKeeper.Error.revision_conflict("view cursor mismatch")}

      through < expected ->
        {:error, VialKeeper.Error.invalid_request("view through_sequence regressed")}

      true ->
        {:ok, :apply}
    end
  end

  @doc "Ensures a rebuild page targets the active building generation."
  @spec ensure_building_generation(map(), integer()) :: :ok | {:error, VialKeeper.Error.t()}
  def ensure_building_generation(%{building_generation: generation}, generation), do: :ok

  def ensure_building_generation(_state, _generation),
    do: {:error, VialKeeper.Error.invalid_request("view rebuild generation mismatch")}

  @doc "Checks whether creating a definition conflicts with an existing name."
  @spec ensure_no_conflict(map() | nil, map()) :: :ok | {:error, VialKeeper.Error.t()}
  def ensure_no_conflict(nil, _normalized), do: :ok

  def ensure_no_conflict(%{definition_digest: digest}, %{definition_digest: digest}), do: :ok

  def ensure_no_conflict(%{view_id: view_id, definition_digest: _digest}, _normalized),
    do:
      {:error,
       VialKeeper.Error.view_name_conflict("view name is already used by another definition", %{
         view_id: view_id
       })}

  @doc "Returns whether a fetch should include a SQL/memory limit (+1 probe)."
  @spec fetch_limit(map(), pos_integer()) :: pos_integer() | nil
  def fetch_limit(%{reducer: reducer}, _limit) when not is_nil(reducer), do: nil
  def fetch_limit(%{reducer: nil}, limit) when is_integer(limit) and limit > 0, do: limit + 1

  @doc "Splits a limited physical fetch into page rows and has_more."
  @spec split_fetch([map()], pos_integer() | nil, pos_integer()) :: {[map()], boolean() | nil}
  def split_fetch(rows, nil, _limit), do: {rows, nil}

  def split_fetch(rows, _fetch_limit, limit) when is_list(rows) and is_integer(limit) do
    {Enum.take(rows, limit), length(rows) > limit}
  end

  @doc "Returns true when a row's key_sort is within the query plan bounds."
  @spec row_in_range?(map(), query_plan()) :: boolean()
  def row_in_range?(row, plan) when is_map(row) and is_map(plan) do
    key_sort = Map.fetch!(row, :key_sort)
    document_id = Map.get(row, :id) || MapAccess.get(row, :document_id)

    after_start?(key_sort, document_id, plan) and before_end?(key_sort, plan)
  end

  defp after_start?(_key_sort, _document_id, %{start_sort: nil}), do: true

  defp after_start?(key_sort, document_id, %{start_sort: start_sort, bookmark_after: after_id})
       when is_binary(start_sort) and is_binary(after_id) do
    if key_sort == start_sort, do: document_id > after_id, else: key_sort > start_sort
  end

  defp after_start?(key_sort, _document_id, %{start_sort: start_sort}) when is_binary(start_sort),
    do: key_sort >= start_sort

  defp before_end?(_key_sort, %{end_sort: nil}), do: true

  defp before_end?(key_sort, %{end_sort: end_sort, inclusive_end: true}) when is_binary(end_sort),
    do: key_sort <= end_sort

  defp before_end?(key_sort, %{end_sort: end_sort}) when is_binary(end_sort),
    do: key_sort < end_sort

  @doc "Sorts view rows by key_sort then document id."
  @spec sort_rows([map()]) :: [map()]
  def sort_rows(rows) when is_list(rows) do
    Enum.sort_by(rows, fn row ->
      {Map.fetch!(row, :key_sort), Map.get(row, :id) || MapAccess.get(row, :document_id) || ""}
    end)
  end

  defp validate_exact_key(request) do
    case fetch_query_option(request, :key) do
      :missing ->
        :ok

      {:ok, _key} ->
        if query_option_present?(request, :start_key) or
             query_option_present?(request, :end_key) or
             fetch_query_option(request, :inclusive_end) == {:ok, false} do
          {:error,
           VialKeeper.Error.invalid_request("view query key cannot be combined with range options")}
        else
          :ok
        end
    end
  end

  defp query_option_present?(request, key) do
    Map.has_key?(request, key) or Map.has_key?(request, Atom.to_string(key))
  end

  defp validate_query_option(request, key, predicate, message) do
    case fetch_query_option(request, key) do
      :missing ->
        :ok

      {:ok, value} ->
        if predicate.(value), do: :ok, else: {:error, VialKeeper.Error.invalid_request(message)}
    end
  end

  defp fetch_query_option(request, key) do
    case Map.fetch(request, key) do
      {:ok, value} ->
        {:ok, value}

      :error ->
        case Map.fetch(request, Atom.to_string(key)) do
          {:ok, value} -> {:ok, value}
          :error -> :missing
        end
    end
  end

  defp normalize_group_level(nil, key_length, nil), do: key_length
  defp normalize_group_level(level, _key_length, nil) when is_integer(level), do: level
  defp normalize_group_level(level, _key_length, _reducer) when is_integer(level), do: level
  defp normalize_group_level(_level, key_length, _reducer), do: key_length

  defp decode_bookmark(nil, _definition, _state), do: {:ok, %{}}

  defp decode_bookmark(bookmark, definition, state) do
    expected = %{
      "definition_digest" => definition.definition_digest,
      "indexed_through" => state.indexed_through
    }

    with {:ok, decoded} <- BookmarkCodec.decode(bookmark, expected),
         {:ok, key_sort} <- decode_key_sort(decoded["key_sort"]) do
      {:ok, %{start_sort: key_sort, after: decoded["document_id"]}}
    else
      {:error, %VialKeeper.Error{code: :invalid_bookmark}} ->
        {:error, VialKeeper.Error.bookmark_stale("view bookmark is stale")}

      {:error, _} = error ->
        error
    end
  end

  defp encode_bound(nil), do: {:ok, nil}

  defp encode_bound(key) when is_list(key) do
    case KeyCodec.encode(key) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _} = error -> error
    end
  end

  defp encode_bound(_),
    do: {:error, VialKeeper.Error.invalid_request("view query key bound is invalid")}

  defp decode_key_sort(encoded) when is_binary(encoded) do
    case Base.decode64(encoded, padding: false) do
      {:ok, key_sort} -> {:ok, key_sort}
      :error -> {:error, VialKeeper.Error.invalid_bookmark("view bookmark key_sort is invalid")}
    end
  end

  defp grouped_results(mapped_rows, %{reducer: nil}, _plan),
    do: Reducer.reduce_rows(mapped_rows, nil)

  defp grouped_results(mapped_rows, %{reducer: reducer}, plan),
    do: Reducer.reduce_grouped(mapped_rows, reducer, plan.group_level, plan.key_length)

  defp paginate_results(results, reducer, limit) when not is_nil(reducer) do
    page = Enum.take(results, limit)
    {page, length(results) > limit}
  end

  defp paginate_results(results, nil, _limit), do: {results, false}

  defp query_bookmark(_page, false, _definition, _state), do: nil

  defp query_bookmark([], true, _definition, _state), do: nil

  defp query_bookmark(page, true, definition, state) do
    encode_query_bookmark(hd(Enum.reverse(page)), definition, state)
  end

  defp encode_query_bookmark(nil, _definition, _state), do: nil

  defp encode_query_bookmark(last, definition, state) do
    document_id = Map.get(last, :id) || last_document_id(last)

    case BookmarkCodec.encode(%{
           "definition_digest" => definition.definition_digest,
           "indexed_through" => state.indexed_through,
           "key_sort" => Base.encode64(last.key_sort, padding: false),
           "document_id" => document_id || ""
         }) do
      {:ok, encoded} -> encoded
      _ -> nil
    end
  end

  defp last_document_id(%{ids: []}), do: nil
  defp last_document_id(%{ids: ids}) when is_list(ids), do: hd(Enum.reverse(ids))
  defp last_document_id(_), do: nil

  defp public_result(%{key: key, value: value} = row) do
    base = %{"key" => key, "value" => value}
    if Map.has_key?(row, :id) and row.id, do: Map.put(base, "id", row.id), else: base
  end

  @doc "Decodes and normalizes a stored local-view definition JSON payload."
  @spec decode_definition(binary()) :: {:ok, map()} | {:error, term()}
  def decode_definition(json) when is_binary(json) do
    case StrictDecoder.decode(json) do
      {:ok, decoded} -> Definition.normalize(decoded)
      {:error, _} = error -> error
    end
  end

  @doc "Runs an optional view fault injector."
  @spec view_fault_check(term(), atom()) :: :ok | {:error, VialKeeper.Error.t()}
  def view_fault_check(nil, _point), do: :ok

  def view_fault_check(fun, point) when is_function(fun, 1) do
    case fun.(point) do
      :ok -> :ok
      {:error, %VialKeeper.Error{} = error} -> {:error, error}
    end
  end
end
