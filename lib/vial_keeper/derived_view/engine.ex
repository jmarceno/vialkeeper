defmodule VialKeeper.DerivedView.Engine do
  @moduledoc """
  Shared derived-materialization semantics.

  Owns source-batch normalization, contribution diffs, grouping, reducers,
  numeric extremes, generated output mapping, rebuild generation checks, and
  cursor/history validation. Physical contribution and group persistence stay
  behind storage ports.
  """

  alias VialKeeper.DerivedView.Definition
  alias VialKeeper.JSON.{Canonical, StrictDecoder}
  alias VialKeeper.MapAccess
  alias VialKeeper.View.{KeyCodec, Number, NumericAccumulator}

  @default_batch_limit 500

  @type source_row :: %{
          required(:source_document_id) => binary(),
          required(:source_revision_id) => binary(),
          required(:key) => [term()],
          optional(:value) => term()
        }

  @type batch_request :: %{
          required(:materialization_id) => binary(),
          required(:source_database_uuid) => binary(),
          required(:source_history_epoch) => binary(),
          required(:expected_checkpoint_sequence) => non_neg_integer(),
          required(:through_sequence) => non_neg_integer(),
          required(:rows) => [source_row()],
          required(:removals) => [binary()]
        }

  @doc "Builds one storage-neutral source contribution row."
  @spec source_row(binary(), binary(), [term()]) :: source_row()
  def source_row(document_id, revision_id, key) do
    %{source_document_id: document_id, source_revision_id: revision_id, key: key}
  end

  @doc "Builds one storage-neutral source contribution row with a value."
  @spec source_row(binary(), binary(), [term()], term()) :: source_row()
  def source_row(document_id, revision_id, key, value) do
    source_row(document_id, revision_id, key)
    |> Map.put(:value, value)
  end

  @doc "Builds the storage-neutral request for one incremental source batch."
  @spec batch_request(
          binary(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [source_row()],
          [binary()]
        ) :: batch_request()
  def batch_request(
        materialization_id,
        source_database_uuid,
        source_history_epoch,
        expected_checkpoint_sequence,
        through_sequence,
        rows,
        removals
      ) do
    %{
      materialization_id: materialization_id,
      source_database_uuid: source_database_uuid,
      source_history_epoch: source_history_epoch,
      expected_checkpoint_sequence: expected_checkpoint_sequence,
      through_sequence: through_sequence,
      rows: rows,
      removals: removals
    }
  end

  @doc "Normalizes an incremental derived source batch."
  @spec normalize_batch(map(), map(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def normalize_batch(request, definition, source)
      when is_map(request) and is_map(definition) and is_map(source) do
    rows = MapAccess.get(request, :rows, [])
    removals = MapAccess.get(request, :removals, [])

    with {:ok, expected} <- required_non_negative(request, :expected_checkpoint_sequence),
         {:ok, through} <- required_non_negative(request, :through_sequence),
         {:ok, history_epoch} <- required_string(request, :source_history_epoch),
         {:ok, normalized_rows} <- normalize_rows(rows, definition),
         {:ok, normalized_removals} <- normalize_removals(removals),
         :ok <- validate_batch_size(normalized_rows, normalized_removals),
         :ok <- validate_disjoint(normalized_rows, normalized_removals) do
      {:ok,
       %{
         expected: expected,
         through: through,
         history_epoch: history_epoch,
         rows: normalized_rows,
         removals: normalized_removals,
         rebuild_generation: source.rebuild_generation
       }}
    end
  end

  @doc "Normalizes a rebuild page batch."
  @spec normalize_rebuild_batch(map(), map(), map()) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def normalize_rebuild_batch(request, definition, source)
      when is_map(request) and is_map(definition) and is_map(source) do
    rows = MapAccess.get(request, :rows, [])
    removals = MapAccess.get(request, :removals, [])

    with {:ok, normalized_rows} <- normalize_rows(rows, definition),
         {:ok, normalized_removals} <- normalize_removals(removals),
         :ok <- validate_batch_size(normalized_rows, normalized_removals),
         :ok <- validate_disjoint(normalized_rows, normalized_removals) do
      {:ok,
       %{
         rows: normalized_rows,
         removals: normalized_removals,
         rebuild_generation: source.rebuild_generation
       }}
    end
  end

  @doc "Validates checkpoint CAS for an incremental batch."
  @spec validate_batch_cursor(map(), integer(), integer()) ::
          {:ok, :apply | :idempotent} | {:error, VialKeeper.Error.t()}
  def validate_batch_cursor(source, expected, through)
      when is_map(source) and is_integer(expected) and is_integer(through) and through < expected do
    {:error, VialKeeper.Error.invalid_request("derived source checkpoint regressed")}
  end

  def validate_batch_cursor(source, _expected, through)
      when is_map(source) and is_integer(through) and source.checkpoint_sequence > through do
    {:ok, :idempotent}
  end

  def validate_batch_cursor(%{checkpoint_sequence: through} = source, _expected, through)
      when is_integer(through) and (through != 0 or not is_nil(source.history_epoch)) do
    {:ok, :idempotent}
  end

  def validate_batch_cursor(source, expected, through)
      when is_map(source) and is_integer(expected) and is_integer(through) and
             source.checkpoint_sequence != expected do
    {:error, VialKeeper.Error.revision_conflict("derived source checkpoint mismatch")}
  end

  def validate_batch_cursor(source, expected, through)
      when is_map(source) and is_integer(expected) and is_integer(through) do
    {:ok, :apply}
  end

  @doc "Validates that a source history epoch matches the stored checkpoint."
  @spec validate_history_epoch(map(), binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_history_epoch(%{history_epoch: nil}, _requested), do: :ok

  def validate_history_epoch(%{history_epoch: current}, requested) when current == requested,
    do: :ok

  def validate_history_epoch(%{checkpoint_sequence: 0}, _requested), do: :ok

  def validate_history_epoch(_source, requested),
    do:
      {:error,
       VialKeeper.Error.source_history_reset("source history epoch changed", %{
         source_history_epoch: requested
       })}

  @doc "Ensures a rebuild page targets the active rebuild generation."
  @spec validate_rebuild_source(map(), integer()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_rebuild_source(%{state: :rebuilding, rebuild_generation: generation}, generation),
    do: :ok

  def validate_rebuild_source(_source, _generation),
    do: {:error, VialKeeper.Error.revision_conflict("derived rebuild generation is not active")}

  @doc "Validates optional materialization id against metadata."
  @spec validate_materialization(map(), map()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_materialization(metadata, request) when is_map(metadata) and is_map(request) do
    case MapAccess.get(request, :materialization_id) do
      nil -> :ok
      value when value == metadata.materialization_id -> :ok
      _ -> {:error, VialKeeper.Error.revision_conflict("derived materialization id mismatch")}
    end
  end

  @doc "Builds map-only generated document put/delete changes."
  @spec map_output_changes(binary(), map(), map(), [binary()]) :: [tuple()]
  def map_output_changes(source_uuid, old_rows, row_map, removals)
      when is_binary(source_uuid) and is_map(old_rows) and is_map(row_map) and is_list(removals) do
    removed =
      removals
      |> Enum.filter(&Map.has_key?(old_rows, &1))
      |> Enum.map(&{:delete, map_document_id(source_uuid, &1)})

    updated =
      row_map
      |> Map.values()
      |> Enum.flat_map(fn row ->
        old = Map.get(old_rows, row.source_document_id)

        if contribution_equal?(old, row),
          do: [],
          else: [
            {:put, map_document_id(source_uuid, row.source_document_id), map_body(row, source_uuid)}
          ]
      end)

    Enum.sort_by(removed ++ updated, &output_change_sort_key/1)
  end

  @doc "Returns group keys affected by a contribution diff."
  @spec affected_groups(map(), map(), [binary()]) :: %{optional(binary()) => binary()}
  def affected_groups(old_rows, row_map, removals)
      when is_map(old_rows) and is_map(row_map) and is_list(removals) do
    old_groups =
      old_rows
      |> Map.values()
      |> Enum.reduce(%{}, &put_group/2)

    new_groups =
      row_map
      |> Map.values()
      |> Enum.reduce(%{}, &put_group/2)

    removals
    |> Enum.reduce(old_groups, fn document_id, groups ->
      case Map.get(old_rows, document_id) do
        nil -> groups
        row -> put_group(row, groups)
      end
    end)
    |> Map.merge(new_groups)
  end

  @doc "Sorts affected groups by group_key_sort."
  @spec sorted_groups(map()) :: [{binary(), binary()}]
  def sorted_groups(groups) when is_map(groups),
    do: Enum.sort_by(groups, fn {sort, _json} -> sort end)

  @doc "Applies contribution add/remove operations to a group aggregate."
  @spec update_group(map(), binary(), map(), map(), [binary()]) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def update_group(aggregate, group_sort, old_rows, row_map, removals)
      when is_map(aggregate) and is_binary(group_sort) do
    group_changes = group_changes(group_sort, old_rows, row_map, removals)

    Enum.reduce_while(group_changes, {:ok, aggregate}, fn {operation, row}, {:ok, aggregate} ->
      case apply_group_change(operation, row, aggregate) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  @doc "Builds an empty group aggregate."
  @spec empty_group(binary()) :: map()
  def empty_group(group_json) when is_binary(group_json) do
    %{
      group_key_json: group_json,
      count: 0,
      sum_units: 0,
      sumsqr_units: 0,
      min_value_json: nil,
      min_value_sort: nil,
      max_value_json: nil,
      max_value_sort: nil
    }
  end

  @doc "Computes numeric extrema from contribution value rows."
  @spec extrema_from_values([{term(), binary() | nil, binary(), binary()}]) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def extrema_from_values(values) when is_list(values) do
    numeric =
      values
      |> Enum.filter(fn {_value, sort, _source, _doc} -> is_binary(sort) end)
      |> Enum.sort_by(fn {_value, sort, source, doc} -> {sort, source, doc} end)

    case numeric do
      [] ->
        {:ok,
         %{
           min_value_json: nil,
           min_value_sort: nil,
           max_value_json: nil,
           max_value_sort: nil,
           min_value: nil,
           max_value: nil,
           numeric_count: 0
         }}

      _ ->
        {min_value, min_sort, _min_source, _min_doc} = hd(numeric)
        {max_value, max_sort, _max_source, _max_doc} = hd(Enum.reverse(numeric))

        with {:ok, min_float} <- numeric_value(min_value),
             {:ok, max_float} <- numeric_value(max_value),
             {:ok, min_json, encoded_min_sort} <- encoded_numeric(min_float),
             {:ok, max_json, encoded_max_sort} <- encoded_numeric(max_float),
             true <- encoded_min_sort == min_sort,
             true <- encoded_max_sort == max_sort do
          {:ok,
           %{
             min_value_json: min_json,
             min_value_sort: min_sort,
             max_value_json: max_json,
             max_value_sort: max_sort,
             min_value: min_float,
             max_value: max_float,
             numeric_count: length(numeric)
           }}
        else
          false ->
            {:error, VialKeeper.Error.integrity_violation("derived numeric sort is inconsistent")}

          {:error, _} = error ->
            error
        end
    end
  end

  @doc "Builds reducer output for a fully enriched aggregate."
  @spec reducer_output(atom(), map()) :: {:ok, term()} | {:error, VialKeeper.Error.t()}
  def reducer_output(:_count, aggregate), do: {:ok, aggregate.count}

  def reducer_output(:_sum, aggregate),
    do: NumericAccumulator.sum(aggregate_accumulator(aggregate))

  def reducer_output(:_min, %{min_value: value}), do: {:ok, value}
  def reducer_output(:_max, %{max_value: value}), do: {:ok, value}

  def reducer_output(:_stats, aggregate) do
    with {:ok, sum} <- NumericAccumulator.sum(aggregate_accumulator(aggregate)),
         {:ok, sumsqr} <- NumericAccumulator.sumsqr(aggregate_accumulator(aggregate)) do
      {:ok,
       %{
         "count" => aggregate.numeric_count,
         "sum" => sum,
         "min" => aggregate.min_value,
         "max" => aggregate.max_value,
         "sumsqr" => sumsqr
       }}
    end
  end

  @doc "Builds the public group document body."
  @spec group_body(list(), term()) :: map()
  def group_body(group_key, output), do: %{"key" => group_key, "value" => output}

  @doc "Builds a map-only generated document body."
  @spec map_body(map(), binary()) :: map()
  def map_body(row, source_uuid) when is_map(row) and is_binary(source_uuid) do
    body = %{
      "key" => row.key,
      "source_database_uuid" => source_uuid,
      "source_document_id" => row.source_document_id
    }

    if row.value_present, do: Map.put(body, "value", row.value), else: body
  end

  @doc "Deterministic document id for a map contribution."
  @spec map_document_id(binary(), binary()) :: binary()
  def map_document_id(source_uuid, source_document_id)
      when is_binary(source_uuid) and is_binary(source_document_id) do
    "m-" <> digest_json([source_uuid, source_document_id])
  end

  @doc "Deterministic document id for a group aggregate."
  @spec group_document_id(list()) :: {:ok, binary()}
  def group_document_id(group_key) when is_list(group_key),
    do: {:ok, "g-" <> digest_json(group_key)}

  @doc "Status string after enable/disable."
  @spec enabled_status(map(), boolean()) :: binary()
  def enabled_status(_metadata, false), do: "disabled"
  def enabled_status(%{enabled: true, status: status}, true), do: Atom.to_string(status)
  def enabled_status(_metadata, true), do: "rebuilding"

  @doc "Returns whether two contributions are identical for output purposes."
  @spec contribution_equal?(map() | nil, map()) :: boolean()
  def contribution_equal?(nil, _row), do: false

  def contribution_equal?(old, row) do
    old.source_revision_id == row.source_revision_id and old.key_json == row.key_json and
      old.group_key_sort == row.group_key_sort and old.value_json == row.value_json
  end

  defp normalize_rows(rows, definition) when is_list(rows) do
    Enum.reduce_while(rows, {:ok, MapSet.new(), []}, fn row, {:ok, seen, acc} ->
      with {:ok, normalized} <- normalize_row(row, definition),
           false <- MapSet.member?(seen, normalized.source_document_id) do
        {:cont, {:ok, MapSet.put(seen, normalized.source_document_id), [normalized | acc]}}
      else
        true ->
          {:halt,
           {:error,
            VialKeeper.Error.invalid_request("derived source batch contains duplicate documents")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> reverse_normalized_rows()
  end

  defp normalize_rows(_rows, _definition),
    do: {:error, VialKeeper.Error.invalid_request("derived source rows must be an array")}

  defp reverse_normalized_rows({:ok, _seen, rows}), do: {:ok, Enum.reverse(rows)}
  defp reverse_normalized_rows({:error, _} = error), do: error

  defp normalize_row(row, definition) when is_map(row) do
    with {:ok, document_id} <- required_string(row, :source_document_id),
         {:ok, revision_id} <- required_string(row, :source_revision_id),
         {:ok, key} <- required_list(row, :key),
         {:ok, key_json} <- Canonical.encode(key),
         {:ok, key_sort} <- KeyCodec.encode(key),
         {:ok, {group_key, group_json, group_sort}} <- group_fields(definition, key),
         {:ok, {value, value_json, value_sort, value_present}} <- value_fields(row) do
      {:ok,
       %{
         source_document_id: document_id,
         source_revision_id: revision_id,
         key: key,
         key_json: key_json,
         key_sort: key_sort,
         group_key: group_key,
         group_key_json: group_json,
         group_key_sort: group_sort,
         value: value,
         value_json: value_json,
         value_sort: value_sort,
         value_present: value_present
       }}
    end
  end

  defp normalize_row(_row, _definition),
    do: {:error, VialKeeper.Error.invalid_request("derived source row must be an object")}

  defp group_fields(%{reducer: nil}, _key), do: {:ok, {nil, nil, nil}}

  defp group_fields(%{group_level: level}, key) do
    group_key = Enum.take(key, level)

    with {:ok, group_sort} <- KeyCodec.encode(group_key),
         {:ok, group_json} <- Canonical.encode(group_key) do
      {:ok, {group_key, group_json, group_sort}}
    end
  end

  defp value_fields(row) do
    if value_present?(row) do
      value = MapAccess.get(row, :value)

      with {:ok, value_json} <- Canonical.encode(value),
           {:ok, value_sort} <- numeric_value_sort(value) do
        {:ok, {value, value_json, value_sort, true}}
      end
    else
      {:ok, {nil, nil, nil, false}}
    end
  end

  defp value_present?(row), do: Map.has_key?(row, :value) or Map.has_key?(row, "value")

  defp numeric_value_sort(value) when is_number(value) do
    case KeyCodec.encode([value]) do
      {:ok, sort} -> {:ok, sort}
      {:error, _} -> {:error, VialKeeper.Error.resource_limit("numeric value is not representable")}
    end
  end

  defp numeric_value_sort(_value), do: {:ok, nil}

  defp normalize_removals(removals) when is_list(removals) do
    Enum.reduce_while(removals, {:ok, MapSet.new(), []}, fn document_id, {:ok, seen, acc} ->
      with {:ok, normalized} <- validate_string(document_id, "source document id"),
           false <- MapSet.member?(seen, normalized) do
        {:cont, {:ok, MapSet.put(seen, normalized), [normalized | acc]}}
      else
        true ->
          {:halt,
           {:error,
            VialKeeper.Error.invalid_request("derived source batch contains duplicate removals")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, _seen, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = error -> error
    end
  end

  defp normalize_removals(_removals),
    do: {:error, VialKeeper.Error.invalid_request("derived source removals must be an array")}

  defp validate_batch_size(rows, removals) do
    maximum =
      VialKeeper.Config.host_limits()[:max_materialized_view_batch_documents] ||
        @default_batch_limit

    if length(rows) + length(removals) <= maximum,
      do: :ok,
      else: {:error, VialKeeper.Error.resource_limit("derived source batch exceeds the host limit")}
  end

  defp validate_disjoint(rows, removals) do
    row_ids = MapSet.new(rows, & &1.source_document_id)
    removal_ids = MapSet.new(removals)

    if MapSet.disjoint?(row_ids, removal_ids),
      do: :ok,
      else: {:error, VialKeeper.Error.invalid_request("derived source rows and removals overlap")}
  end

  defp put_group(%{group_key_sort: nil}, groups), do: groups

  defp put_group(%{group_key_sort: sort, group_key_json: json}, groups),
    do: Map.put(groups, sort, json)

  defp output_change_sort_key({:delete, document_id}), do: {document_id, :delete}
  defp output_change_sort_key({:put, document_id, _body}), do: {document_id, :put}

  defp group_changes(group_sort, old_rows, row_map, removals) do
    removed =
      removals
      |> Enum.flat_map(&removed_group_changes(old_rows, group_sort, &1))

    replaced_or_added =
      row_map
      |> Enum.sort_by(fn {document_id, _row} -> document_id end)
      |> Enum.flat_map(&replacement_group_changes(old_rows, group_sort, &1))

    removed ++ replaced_or_added
  end

  defp removed_group_changes(old_rows, group_sort, document_id) do
    case Map.get(old_rows, document_id) do
      nil -> []
      row -> group_change(group_sort, :remove, row)
    end
  end

  defp replacement_group_changes(old_rows, group_sort, {document_id, row}) do
    case Map.get(old_rows, document_id) do
      old when is_map(old) -> changed_group_changes(old, row, group_sort)
      nil -> group_change(group_sort, :add, row)
    end
  end

  defp changed_group_changes(old, row, group_sort) do
    if contribution_equal?(old, row),
      do: [],
      else: group_change(group_sort, :remove, old) ++ group_change(group_sort, :add, row)
  end

  defp group_change(group_sort, operation, %{group_key_sort: row_group_sort} = row) do
    if row_group_sort == group_sort, do: [{operation, row}], else: []
  end

  defp group_change(_group_sort, _operation, _row), do: []

  defp apply_group_change(:remove, row, %{count: count} = aggregate) when count > 0 do
    with {:ok, accumulator} <- remove_numeric(aggregate_accumulator(aggregate), row.value) do
      {:ok,
       aggregate
       |> Map.put(:count, count - 1)
       |> put_accumulator(accumulator)}
    end
  end

  defp apply_group_change(:remove, _row, _aggregate),
    do: {:error, VialKeeper.Error.integrity_violation("derived group count underflow")}

  defp apply_group_change(:add, row, aggregate) do
    with {:ok, accumulator} <- add_numeric(aggregate_accumulator(aggregate), row.value) do
      {:ok,
       aggregate
       |> Map.update!(:count, &(&1 + 1))
       |> put_accumulator(accumulator)}
    end
  end

  defp aggregate_accumulator(aggregate),
    do: %NumericAccumulator{sum_units: aggregate.sum_units, sumsqr_units: aggregate.sumsqr_units}

  defp put_accumulator(aggregate, accumulator),
    do:
      Map.merge(aggregate, %{
        sum_units: accumulator.sum_units,
        sumsqr_units: accumulator.sumsqr_units
      })

  defp add_numeric(accumulator, value) when is_number(value),
    do: NumericAccumulator.add(accumulator, value)

  defp add_numeric(accumulator, _value), do: {:ok, accumulator}

  defp remove_numeric(accumulator, value) when is_number(value),
    do: NumericAccumulator.remove(accumulator, value)

  defp remove_numeric(accumulator, _value), do: {:ok, accumulator}

  defp encoded_numeric(value) do
    with {:ok, float} <- numeric_value(value),
         {:ok, json} <- Canonical.encode(float),
         {:ok, sort} <- KeyCodec.encode([float]) do
      {:ok, json, sort}
    end
  end

  defp numeric_value(value) do
    case Number.to_binary64(value) do
      {:ok, float} -> {:ok, float}
      :overflow -> {:error, VialKeeper.Error.resource_limit("numeric value is not representable")}
    end
  end

  defp digest_json(value) do
    value
    |> Canonical.encode!()
    |> then(fn canonical -> :crypto.hash(:sha256, canonical) end)
    |> Base.encode16(case: :lower)
  end

  @doc "Decodes a stored derived definition JSON payload."
  @spec decode_definition(binary()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def decode_definition(json) when is_binary(json) do
    with {:ok, decoded} <- StrictDecoder.decode(json),
         {:ok, normalized} <- Definition.normalize(decoded, enforce_host_limits: false) do
      {:ok, normalized}
    else
      {:error, _} ->
        {:error, VialKeeper.Error.integrity_violation("derived view definition is invalid")}
    end
  end

  def decode_definition(_),
    do: {:error, VialKeeper.Error.integrity_violation("derived view definition is missing")}

  @doc "Loads metadata plus definition for a derived request."
  @spec load_context(map(), map(), (map() -> {:ok, map()} | {:error, VialKeeper.Error.t()})) ::
          {:ok, map()} | {:error, VialKeeper.Error.t()}
  def load_context(container, request, fetch_metadata_fun)
      when is_map(request) and is_function(fetch_metadata_fun, 1) do
    with {:ok, metadata} <- fetch_metadata_fun.(container),
         :ok <- validate_materialization(metadata, request),
         {:ok, definition} <- decode_definition(metadata.definition_json) do
      {:ok, %{metadata: metadata, definition: definition}}
    end
  end

  @doc "Extracts a source database UUID from seed metadata."
  @spec source_uuid(term()) :: binary() | nil
  def source_uuid(source) when is_binary(source), do: source
  def source_uuid(%{database_uuid: uuid}), do: uuid
  def source_uuid(%{"database_uuid" => uuid}), do: uuid
  def source_uuid(_), do: nil

  @doc "Requires a non-empty string field."
  @spec required_string(map(), atom()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def required_string(map, key) when is_map(map) and is_atom(key) do
    case MapAccess.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a non-empty string")}
    end
  end

  @doc "Requires a non-negative integer field."
  @spec required_non_negative(map(), atom()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def required_non_negative(map, key) when is_map(map) and is_atom(key) do
    case MapAccess.get(map, key) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a non-negative integer")}
    end
  end

  @doc "Requires a positive integer field."
  @spec required_positive(map(), atom()) :: {:ok, pos_integer()} | {:error, VialKeeper.Error.t()}
  def required_positive(map, key) when is_map(map) and is_atom(key) do
    case MapAccess.get(map, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a positive integer")}
    end
  end

  @doc "Requires a boolean field."
  @spec required_boolean(map(), atom()) :: {:ok, boolean()} | {:error, VialKeeper.Error.t()}
  def required_boolean(map, key) when is_map(map) and is_atom(key) do
    case MapAccess.get(map, key) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a boolean")}
    end
  end

  @doc "Requires a UUID string field."
  @spec required_uuid(map(), atom()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def required_uuid(map, key) when is_map(map) and is_atom(key) do
    with {:ok, value} <- required_string(map, key) do
      if uuid?(value),
        do: {:ok, String.downcase(value)},
        else: {:error, VialKeeper.Error.invalid_request("#{key} must be a UUID")}
    end
  end

  @doc "Reads an optional non-empty string field."
  @spec optional_string(map(), atom()) :: {:ok, binary() | nil} | {:error, VialKeeper.Error.t()}
  def optional_string(map, key) when is_map(map) and is_atom(key) do
    case MapAccess.get(map, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a non-empty string")}
    end
  end

  @doc "Reads an optional non-negative integer, falling back to `default`."
  @spec optional_non_negative(map(), atom(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, VialKeeper.Error.t()}
  def optional_non_negative(map, key, default)
      when is_map(map) and is_atom(key) and is_integer(default) and default >= 0 do
    case MapAccess.get(map, key) do
      nil -> {:ok, default}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be a non-negative integer")}
    end
  end

  @doc "Normalizes a rebuild page limit against host batch ceilings."
  @spec rebuild_page_limit(map()) :: {:ok, pos_integer()} | {:error, VialKeeper.Error.t()}
  def rebuild_page_limit(request) when is_map(request) do
    limit = MapAccess.get(request, :limit, @default_batch_limit)

    maximum =
      VialKeeper.Config.host_limits()[:max_materialized_view_batch_documents] ||
        @default_batch_limit

    cond do
      not is_integer(limit) or limit <= 0 ->
        {:error, VialKeeper.Error.invalid_request("derived rebuild page limit must be positive")}

      limit > maximum ->
        {:error,
         VialKeeper.Error.resource_limit("derived rebuild page limit exceeds the host limit")}

      true ->
        {:ok, limit}
    end
  end

  @doc "Maps a stored derived status string to a public atom."
  @spec status_atom(term()) :: atom()
  def status_atom("disabled"), do: :disabled
  def status_atom("rebuilding"), do: :rebuilding
  def status_atom("current"), do: :current
  def status_atom("stale"), do: :stale
  def status_atom("resource_limit"), do: :resource_limit
  def status_atom(atom) when is_atom(atom), do: atom
  def status_atom(_), do: :unknown

  @doc "Optional derived fault-injection hook used by storage contract tests."
  @spec derived_fault_check(term(), atom()) :: :ok | {:error, VialKeeper.Error.t()}
  def derived_fault_check(nil, _point), do: :ok

  def derived_fault_check(fun, point) when is_function(fun, 1) do
    case fun.(point) do
      :ok -> :ok
      {:error, %VialKeeper.Error{}} = error -> error
      other -> {:error, VialKeeper.Error.internal_error("derived fault returned #{inspect(other)}")}
    end
  end

  def derived_fault_check(_fun, _point), do: :ok

  defp uuid?(value) do
    Regex.match?(
      ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
      value
    )
  end

  defp required_list(map, key) do
    case MapAccess.get(map, key) do
      value when is_list(value) -> {:ok, value}
      _ -> {:error, VialKeeper.Error.invalid_request("#{key} must be an array")}
    end
  end

  defp validate_string(value, _label) when is_binary(value) and value != "", do: {:ok, value}

  defp validate_string(_value, label),
    do: {:error, VialKeeper.Error.invalid_request("#{label} must be a non-empty string")}
end
