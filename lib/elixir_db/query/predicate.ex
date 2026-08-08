defmodule ElixirDB.Query.Predicate do
  @moduledoc """
  Canonical, storage-neutral query predicate values.

  This module deliberately has no knowledge of HTTP, indexes, SQLite, or
  database processes.  It is the semantic vocabulary shared by query
  normalization and later query execution waves.
  """

  alias ElixirDB.Query.Regex, as: QueryRegex

  @type json_value ::
          nil
          | boolean()
          | number()
          | binary()
          | [json_value()]
          | %{optional(binary()) => json_value()}

  @type scalar :: nil | boolean() | number() | binary()

  @type field_predicate ::
          {:eq, json_value()}
          | {:ne, json_value()}
          | {:gt, scalar()}
          | {:gte, scalar()}
          | {:lt, scalar()}
          | {:lte, scalar()}
          | {:in, [scalar()]}
          | {:nin, [scalar()]}
          | {:exists, boolean()}
          | {:type, :null | :boolean | :number | :string | :array | :object}
          | {:begins_with, binary()}
          | {:regex, QueryRegex.t()}
          | {:all, [scalar()]}
          | {:elem_match, t()}
          | {:size, non_neg_integer()}
          | {:mod, integer(), integer()}

  @type t ::
          :match_all
          | {:and, [t()]}
          | {:or, [t()]}
          | {:not, t()}
          | {:field, binary(), [field_predicate()]}

  @type comparison :: :lt | :eq | :gt

  @spec json_type(json_value()) ::
          :null | :boolean | :number | :string | :array | :object
  def json_type(nil), do: :null
  def json_type(value) when is_boolean(value), do: :boolean
  def json_type(value) when is_number(value), do: :number
  def json_type(value) when is_binary(value), do: :string
  def json_type(value) when is_list(value), do: :array
  def json_type(value) when is_map(value), do: :object

  @doc "Alias for `json_type/1`."
  @spec type(json_value()) :: atom()
  def type(value), do: json_type(value)

  @spec scalar?(term()) :: boolean()
  def scalar?(value), do: json_type(value) in [:null, :boolean, :number, :string]

  @spec exact_equal?(json_value(), json_value()) :: boolean()
  def exact_equal?(left, right) when is_number(left) and is_number(right), do: left == right
  def exact_equal?(left, right) when is_boolean(left) or is_boolean(right), do: left === right
  def exact_equal?(nil, nil), do: true
  def exact_equal?(left, right) when is_binary(left) and is_binary(right), do: left == right

  def exact_equal?(left, right) when is_list(left) and is_list(right) do
    length(left) == length(right) and
      Enum.zip(left, right) |> Enum.all?(fn {a, b} -> exact_equal?(a, b) end)
  end

  def exact_equal?(left, right) when is_map(left) and is_map(right) do
    Map.keys(left) == Map.keys(right) and
      Enum.all?(left, fn {key, value} -> exact_equal?(value, Map.fetch!(right, key)) end)
  end

  def exact_equal?(_left, _right), do: false

  @doc "Alias for `exact_equal?/2`."
  @spec equal?(json_value(), json_value()) :: boolean()
  def equal?(left, right), do: exact_equal?(left, right)

  @doc "Explicitly named alias for exact JSON equality."
  @spec exact_json_equal?(json_value(), json_value()) :: boolean()
  def exact_json_equal?(left, right), do: exact_equal?(left, right)

  @doc """
  Compare two scalar values when their JSON types support ordering.

  `:incomparable` represents either a type mismatch or a non-scalar value.
  """
  @spec ordered_compare(scalar(), scalar()) :: comparison() | :incomparable
  def ordered_compare(left, right) when is_number(left) and is_number(right),
    do: compare_values(left, right)

  def ordered_compare(left, right) when is_binary(left) and is_binary(right),
    do: compare_values(left, right)

  def ordered_compare(_left, _right), do: :incomparable

  @doc "Alias for `ordered_compare/2`."
  @spec compare(scalar(), scalar()) :: comparison() | :incomparable
  def compare(left, right), do: ordered_compare(left, right)

  @doc "Alias for `ordered_compare/2`."
  @spec compare_ordered(scalar(), scalar()) :: comparison() | :incomparable
  def compare_ordered(left, right), do: ordered_compare(left, right)

  @spec same_type?(json_value(), json_value()) :: boolean()
  def same_type?(left, right), do: json_type(left) == json_type(right)

  @doc "Return a deterministic, storage-neutral representation for explain/tests."
  @spec render(t()) :: map()
  def render(:match_all), do: %{"$matchAll" => true}

  def render({:and, children}),
    do: %{"$and" => Enum.map(children, &render/1)}

  def render({:or, children}),
    do: %{"$or" => Enum.map(children, &render/1)}

  def render({:not, child}), do: %{"$not" => render(child)}

  def render({:field, path, predicates}) do
    %{
      "$field" => path,
      "predicates" => Enum.map(predicates, &render_field_predicate/1)
    }
  end

  @doc "Alias for `render/1`."
  @spec canonical(t()) :: map()
  def canonical(predicate), do: render(predicate)

  @doc "Explicitly named alias for canonical predicate rendering."
  @spec canonical_render(t()) :: map()
  def canonical_render(predicate), do: render(predicate)

  @doc "Return the number of predicate and field-predicate nodes."
  @spec node_count(t()) :: pos_integer()
  def node_count(:match_all), do: 1
  def node_count({:and, children}), do: 1 + Enum.reduce(children, 0, &(&2 + node_count(&1)))
  def node_count({:or, children}), do: 1 + Enum.reduce(children, 0, &(&2 + node_count(&1)))
  def node_count({:not, child}), do: 1 + node_count(child)

  def node_count({:field, _path, predicates}),
    do: 1 + length(predicates) + Enum.reduce(predicates, 0, &(&2 + field_node_count(&1)))

  @doc "Return the maximum predicate-tree height, counting the root as one."
  @spec depth(t()) :: pos_integer()
  def depth(:match_all), do: 1

  def depth({:field, _path, predicates}) do
    1 + Enum.max(Enum.map(predicates, &field_predicate_depth/1), fn -> 0 end)
  end

  def depth({operator, children}) when operator in [:and, :or],
    do: 1 + Enum.max(Enum.map(children, &depth/1), fn -> 0 end)

  def depth({:not, child}), do: 1 + depth(child)

  @doc "Return both complexity measures in one storage-neutral map."
  @spec introspect(t()) :: %{node_count: pos_integer(), depth: pos_integer()}
  def introspect(predicate), do: %{node_count: node_count(predicate), depth: depth(predicate)}

  defp field_node_count({:elem_match, predicate}), do: node_count(predicate)
  defp field_node_count(_predicate), do: 0

  defp field_predicate_depth({:elem_match, predicate}), do: 1 + depth(predicate)
  defp field_predicate_depth(_predicate), do: 1

  defp render_field_predicate({:eq, value}), do: %{"op" => "$eq", "value" => value}
  defp render_field_predicate({:ne, value}), do: %{"op" => "$ne", "value" => value}
  defp render_field_predicate({:gt, value}), do: %{"op" => "$gt", "value" => value}
  defp render_field_predicate({:gte, value}), do: %{"op" => "$gte", "value" => value}
  defp render_field_predicate({:lt, value}), do: %{"op" => "$lt", "value" => value}
  defp render_field_predicate({:lte, value}), do: %{"op" => "$lte", "value" => value}
  defp render_field_predicate({:in, values}), do: %{"op" => "$in", "values" => values}
  defp render_field_predicate({:nin, values}), do: %{"op" => "$nin", "values" => values}
  defp render_field_predicate({:exists, value}), do: %{"op" => "$exists", "value" => value}

  defp render_field_predicate({:type, value}),
    do: %{"op" => "$type", "value" => Atom.to_string(value)}

  defp render_field_predicate({:begins_with, value}), do: %{"op" => "$beginsWith", "value" => value}

  defp render_field_predicate({:regex, regex}),
    do: %{"op" => "$regex", "source" => QueryRegex.source(regex)}

  defp render_field_predicate({:all, values}), do: %{"op" => "$all", "values" => values}

  defp render_field_predicate({:elem_match, predicate}),
    do: %{"op" => "$elemMatch", "value" => render(predicate)}

  defp render_field_predicate({:size, value}), do: %{"op" => "$size", "value" => value}

  defp render_field_predicate({:mod, divisor, remainder}),
    do: %{"op" => "$mod", "value" => [divisor, remainder]}

  defp compare_values(left, right) when left < right, do: :lt
  defp compare_values(left, right) when left > right, do: :gt
  defp compare_values(_left, _right), do: :eq
end
