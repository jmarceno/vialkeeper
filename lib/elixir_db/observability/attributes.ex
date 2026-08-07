defmodule ElixirDB.Observability.Attributes do
  @moduledoc """
  Attribute allow-list for OpenTelemetry spans and metrics (Plan §11 / OBSV-003).

  A single module owns the allow-list so a future field addition cannot
  accidentally leak a document body, search text, revision body, or any other
  customer data. Any attribute not constructed through `build/1` MUST NOT be
  attached to a span or metric.

  ## Allowed fields

  `db.uuid`, `command.type` (atom), `error.code` (atom), `outcome` (atom),
  `http.method`, `http.route` (route template, never the raw path),
  `http.status_code`, `index_id`, `index_type`, `replication.id`, `endpoint`
  (`:source` | `:target`), the bounded counts `entries`, `examined`,
  `revisions_written`, and `finch.duration` (the telemetry bridge only: a
  bounded numeric duration from the Finch stop event, never customer data).

  ## Forbidden (enforced by absence)

  Document bodies, document IDs (unbounded cardinality and customer data),
  search text, revision IDs/bodies, full remote URLs, request bodies.
  """

  @allowed [
    db_uuid: :"db.uuid",
    command_type: :"command.type",
    error_code: :"error.code",
    outcome: :outcome,
    http_method: :"http.method",
    http_route: :"http.route",
    http_status_code: :"http.status_code",
    index_id: :index_id,
    index_type: :index_type,
    replication_id: :"replication.id",
    endpoint: :endpoint,
    entries: :entries,
    examined: :examined,
    revisions_written: :revisions_written,
    finch_duration: :"finch.duration"
  ]

  @allowed_keys Keyword.keys(@allowed)

  @doc """
  Builds an OTel attribute map from a keyword list, dropping any key not on the
  allow-list (and any nil value) and coercing values to OTel-compatible types
  (atoms/binary/number/boolean).

  Unknown keys are dropped silently; callers should only ever pass allow-listed
  keys. A value that cannot be coerced is dropped rather than raised — build/1
  runs on hot paths and must never crash the caller (§1).
  """
  @spec build(keyword()) :: map()
  def build(attrs) when is_list(attrs) do
    Enum.reduce(attrs, %{}, &build_attribute/2)
  end

  defp build_attribute({key, _value}, acc) when key not in @allowed_keys, do: acc
  defp build_attribute({_key, nil}, acc), do: acc

  defp build_attribute({key, value}, acc) do
    case coerce(value) do
      nil -> acc
      coerced -> Map.put(acc, otel_key!(key), coerced)
    end
  end

  @doc "Returns the OTel attribute key for an allow-listed local key."
  def otel_key(key) when key in @allowed_keys, do: Keyword.fetch!(@allowed, key)

  defp otel_key!(key), do: Keyword.fetch!(@allowed, key)

  defp coerce(value) when is_atom(value), do: value
  defp coerce(value) when is_binary(value), do: value
  defp coerce(value) when is_number(value), do: value

  defp coerce(value) when is_list(value),
    do: value |> Enum.map(&coerce/1) |> Enum.reject(&is_nil/1)

  defp coerce(other) do
    # Maps, PIDs, refs, tuples: not OTel attribute material. Drop instead of
    # raising (to_string/1 crashes on them).
    if String.Chars.impl_for(other), do: to_string(other), else: nil
  end
end
