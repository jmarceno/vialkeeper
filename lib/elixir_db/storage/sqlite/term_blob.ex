defmodule ElixirDB.Storage.SQLite.TermBlob do
  @moduledoc """
  Versioned, integrity-bound BEAM terms stored alongside canonical JSON.

  The JSON digest in the header binds a decoded term to canonical JSON. Full
  hash-verified reads and integrity checks compare it with the text in the same
  row; trusted hot reads compare it with the decoded term's canonical encoding
  without selecting or parsing JSON. A damaged BLOB returns an integrity error
  on those hot paths.
  """

  @magic "EXDBTERM"
  @version 1
  @safe_integer_max 9_007_199_254_740_991
  @max_term_depth 256
  @cache_key :elixir_db_sqlite_term_blob_cache

  alias ElixirDB.JSON.{Canonical, StrictCache}

  @type fallback_reason :: :missing | :invalid_header | :digest_mismatch | :invalid_term

  @spec bind(binary() | nil) :: {:blob, binary()} | nil
  def bind(nil), do: nil
  def bind(value) when is_binary(value), do: {:blob, value}

  @spec encode(term(), binary()) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def encode(value, canonical_json) when is_binary(canonical_json) do
    if valid_json_term?(value, 0, @max_term_depth) do
      payload = :erlang.term_to_binary(value)
      digest = :crypto.hash(:sha256, canonical_json)
      {:ok, <<@magic::binary, @version, digest::binary-size(32), payload::binary>>}
    else
      {:error, ElixirDB.Error.integrity_violation("cannot encode an invalid JSON term")}
    end
  end

  @spec decode(binary() | nil, binary() | nil) ::
          {:ok, term()} | {:fallback, fallback_reason()}
  def decode(nil, _canonical_json), do: {:fallback, :missing}

  def decode(blob, canonical_json) when is_binary(blob) and is_binary(canonical_json) do
    case payload_from_blob(blob) do
      {:ok, digest, payload} ->
        if digest == :crypto.hash(:sha256, canonical_json),
          do: decode_payload(payload),
          else: {:fallback, :digest_mismatch}

      :error ->
        {:fallback, :invalid_header}
    end
  end

  def decode(_blob, _canonical_json), do: {:fallback, :invalid_header}

  @doc "Decodes and verifies a trusted stored term without loading canonical JSON."
  @spec decode_trusted(binary() | nil) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
  def decode_trusted(blob), do: decode_trusted(blob, @max_term_depth)

  @spec decode_trusted(binary() | nil, non_neg_integer()) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def decode_trusted(blob, max_depth)
      when is_binary(blob) and is_integer(max_depth) and max_depth >= 0 do
    case blob |> payload_from_blob() |> trusted_payload(max_depth) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, ElixirDB.Error.integrity_violation("stored JSON term BLOB is invalid")}
    end
  end

  def decode_trusted(_blob, max_depth) when not is_integer(max_depth) or max_depth < 0,
    do: {:error, ElixirDB.Error.invalid_request("JSON depth limit must be a non-negative integer")}

  def decode_trusted(_blob, _max_depth),
    do: {:error, ElixirDB.Error.integrity_violation("stored JSON term BLOB is missing")}

  @spec decode_trusted_with_cache(binary() | nil, atom(), non_neg_integer(), pos_integer()) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def decode_trusted_with_cache(blob, cache_name, max_depth, limit)
      when is_atom(cache_name) and is_integer(max_depth) and max_depth >= 0 and
             is_integer(limit) and limit > 0 do
    StrictCache.memoize(
      {:term_blob, cache_name, max_depth},
      blob,
      limit,
      fn -> decode_trusted(blob, max_depth) end
    )
  end

  @spec decode_with_cache(binary() | nil, binary() | nil, atom(), pos_integer()) ::
          {:ok, term()} | {:fallback, fallback_reason()}
  def decode_with_cache(blob, canonical_json, cache_name, limit)
      when is_atom(cache_name) and is_integer(limit) and limit > 0 do
    cache_key = {@cache_key, cache_name}
    cache = Process.get(cache_key, %{})

    case Map.fetch(cache, blob) do
      {:ok, {^canonical_json, value}} ->
        {:ok, value}

      _ ->
        case decode(blob, canonical_json) do
          {:ok, value} = result ->
            Process.put(
              cache_key,
              put_verified_cache_value(cache, blob, canonical_json, value, limit)
            )

            result

          fallback ->
            fallback
        end
    end
  end

  defp put_verified_cache_value(cache, blob, canonical_json, value, limit)
       when map_size(cache) < limit,
       do: Map.put(cache, blob, {canonical_json, value})

  defp put_verified_cache_value(cache, blob, canonical_json, value, _limit) do
    [evicted_key | _] = Map.keys(cache)
    cache |> Map.delete(evicted_key) |> Map.put(blob, {canonical_json, value})
  end

  defp payload_from_blob(<<@magic::binary, @version, digest::binary-size(32), payload::binary>>)
       when byte_size(payload) > 0,
       do: {:ok, digest, payload}

  defp payload_from_blob(_blob), do: :error

  defp trusted_payload({:ok, digest, payload}, max_depth) do
    case decode_payload(payload, max_depth) do
      {:ok, value} ->
        verify_trusted_value(digest, value)

      _ ->
        :error
    end
  end

  defp trusted_payload(:error, _max_depth), do: :error

  defp verify_trusted_value(digest, value) do
    case Canonical.encode(value) do
      {:ok, canonical_json} ->
        if :crypto.hash(:sha256, canonical_json) == digest, do: {:ok, value}, else: :error

      _ ->
        :error
    end
  end

  defp decode_payload(payload), do: decode_payload(payload, @max_term_depth)

  defp decode_payload(payload, max_depth) do
    value = :erlang.binary_to_term(payload, [:safe])

    if valid_json_term?(value, 0, max_depth),
      do: {:ok, value},
      else: {:fallback, :invalid_term}
  rescue
    _error in [ArgumentError, SystemLimitError] -> {:fallback, :invalid_term}
  end

  defp valid_json_term?(value, depth, max_depth)
       when depth <= @max_term_depth and depth <= max_depth do
    case value do
      value when is_map(value) -> valid_map?(value, depth, max_depth)
      value when is_list(value) -> valid_list?(value, depth, max_depth)
      scalar -> valid_scalar?(scalar)
    end
  end

  defp valid_json_term?(_value, _depth, _max_depth), do: false

  defp valid_scalar?(nil), do: true
  defp valid_scalar?(value) when is_boolean(value), do: true
  defp valid_scalar?(value) when is_binary(value), do: String.valid?(value)
  defp valid_scalar?(value) when is_integer(value), do: abs(value) <= @safe_integer_max
  defp valid_scalar?(value) when is_float(value), do: finite_float?(value)
  defp valid_scalar?(_value), do: false

  defp finite_float?(value) do
    not (value !== value) and finite_float_magnitude?(abs(value))
  end

  defp finite_float_magnitude?(value) do
    maximum = Float.max_finite()
    value < maximum or value == maximum
  end

  defp valid_map?(value, depth, max_depth) do
    Enum.all?(value, fn {key, nested} ->
      is_binary(key) and String.valid?(key) and valid_json_term?(nested, depth + 1, max_depth)
    end)
  end

  defp valid_list?([], _depth, _max_depth), do: true

  defp valid_list?([head | tail], depth, max_depth),
    do: valid_json_term?(head, depth + 1, max_depth) and valid_list?(tail, depth, max_depth)

  defp valid_list?(_improper, _depth, _max_depth), do: false
end
