defmodule VialKeeper.Federation.BookmarkCodec do
  @moduledoc "Independent codec for stateless federation continuation bookmarks."
  alias VialKeeper.JSON.{Canonical, StrictDecoder}

  @spec encode(map()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def encode(payload) when is_map(payload) do
    unsigned =
      payload |> Map.put("version", 1) |> Map.put("protocol_major", VialKeeper.protocol_major())

    with {:ok, json} <- Canonical.encode(unsigned) do
      {:ok,
       Base.url_encode64(Canonical.encode!(Map.put(unsigned, "checksum", digest(json))),
         padding: false
       )}
    end
  end

  @spec decode(term()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  @spec decode(term(), map()) :: {:ok, map()} | {:error, VialKeeper.Error.t()}
  def decode(bookmark, expected \\ %{})

  def decode(bookmark, expected) when is_binary(bookmark) do
    with {:ok, bytes} <- Base.url_decode64(bookmark, padding: false),
         {:ok, value} when is_map(value) <- StrictDecoder.decode(bytes),
         checksum when is_binary(checksum) <- value["checksum"],
         unsigned <- Map.delete(value, "checksum"),
         {:ok, json} <- Canonical.encode(unsigned),
         true <- checksum == digest(json),
         true <-
           unsigned["version"] == 1 and unsigned["protocol_major"] == VialKeeper.protocol_major(),
         :ok <- validate_expected(unsigned, expected),
         :ok <- validate_shape(unsigned) do
      {:ok, unsigned}
    else
      _ ->
        {:error,
         VialKeeper.Error.invalid_bookmark(
           "bookmark is invalid or bound to another federation query"
         )}
    end
  end

  def decode(_, _),
    do: {:error, VialKeeper.Error.invalid_bookmark("bookmark must be an opaque string")}

  defp validate_expected(value, expected),
    do:
      if(Enum.all?(expected, fn {key, val} -> value[key] == val end),
        do: :ok,
        else: {:error, :mismatch}
      )

  defp validate_shape(value) do
    with true <-
           Map.keys(value) |> Enum.sort() ==
             ~w(last_document_id last_source_uuid ordering_key protocol_major query_fingerprint sort sources version),
         true <-
           is_binary(value["query_fingerprint"]) and
             Regex.match?(~r/\A[0-9a-f]{64}\z/, value["query_fingerprint"]),
         true <- valid_sources?(value["sources"]),
         true <- valid_sort?(value["sort"]),
         true <-
           valid_ordering_key?(value["ordering_key"], value["sort"], value["last_document_id"]),
         true <- uuid?(value["last_source_uuid"]),
         true <- is_binary(value["last_document_id"]) and value["last_document_id"] != "",
         true <-
           Enum.any?(value["sources"], &(source_database_uuid(&1) == value["last_source_uuid"])) do
      :ok
    else
      _ -> {:error, :shape}
    end
  end

  defp valid_sources?(sources) when is_list(sources) and sources != [] do
    uuids = Enum.map(sources, &source_database_uuid/1)

    Enum.all?(sources, fn source ->
      is_map(source) and Enum.sort(Map.keys(source)) == ["database_uuid", "sequence"] and
        uuid?(source_database_uuid(source)) and
        is_integer(source["sequence"]) and source["sequence"] >= 0
    end) and
      length(Enum.uniq(uuids)) == length(uuids)
  end

  defp valid_sources?(_), do: false
  defp source_database_uuid(%{"database_uuid" => uuid}), do: uuid
  defp source_database_uuid(_), do: nil
  defp valid_sort?(sort), do: is_list(sort) and Enum.all?(sort, &valid_sort_entry?/1)

  defp valid_sort_entry?(%{"path" => path, "direction" => direction} = entry),
    do: map_size(entry) == 2 and is_binary(path) and path != "" and direction in ["asc", "desc"]

  defp valid_sort_entry?(_), do: false

  defp valid_ordering_key?(key, sort, id),
    do:
      is_list(key) and length(key) == length(sort) + 1 and hd(Enum.reverse(key)) == id and
        Enum.all?(key, &valid_json?/1)

  defp valid_json?(nil), do: true
  defp valid_json?(v) when is_binary(v) or is_boolean(v) or is_integer(v) or is_float(v), do: true
  defp valid_json?(v) when is_list(v), do: Enum.all?(v, &valid_json?/1)

  defp valid_json?(v) when is_map(v),
    do: Enum.all?(v, fn {k, x} -> is_binary(k) and valid_json?(x) end)

  defp valid_json?(_), do: false

  defp uuid?(value) when is_binary(value),
    do:
      Regex.match?(
        ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/,
        value
      )

  defp uuid?(_), do: false

  defp digest(json), do: :crypto.hash(:sha256, json) |> Base.encode16(case: :lower)
end
