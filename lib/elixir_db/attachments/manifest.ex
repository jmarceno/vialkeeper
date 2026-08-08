defmodule ElixirDB.Attachments.Manifest do
  @moduledoc """
  Pure attachment-manifest normalization and validation.

  No filesystem or SQLite access.
  """

  alias ElixirDB.MapAccess

  @max_name_bytes 1024
  @max_content_type_bytes 256
  @digest_pattern ~r/^[0-9a-f]{64}$/

  @type entry :: %{
          required(:digest) => binary(),
          required(:length) => non_neg_integer(),
          required(:content_type) => binary()
        }

  @type t :: %{binary() => entry()}

  @doc "Normalizes and validates a manifest map keyed by attachment name."
  @spec normalize(term()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def normalize(manifest) when manifest == %{} do
    {:ok, %{}}
  end

  def normalize(manifest) when is_map(manifest) do
    Enum.reduce_while(manifest, {:ok, %{}}, fn {name, entry}, {:ok, acc} ->
      with {:ok, validated_name} <- validate_name(name),
           {:ok, normalized_entry} <- normalize_entry(entry) do
        {:cont, {:ok, Map.put(acc, validated_name, normalized_entry)}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  def normalize(_), do: {:error, ElixirDB.Error.invalid_request("attachments must be an object")}

  @doc "Produces the canonical map input for revision hashing."
  @spec canonical_for_hash(t()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def canonical_for_hash(manifest) when manifest == %{} do
    {:ok, %{}}
  end

  def canonical_for_hash(manifest) when is_map(manifest) do
    with {:ok, normalized} <- normalize(manifest) do
      {:ok,
       normalized
       |> Enum.sort_by(fn {name, _entry} -> name end)
       |> Map.new(fn {name, entry} ->
         {name,
          %{
            "digest" => entry.digest,
            "length" => entry.length,
            "content_type" => entry.content_type
          }}
       end)}
    end
  end

  @spec empty() :: t()
  def empty, do: %{}

  @doc "Resolves create/update inheritance semantics for mutation requests."
  @spec resolve_inheritance(atom(), :omitted | map(), t() | nil) ::
          {:ok, t()} | {:error, ElixirDB.Error.t()}
  def resolve_inheritance(:create, :omitted, _parent), do: {:ok, %{}}
  def resolve_inheritance(:update, :omitted, parent) when is_map(parent), do: {:ok, parent}

  def resolve_inheritance(:resolve_conflict, :omitted, parent) when is_map(parent),
    do: {:ok, parent}

  def resolve_inheritance(:resolve_conflict, :omitted, nil),
    do: {:error, ElixirDB.Error.invalid_request("attachments cannot be inherited without a parent")}

  def resolve_inheritance(:update, :omitted, nil),
    do: {:error, ElixirDB.Error.invalid_request("attachments cannot be inherited without a parent")}

  def resolve_inheritance(_operation, :omitted, _parent),
    do: {:error, ElixirDB.Error.invalid_request("attachments must be explicit for this operation")}

  def resolve_inheritance(_operation, manifest, _parent) when is_map(manifest),
    do: normalize(manifest)

  def resolve_inheritance(_operation, _manifest, _parent),
    do: {:error, ElixirDB.Error.invalid_request("attachments must be an object")}

  @doc "Validates client attachment references (`blob` + `content_type` only)."
  @spec normalize_references(term()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def normalize_references(references) when references == %{} do
    {:ok, %{}}
  end

  def normalize_references(references) when is_map(references) do
    Enum.reduce_while(references, {:ok, %{}}, fn {name, ref}, {:ok, acc} ->
      with {:ok, validated_name} <- validate_name(name),
           {:ok, digest} <- validate_digest(MapAccess.get(ref, :blob)),
           {:ok, content_type} <- validate_content_type(MapAccess.get(ref, :content_type)),
           :ok <- reject_client_length(ref) do
        {:cont, {:ok, Map.put(acc, validated_name, %{digest: digest, content_type: content_type})}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  def normalize_references(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment references must be an object")}

  @doc "Builds immutable manifest entries from validated local blob metadata."
  @spec from_blob_metadata(map(), map()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def from_blob_metadata(%{} = references, %{} = metadata_by_digest) do
    Enum.reduce_while(references, {:ok, %{}}, fn {name, ref}, {:ok, acc} ->
      with {:ok, validated_name} <- validate_name(name),
           {:ok, digest} <- validate_digest(ref.digest),
           {:ok, content_type} <- validate_content_type(ref.content_type),
           {:ok, length} <- lookup_length(metadata_by_digest, digest) do
        {:cont,
         {:ok,
          Map.put(acc, validated_name, %{
            digest: digest,
            length: length,
            content_type: content_type
          })}}
      else
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
  end

  @spec validate_name(term()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def validate_name(name) when is_binary(name) do
    cond do
      name == "" ->
        {:error, ElixirDB.Error.invalid_request("attachment name must be non-empty")}

      :binary.match(name, <<0>>) != :nomatch ->
        {:error, ElixirDB.Error.invalid_request("attachment name must not contain NUL")}

      not String.valid?(name) ->
        {:error, ElixirDB.Error.invalid_request("attachment name must be valid UTF-8")}

      control_char?(name) ->
        {:error,
         ElixirDB.Error.invalid_request("attachment name must not contain control characters")}

      byte_size(name) > @max_name_bytes ->
        {:error, ElixirDB.Error.invalid_request("attachment name exceeds maximum length")}

      true ->
        {:ok, name}
    end
  end

  def validate_name(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment name must be a string")}

  @spec validate_digest(term()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def validate_digest(digest) when is_binary(digest) do
    if Regex.match?(@digest_pattern, digest),
      do: {:ok, digest},
      else:
        {:error, ElixirDB.Error.invalid_request("attachment digest must be lowercase SHA-256 hex")}
  end

  def validate_digest(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment digest must be a string")}

  @spec validate_content_type(term()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def validate_content_type(content_type) when is_binary(content_type) do
    cond do
      content_type == "" ->
        {:error, ElixirDB.Error.invalid_request("attachment content_type must be non-empty")}

      not String.valid?(content_type) ->
        {:error, ElixirDB.Error.invalid_request("attachment content_type must be valid UTF-8")}

      byte_size(content_type) > @max_content_type_bytes ->
        {:error, ElixirDB.Error.invalid_request("attachment content_type exceeds maximum length")}

      true ->
        {:ok, content_type}
    end
  end

  def validate_content_type(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment content_type must be a string")}

  defp normalize_entry(entry) when is_map(entry) do
    with {:ok, digest} <- validate_digest(MapAccess.get(entry, :digest)),
         {:ok, content_type} <- validate_content_type(MapAccess.get(entry, :content_type)),
         length when is_integer(length) and length >= 0 <- MapAccess.get(entry, :length) do
      {:ok, %{digest: digest, length: length, content_type: content_type}}
    else
      {:error, _} = error -> error
      _ -> {:error, ElixirDB.Error.invalid_request("attachment entry fields are invalid")}
    end
  end

  defp normalize_entry(_),
    do: {:error, ElixirDB.Error.invalid_request("attachment entry must be an object")}

  defp control_char?(name) do
    Enum.any?(:binary.bin_to_list(name), fn code -> code < 32 or code == 127 end)
  end

  defp reject_client_length(ref) when is_map(ref) do
    if is_nil(MapAccess.get(ref, :length)),
      do: :ok,
      else: {:error, ElixirDB.Error.invalid_request("attachment length is server-derived")}
  end

  defp reject_client_length(_), do: :ok

  defp lookup_length(metadata_by_digest, digest) do
    case MapAccess.get(Map.get(metadata_by_digest, digest, %{}), :length) do
      length when is_integer(length) and length >= 0 -> {:ok, length}
      _ -> {:error, ElixirDB.Error.attachment_blob_not_found("attachment blob not found")}
    end
  end
end
