defmodule VialKeeper.Revisions.Id do
  @moduledoc "Content-addressed revision identifier helpers."

  alias VialKeeper.Attachments.Manifest
  alias VialKeeper.JSON.Canonical
  alias VialKeeper.UUID

  @type calculate_attrs :: %{
          required(:document_id) => binary(),
          required(:history_id) => binary(),
          required(:parent_revision) => binary() | nil,
          required(:deleted) => boolean(),
          required(:body) => map() | nil,
          required(:attachments) => Manifest.t() | map()
        }

  @doc """
  Calculates a content-addressed revision ID from a history-aware attribute map.

  The digest payload includes `version`, `document_id`, `history_id`,
  `parent_revision`, `deleted`, `body`, and `attachments` (`REV-002`).
  """
  @spec calculate(calculate_attrs()) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def calculate(%{
        document_id: document_id,
        history_id: history_id,
        parent_revision: parent_revision,
        deleted: deleted,
        body: body,
        attachments: attachments
      })
      when is_binary(document_id) and is_binary(history_id) and
             (is_binary(parent_revision) or is_nil(parent_revision)) and is_boolean(deleted) do
    calculate(document_id, history_id, parent_revision, deleted, body, attachments)
  end

  def calculate(_),
    do: {:error, VialKeeper.Error.invalid_request("invalid revision identity attributes")}

  @doc """
  Calculates a content-addressed revision ID with an explicit history ID.
  """
  @spec calculate(binary(), binary(), binary() | nil, boolean(), map() | nil, Manifest.t() | map()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def calculate(document_id, history_id, parent_revision, deleted, body, attachments),
    do: calculate(document_id, history_id, parent_revision, deleted, body, attachments, nil)

  @doc """
  Calculates a revision ID, reusing `body_json` when the caller already holds
  the body's canonical JSON (`Canonical.encode(body)`), so the body is not
  encoded a second time. The ID is identical either way.
  """
  @spec calculate(
          binary(),
          binary(),
          binary() | nil,
          boolean(),
          map() | nil,
          Manifest.t() | map(),
          binary() | nil
        ) :: {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def calculate(document_id, history_id, parent_revision, deleted, body, attachments, body_json) do
    with {:ok, revision_id, _generation} <-
           calculate_with_generation(
             document_id,
             history_id,
             parent_revision,
             deleted,
             body,
             attachments,
             body_json
           ) do
      {:ok, revision_id}
    end
  end

  @doc """
  Like `calculate/7`, also returning the new revision's generation so callers
  need not parse it back out of the ID.
  """
  @spec calculate_with_generation(
          binary(),
          binary(),
          binary() | nil,
          boolean(),
          map() | nil,
          Manifest.t() | map(),
          binary() | nil
        ) :: {:ok, binary(), pos_integer()} | {:error, VialKeeper.Error.t()}
  def calculate_with_generation(
        document_id,
        history_id,
        parent_revision,
        deleted,
        body,
        attachments,
        body_json
      )
      when is_binary(document_id) and is_binary(history_id) and
             (is_binary(parent_revision) or is_nil(parent_revision)) and is_boolean(deleted) and
             (is_binary(body_json) or is_nil(body_json)) do
    with :ok <- validate_history_id(history_id),
         {:ok, generation} <- next_generation(parent_revision),
         {:ok, canonical_attachments} <- canonical_attachments(attachments, deleted),
         payload <- %{
           "version" => 1,
           "document_id" => document_id,
           "history_id" => history_id,
           "parent_revision" => parent_revision,
           "deleted" => deleted,
           "body" => payload_body(deleted, body, body_json),
           "attachments" => canonical_attachments
         },
         {:ok, canonical} <- Canonical.encode(payload) do
      digest = :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)
      {:ok, "#{generation}-#{digest}", generation}
    end
  end

  @spec new_root(binary(), map(), Manifest.t() | map()) ::
          {:ok, binary(), binary()} | {:error, VialKeeper.Error.t()}
  def new_root(document_id, body, attachments)
      when is_binary(document_id) and is_map(body) do
    history_id = UUID.v4()

    with {:ok, revision_id} <- calculate(document_id, history_id, nil, false, body, attachments) do
      {:ok, revision_id, history_id}
    end
  end

  @doc """
  Builds a generation-1 root revision ID with an explicit history ID.
  """
  @spec new_root(binary(), binary(), map(), Manifest.t() | map()) ::
          {:ok, binary()} | {:error, VialKeeper.Error.t()}
  def new_root(document_id, history_id, body, attachments)
      when is_binary(document_id) and is_binary(history_id) and is_map(body) do
    calculate(document_id, history_id, nil, false, body, attachments)
  end

  # Revision and history IDs are matched as binaries rather than with regexes,
  # which are recompiled on every call on recent OTP releases. The matches
  # accept exactly what `~r/^(\d+)-[0-9a-f]{64}$/` and the case-insensitive
  # UUID regex accepted, including one trailing newline before the end.
  defguardp digit?(byte) when byte in ?0..?9
  defguardp lower_hex?(byte) when byte in ?0..?9 or byte in ?a..?f
  defguardp hex?(byte) when lower_hex?(byte) or byte in ?A..?F

  @spec generation(binary()) :: {:ok, pos_integer()} | {:error, VialKeeper.Error.t()}
  def generation(revision_id) when is_binary(revision_id) do
    with [digits, digest] when digits != "" <- :binary.split(revision_id, "-"),
         true <- all_digits?(digits),
         true <- revision_digest?(digest) do
      {:ok, String.to_integer(digits)}
    else
      _ -> {:error, VialKeeper.Error.invalid_request("invalid revision id")}
    end
  end

  defp all_digits?(<<byte, rest::binary>>) when digit?(byte), do: all_digits?(rest)
  defp all_digits?(<<>>), do: true
  defp all_digits?(_binary), do: false

  defp revision_digest?(<<digest::binary-size(64)>>), do: lower_hex_digest?(digest)
  defp revision_digest?(<<digest::binary-size(64), ?\n>>), do: lower_hex_digest?(digest)
  defp revision_digest?(_binary), do: false

  defp lower_hex_digest?(<<byte, rest::binary>>) when lower_hex?(byte),
    do: lower_hex_digest?(rest)

  defp lower_hex_digest?(<<>>), do: true
  defp lower_hex_digest?(_binary), do: false

  @spec validate_history_id(binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_history_id(<<uuid::binary-size(36)>>), do: validate_uuid(uuid)
  def validate_history_id(<<uuid::binary-size(36), ?\n>>), do: validate_uuid(uuid)

  def validate_history_id(_),
    do: {:error, VialKeeper.Error.invalid_request("invalid history id")}

  defp validate_uuid(
         <<a::binary-size(8), ?-, b::binary-size(4), ?-, version, c::binary-size(3), ?-, variant,
           d::binary-size(3), ?-, e::binary-size(12)>>
       )
       when version in ?1..?5 and variant in [?8, ?9, ?a, ?b, ?A, ?B] do
    if Enum.all?([a, b, c, d, e], &all_hex?/1),
      do: :ok,
      else: {:error, VialKeeper.Error.invalid_request("invalid history id")}
  end

  defp validate_uuid(_uuid),
    do: {:error, VialKeeper.Error.invalid_request("invalid history id")}

  defp all_hex?(<<byte, rest::binary>>) when hex?(byte), do: all_hex?(rest)
  defp all_hex?(<<>>), do: true
  defp all_hex?(_binary), do: false

  defp payload_body(true, _body, _body_json), do: nil
  defp payload_body(false, body, nil), do: body
  defp payload_body(false, _body, body_json), do: Canonical.fragment(body_json)

  defp canonical_attachments(_attachments, true), do: {:ok, %{}}

  defp canonical_attachments(attachments, false) do
    Manifest.canonical_for_hash(attachments || %{})
  end

  defp next_generation(nil), do: {:ok, 1}

  defp next_generation(parent) do
    with {:ok, value} <- generation(parent), do: {:ok, value + 1}
  end
end
