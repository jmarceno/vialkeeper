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

  @spec generation(binary()) :: {:ok, pos_integer()} | {:error, VialKeeper.Error.t()}
  def generation(revision_id) when is_binary(revision_id) do
    case Regex.run(~r/^(\d+)-[0-9a-f]{64}$/, revision_id) do
      [_, generation] -> {:ok, String.to_integer(generation)}
      _ -> {:error, VialKeeper.Error.invalid_request("invalid revision id")}
    end
  end

  @spec validate_history_id(binary()) :: :ok | {:error, VialKeeper.Error.t()}
  def validate_history_id(history_id) when is_binary(history_id) do
    if Regex.match?(
         ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
         history_id
       ),
       do: :ok,
       else: {:error, VialKeeper.Error.invalid_request("invalid history id")}
  end

  def validate_history_id(_),
    do: {:error, VialKeeper.Error.invalid_request("invalid history id")}

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
