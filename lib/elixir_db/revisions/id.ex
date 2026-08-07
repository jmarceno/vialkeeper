defmodule ElixirDB.Revisions.Id do
  @moduledoc "Content-addressed revision identifier helpers."

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.UUID

  @type calculate_attrs :: %{
          required(:document_id) => binary(),
          required(:history_id) => binary(),
          required(:parent_revision) => binary() | nil,
          required(:deleted) => boolean(),
          required(:body) => map() | nil
        }

  @doc """
  Calculates a content-addressed revision ID from a history-aware attribute map.

  The digest payload includes `version`, `document_id`, `history_id`,
  `parent_revision`, `deleted`, and `body` (`REV-002`).
  """
  @spec calculate(calculate_attrs()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(%{
        document_id: document_id,
        history_id: history_id,
        parent_revision: parent_revision,
        deleted: deleted,
        body: body
      })
      when is_binary(document_id) and is_binary(history_id) and
             (is_binary(parent_revision) or is_nil(parent_revision)) and is_boolean(deleted) do
    calculate(document_id, history_id, parent_revision, deleted, body)
  end

  def calculate(_),
    do: {:error, ElixirDB.Error.invalid_request("invalid revision identity attributes")}

  @doc """
  Calculates a content-addressed revision ID with an explicit history ID.
  """
  @spec calculate(binary(), binary(), binary() | nil, boolean(), map() | nil) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(document_id, history_id, parent_revision, deleted, body)
      when is_binary(document_id) and is_binary(history_id) and
             (is_binary(parent_revision) or is_nil(parent_revision)) and is_boolean(deleted) do
    with :ok <- validate_history_id(history_id),
         {:ok, generation} <- next_generation(parent_revision),
         payload <- %{
           "version" => 1,
           "document_id" => document_id,
           "history_id" => history_id,
           "parent_revision" => parent_revision,
           "deleted" => deleted,
           "body" => if(deleted, do: nil, else: body)
         },
         {:ok, canonical} <- Canonical.encode(payload) do
      digest = :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)
      {:ok, "#{generation}-#{digest}"}
    end
  end

  @doc """
  Builds a generation-1 root revision ID with a freshly generated history ID.

  Returns `{:ok, revision_id, history_id}`.
  """
  @spec new_root(binary(), map()) :: {:ok, binary(), binary()} | {:error, ElixirDB.Error.t()}
  def new_root(document_id, body) when is_binary(document_id) and is_map(body) do
    history_id = UUID.v4()

    with {:ok, revision_id} <- calculate(document_id, history_id, nil, false, body) do
      {:ok, revision_id, history_id}
    end
  end

  @doc """
  Builds a generation-1 root revision ID with an explicit history ID.

  Used by fixtures and import paths that already own the history identifier.
  """
  @spec new_root(binary(), binary(), map()) :: {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def new_root(document_id, history_id, body)
      when is_binary(document_id) and is_binary(history_id) and is_map(body) do
    calculate(document_id, history_id, nil, false, body)
  end

  @spec generation(binary()) :: {:ok, pos_integer()} | {:error, ElixirDB.Error.t()}
  def generation(revision_id) when is_binary(revision_id) do
    case Regex.run(~r/^(\d+)-[0-9a-f]{64}$/, revision_id) do
      [_, generation] -> {:ok, String.to_integer(generation)}
      _ -> {:error, ElixirDB.Error.invalid_request("invalid revision id")}
    end
  end

  @spec validate_history_id(binary()) :: :ok | {:error, ElixirDB.Error.t()}
  def validate_history_id(history_id) when is_binary(history_id) do
    if Regex.match?(
         ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
         history_id
       ),
       do: :ok,
       else: {:error, ElixirDB.Error.invalid_request("invalid history id")}
  end

  def validate_history_id(_),
    do: {:error, ElixirDB.Error.invalid_request("invalid history id")}

  defp next_generation(nil), do: {:ok, 1}

  defp next_generation(parent) do
    with {:ok, value} <- generation(parent), do: {:ok, value + 1}
  end
end
