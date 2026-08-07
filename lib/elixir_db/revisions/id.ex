defmodule ElixirDB.Revisions.Id do
  @moduledoc "Content-addressed revision identifier helpers."

  alias ElixirDB.JSON.Canonical

  @spec calculate(binary(), binary() | nil, boolean(), map() | nil) ::
          {:ok, binary()} | {:error, ElixirDB.Error.t()}
  def calculate(document_id, parent_revision, deleted, body)
      when is_binary(document_id) and (is_binary(parent_revision) or is_nil(parent_revision)) and
             is_boolean(deleted) do
    with {:ok, generation} <- next_generation(parent_revision),
         payload <- %{
           "version" => 1,
           "document_id" => document_id,
           "parent_revision" => parent_revision,
           "deleted" => deleted,
           "body" => if(deleted, do: nil, else: body)
         },
         {:ok, canonical} <- Canonical.encode(payload) do
      digest = :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)
      {:ok, "#{generation}-#{digest}"}
    end
  end

  def generation(revision_id) when is_binary(revision_id) do
    case Regex.run(~r/^(\d+)-[0-9a-f]{64}$/, revision_id) do
      [_, generation] -> {:ok, String.to_integer(generation)}
      _ -> {:error, ElixirDB.Error.invalid_request("invalid revision id")}
    end
  end

  defp next_generation(nil), do: {:ok, 1}

  defp next_generation(parent) do
    with {:ok, value} <- generation(parent), do: {:ok, value + 1}
  end
end
