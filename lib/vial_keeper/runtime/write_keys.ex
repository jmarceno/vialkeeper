defmodule VialKeeper.Runtime.WriteKeys do
  @moduledoc """
  Facts about a write command that the runtime needs before the write starts:
  the document ids it locks and the most sequences it may use.

  Pure functions of the normalized command.
  """

  alias VialKeeper.Commands
  alias VialKeeper.MapAccess
  alias VialKeeper.Storage.Services.Sequences

  @single_document [
    Commands.PutDocument,
    Commands.CreateDocument,
    Commands.DeleteDocument,
    Commands.ResolveConflict
  ]

  @doc """
  Returns the distinct document ids a write command touches. A document id the
  server generates (a create without an id) needs no lock and is not listed.
  """
  @spec document_ids(struct()) :: [binary()]
  def document_ids(%module{request: request}) when module in @single_document,
    do: request |> request_document_id() |> List.wrap() |> distinct()

  def document_ids(%Commands.BulkWrite{request: request}) when is_map(request) do
    case MapAccess.get(request, :operations) do
      operations when is_list(operations) ->
        operations |> Enum.map(&request_document_id/1) |> distinct()

      _invalid ->
        []
    end
  end

  def document_ids(_command), do: []

  @doc """
  Returns the most sequences a write command may use, reserved before its
  transaction begins. Commands that never write documents return 0.
  """
  @spec sequence_count(struct()) :: non_neg_integer()
  def sequence_count(%module{}) when module in @single_document, do: 1

  def sequence_count(%Commands.BulkWrite{request: request}) when is_map(request),
    do: Sequences.bulk_bound(request)

  def sequence_count(%Commands.ImportRevisionChains{request: request}) when is_map(request),
    do: Sequences.import_bound(request)

  def sequence_count(%Commands.ApplyDerivedSourceBatch{request: request}) when is_map(request),
    do: Sequences.derived_batch_bound(request)

  def sequence_count(%Commands.ApplyDerivedRebuildPage{request: request}) when is_map(request),
    do: Sequences.derived_batch_bound(request)

  def sequence_count(%Commands.PruneDerivedRebuildStalePage{request: request})
      when is_map(request),
      do: Sequences.derived_prune_bound(request)

  def sequence_count(_command), do: 0

  defp request_document_id(request) when is_map(request) do
    case MapAccess.get(request, :document_id) do
      document_id when is_binary(document_id) -> document_id
      _server_generated_or_invalid -> nil
    end
  end

  defp request_document_id(_request), do: nil

  defp distinct(ids), do: ids |> Enum.filter(&is_binary/1) |> Enum.uniq()
end
