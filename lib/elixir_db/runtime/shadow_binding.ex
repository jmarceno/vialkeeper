defmodule ElixirDB.Runtime.ShadowBinding do
  @moduledoc """
  Checks that a shadow command's authority matches durable shadow metadata.

  Owner writes and snapshot readers both run this check. Readers read metadata
  from their own snapshot, not from the writer process.
  """

  alias ElixirDB.Error
  alias ElixirDB.MapAccess
  alias ElixirDB.Runtime.CommandContext
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.Services

  @spec check(DatabaseKind.t() | term(), BackendContext.t(), CommandContext.t(), binary()) ::
          :ok | {:error, Error.t()}
  def check(:shadow, %BackendContext{} = context, %CommandContext{} = authority, uuid)
      when is_binary(uuid) do
    match_shadow_binding(context, authority, uuid)
  end

  def check(_database_kind, %BackendContext{}, %CommandContext{}, uuid) when is_binary(uuid),
    do: :ok

  defp match_shadow_binding(_context, %CommandContext{class: :public}, _uuid), do: :ok

  defp match_shadow_binding(context, authority, uuid) do
    case Services.shadow_metadata(context) do
      {:ok, metadata} when is_map(metadata) ->
        if shadow_context_matches?(authority, metadata, uuid) do
          :ok
        else
          {:error,
           Error.shadow_identity_conflict("shadow command context does not match durable metadata")}
        end

      {:ok, nil} ->
        {:error, Error.shadow_incompatible("shadow metadata is missing")}

      {:error, _} = error ->
        error
    end
  end

  defp shadow_context_matches?(%CommandContext{class: :shadow_control} = authority, metadata, uuid) do
    uuid_field(metadata, :shadow_database_uuid) == uuid and
      (is_nil(authority.shadow_database_uuid) or authority.shadow_database_uuid == uuid) and
      optional_field_matches?(
        authority.source_database_uuid,
        uuid_field(metadata, :source_database_uuid)
      ) and
      optional_field_matches?(authority.generation, MapAccess.get(metadata, :generation)) and
      optional_field_matches?(authority.operation_id, uuid_field(metadata, :operation_id))
  end

  defp shadow_context_matches?(authority, metadata, uuid) do
    authority.shadow_database_uuid == uuid and
      authority.shadow_database_uuid == uuid_field(metadata, :shadow_database_uuid) and
      authority.source_database_uuid == uuid_field(metadata, :source_database_uuid) and
      authority.generation == MapAccess.get(metadata, :generation) and
      authority.operation_id == uuid_field(metadata, :operation_id)
  end

  defp optional_field_matches?(nil, _expected), do: true
  defp optional_field_matches?(value, expected), do: value == expected

  defp uuid_field(metadata, key), do: MapAccess.get(metadata, key)
end
