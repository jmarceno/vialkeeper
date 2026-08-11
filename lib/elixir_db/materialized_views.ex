defmodule ElixirDB.MaterializedViews do
  @moduledoc "Application facade for creating materialized federated views."

  alias ElixirDB.DatabaseKind
  alias ElixirDB.DerivedView.{Definition, Path}
  alias ElixirDB.Runtime.DatabaseCatalog

  @spec create(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def create(request) when is_map(request) do
    with {:ok, definition} <- Definition.normalize(request),
         :ok <- validate_source_kinds(definition.sources),
         database_uuid <- ElixirDB.UUID.v4(),
         relative_path <- Path.for(definition.name, database_uuid),
         initial <- Definition.initial_metadata(definition, database_uuid),
         {:ok, identity} <-
           DatabaseCatalog.create_internal(relative_path, %{
             database_uuid: database_uuid,
             database_kind: DatabaseKind.derived(),
             initial_derived_view: initial
           }) do
      {:ok,
       Map.merge(identity, %{
         materialization_id: database_uuid,
         database_path: relative_path,
         definition_digest: definition.definition_digest
       })}
    end
  end

  def create(_request),
    do: {:error, ElixirDB.Error.invalid_request("materialized view request must be an object")}

  defp validate_source_kinds(sources) do
    Enum.reduce_while(sources, :ok, fn source_uuid, :ok ->
      case DatabaseCatalog.info(source_uuid) do
        {:ok, %{database_kind: :ordinary}} ->
          {:cont, :ok}

        {:ok, %{"database_kind" => "ordinary"}} ->
          {:cont, :ok}

        {:ok, %{database_kind: :derived}} ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("materialized view sources must be ordinary databases")}}

        {:ok, %{"database_kind" => "derived"}} ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("materialized view sources must be ordinary databases")}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end
end
