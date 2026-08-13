defmodule ElixirDB.Shadow.Endpoint do
  @moduledoc "Bounded endpoint contract for local and remote shadow workers."

  @callback capabilities(endpoint :: term(), timeout()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback provision(endpoint :: term(), request :: map(), timeout()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback inspect(endpoint :: term(), request :: map(), timeout()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback destroy(endpoint :: term(), request :: map(), timeout()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}
  @callback read_document(endpoint :: term(), request :: map(), timeout(), keyword()) ::
              {:ok, term()} | {:error, ElixirDB.Error.t()}
  @callback bulk_read_documents(endpoint :: term(), request :: map(), timeout(), keyword()) ::
              {:ok, term()} | {:error, ElixirDB.Error.t()}
  @callback open_attachment_representation(
              endpoint :: term(),
              request :: map(),
              timeout(),
              keyword()
            ) :: {:ok, term()} | {:error, ElixirDB.Error.t()}
end
