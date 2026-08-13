defmodule ElixirDB.Attachments.Store do
  @moduledoc """
  Behaviour for immutable attachment bytes inside a database bundle.

  Streaming writer/reader state belongs to the calling process.

  Logical callbacks stream original uncompressed bytes. Representation
  callbacks stream the encoded payload only and must not decompress.
  """

  alias ElixirDB.Attachments.StoreRef

  @callback begin_put(store_ref :: StoreRef.t(), max_bytes :: pos_integer(), options :: map()) ::
              {:ok, term()} | {:error, ElixirDB.Error.t()}

  @callback write_chunk(writer :: term(), chunk :: binary()) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback finish_put(writer :: term()) ::
              {:ok,
               %{
                 digest: binary(),
                 logical_size: non_neg_integer(),
                 encoding: :raw | :zstd,
                 deduplicated?: boolean()
               }}
              | {:error, ElixirDB.Error.t()}

  @callback abort_put(writer :: term()) :: :ok

  @callback begin_put_representation(
              store_ref :: StoreRef.t(),
              descriptor :: map(),
              max_logical_bytes :: pos_integer()
            ) :: {:ok, term()} | {:error, ElixirDB.Error.t()}

  @callback write_representation_chunk(writer :: term(), chunk :: binary()) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback finish_put_representation(writer :: term()) ::
              {:ok,
               %{
                 digest: binary(),
                 logical_size: non_neg_integer(),
                 encoding: :raw | :zstd,
                 deduplicated?: boolean()
               }}
              | {:error, ElixirDB.Error.t()}

  @callback abort_put_representation(writer :: term()) :: :ok

  @callback exists?(store_ref :: StoreRef.t(), digest :: binary()) :: boolean()

  @callback stat(store_ref :: StoreRef.t(), digest :: binary()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}

  @callback open_read(store_ref :: StoreRef.t(), digest :: binary()) ::
              {:ok, term()} | {:error, ElixirDB.Error.t()}

  @callback read_chunks(reader :: term()) ::
              {:ok, binary()} | {:done, term()} | {:error, ElixirDB.Error.t()}

  @callback close_read(reader :: term()) :: :ok

  @callback open_representation_read(store_ref :: StoreRef.t(), digest :: binary()) ::
              {:ok, {map(), term()}} | {:error, ElixirDB.Error.t()}

  @callback read_representation_chunks(reader :: term()) ::
              {:ok, binary()} | {:done, term()} | {:error, ElixirDB.Error.t()}

  @callback close_representation_read(reader :: term()) :: :ok

  @callback list_digests(store_ref :: StoreRef.t()) ::
              {:ok, [binary()]} | {:error, ElixirDB.Error.t()}

  @callback delete(store_ref :: StoreRef.t(), digest :: binary()) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback verify(
              store_ref :: StoreRef.t(),
              digest :: binary(),
              expected_size :: non_neg_integer()
            ) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback cleanup_tmp(store_ref :: StoreRef.t(), cutoff :: DateTime.t()) ::
              :ok | {:error, ElixirDB.Error.t()}
end
