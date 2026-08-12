defmodule ElixirDB.Attachments.Store do
  @moduledoc """
  Behaviour for immutable attachment bytes inside a database bundle.

  Streaming writer/reader state belongs to the calling process.

  Logical callbacks stream original uncompressed bytes. Representation
  callbacks stream the encoded payload only and must not decompress.
  """

  @callback begin_put(bundle_path :: binary(), max_bytes :: pos_integer(), options :: map()) ::
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
              bundle_path :: binary(),
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

  @callback exists?(bundle_path :: binary(), digest :: binary()) :: boolean()

  @callback stat(bundle_path :: binary(), digest :: binary()) ::
              {:ok, map()} | {:error, ElixirDB.Error.t()}

  @callback open_read(bundle_path :: binary(), digest :: binary()) ::
              {:ok, term()} | {:error, ElixirDB.Error.t()}

  @callback read_chunks(reader :: term()) ::
              {:ok, binary()} | {:done, term()} | {:error, ElixirDB.Error.t()}

  @callback close_read(reader :: term()) :: :ok

  @callback open_representation_read(bundle_path :: binary(), digest :: binary()) ::
              {:ok, {map(), term()}} | {:error, ElixirDB.Error.t()}

  @callback read_representation_chunks(reader :: term()) ::
              {:ok, binary()} | {:done, term()} | {:error, ElixirDB.Error.t()}

  @callback close_representation_read(reader :: term()) :: :ok

  @callback list_digests(bundle_path :: binary()) ::
              {:ok, [binary()]} | {:error, ElixirDB.Error.t()}

  @callback delete(bundle_path :: binary(), digest :: binary()) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback verify(bundle_path :: binary(), digest :: binary(), expected_size :: non_neg_integer()) ::
              :ok | {:error, ElixirDB.Error.t()}

  @callback cleanup_tmp(bundle_path :: binary(), cutoff :: DateTime.t()) ::
              :ok | {:error, ElixirDB.Error.t()}
end
