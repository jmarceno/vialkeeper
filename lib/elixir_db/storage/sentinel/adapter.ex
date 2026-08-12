defmodule ElixirDB.Storage.Sentinel.Adapter do
  @moduledoc """
  SQL-free sentinel storage backend for boundary proofs.

  Implements lifecycle operations only. Feature callbacks return a typed
  unsupported error so contract suites can prove the runtime can select a
  non-SQL backend module without importing a physical engine adapter.
  """
  @behaviour ElixirDB.Storage.Adapter

  alias ElixirDB.MapAccess
  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.RequestValidation
  alias ElixirDB.Storage.Sentinel.{Ownership, Transaction}

  defstruct [:root, :identity, :closed?]

  @type t :: %__MODULE__{
          root: binary(),
          identity: map(),
          closed?: boolean()
        }

  @unsupported [
    {:update_config, 2},
    {:integrity_check, 2},
    {:get_document, 2},
    {:get_revision, 2},
    {:read_changes, 2},
    {:has_local_origin_changes?, 1},
    {:has_local_origin_changes?, 2},
    {:clear_pending_local_causal, 1},
    {:clear_pending_local_causal, 2},
    {:get_local_record, 3},
    {:put_local_record_cas, 2},
    {:retention_state, 1},
    {:list_peer_positions, 1},
    {:put_peer_position_cas, 2},
    {:read_boundary_pages, 2},
    {:install_boundary_pages, 2},
    {:compact_retention, 2},
    {:list_replication_jobs, 1},
    {:put_replication_job, 2},
    {:delete_replication_job, 2},
    {:create_index, 2},
    {:delete_index, 2},
    {:rebuild_index, 2},
    {:list_indexes, 1},
    {:execute_query, 2},
    {:execute_subscription_snapshot, 2},
    {:get_revisions_batch, 2},
    {:explain_query, 2},
    {:resolve_attachment_ticket, 2},
    {:resolve_blob_metadata, 2},
    {:protect_pending_blob, 2},
    {:remove_pending_blob_protection, 2},
    {:list_live_attachment_digests, 2},
    {:cleanup_expired_pending_blobs, 2},
    {:list_views, 1},
    {:create_view, 2},
    {:delete_view, 2},
    {:view_state, 2},
    {:apply_view_batch, 2},
    {:begin_view_rebuild, 2},
    {:append_view_rebuild_page, 2},
    {:finish_view_rebuild, 2},
    {:query_view, 2},
    {:read_winning_documents_page, 2},
    {:get_derived_view, 1},
    {:set_derived_enabled, 2},
    {:list_derived_sources, 1},
    {:set_derived_source_error, 2},
    {:apply_derived_source_batch, 2},
    {:begin_derived_source_rebuild, 2},
    {:apply_derived_rebuild_page, 2},
    {:prune_derived_rebuild_stale_page, 2},
    {:finish_derived_source_rebuild, 2}
  ]

  for {name, arity} <- @unsupported do
    args = Macro.generate_arguments(arity, __MODULE__)

    @impl true
    def unquote(name)(unquote_splicing(args)) do
      unsupported(unquote(name))
    end
  end

  @impl true
  def create(path, options \\ %{}) when is_binary(path) do
    options = normalize_options(options)
    uuid = MapAccess.get(options, :database_uuid, ElixirDB.UUID.v4())

    with :ok <- RequestValidation.validate_uuid(uuid),
         :ok <- ensure_root(path),
         :ok <- write_marker(path, uuid) do
      {:ok, build(path, uuid)}
    end
  end

  @impl true
  def open(path, _options \\ %{}) when is_binary(path) do
    with :ok <- ensure_existing_root(path),
         {:ok, uuid} <- read_marker(marker_path(path)) do
      {:ok, build(path, uuid)}
    end
  end

  @impl true
  def close(%__MODULE__{} = adapter) do
    _ = adapter
    :ok
  end

  @impl true
  def identity(%__MODULE__{identity: identity}), do: {:ok, identity}

  @doc "Returns the sentinel data artifact path (the bundle root)."
  @spec artifact_path(binary()) :: binary()
  def artifact_path(bundle_root) when is_binary(bundle_root), do: Path.expand(bundle_root)

  @doc "Starts sentinel ownership for `bundle_root`."
  @spec start_ownership(binary()) :: GenServer.on_start()
  def start_ownership(bundle_root) when is_binary(bundle_root) do
    Ownership.start_link(artifact_path(bundle_root))
  end

  @doc "Sentinel has no engine capability requirements."
  @spec validate_capabilities!() :: :ok
  def validate_capabilities!, do: :ok

  @doc "Returns opaque sentinel capability metadata."
  @spec capabilities_report() :: map()
  def capabilities_report, do: %{engine: "sentinel"}

  alias ElixirDB.Storage.BackendContext
  alias ElixirDB.Storage.OpaqueHandle

  @doc "Wraps a sentinel adapter in an opaque backend context."
  @spec to_context(t()) :: BackendContext.t()
  def to_context(%__MODULE__{} = adapter) do
    BackendContext.new(
      backend: __MODULE__,
      backend_ref: OpaqueHandle.wrap(adapter),
      bundle_root: adapter.root,
      capabilities: %{engine: "sentinel"},
      identity: adapter.identity
    )
  end

  @doc "Returns the sentinel module that implements `family`."
  @spec port(atom()) :: module()
  def port(family) when is_atom(family) do
    Map.fetch!(port_modules(), family)
  end

  @doc "Returns the sentinel port composition map for implemented families."
  @spec port_modules() :: %{atom() => module()}
  def port_modules do
    %{
      lifecycle: ElixirDB.Storage.Sentinel.Lifecycle,
      transaction: ElixirDB.Storage.Sentinel.Transaction,
      ownership: ElixirDB.Storage.Sentinel.OwnershipPort
    }
  end

  @doc "Transaction port entry used by `ElixirDB.Storage.Transaction.run/2`."
  @spec run_transaction(BackendContext.t(), (BackendContext.t() -> term())) ::
          {:ok, term()} | {:error, ElixirDB.Error.t()}
  def run_transaction(%BackendContext{} = context, fun) when is_function(fun, 1) do
    Transaction.run(context, fun)
  end

  @doc "Returns the sentinel transaction port module."
  @spec transaction_port() :: module()
  def transaction_port, do: Transaction

  defp build(path, uuid) do
    %__MODULE__{
      root: Path.expand(path),
      closed?: false,
      identity: %{
        database_uuid: uuid,
        database_kind: :ordinary,
        backend: "sentinel",
        engine: "none"
      }
    }
  end

  defp normalize_options(options) when is_map(options), do: options
  defp normalize_options(_), do: %{}

  defp ensure_root(path) do
    case File.mkdir_p(path) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, ElixirDB.Error.internal_error("cannot create sentinel root: #{inspect(reason)}")}
    end
  end

  defp ensure_existing_root(path) do
    marker = marker_path(path)

    if File.dir?(path) and File.regular?(marker) do
      :ok
    else
      {:error, ElixirDB.Error.invalid_request("sentinel backend marker is missing")}
    end
  end

  defp write_marker(path, uuid) do
    case File.write(marker_path(path), uuid) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, ElixirDB.Error.internal_error("cannot write sentinel marker: #{inspect(reason)}")}
    end
  end

  defp read_marker(path) do
    case File.read(path) do
      {:ok, uuid} ->
        uuid = String.trim(uuid)
        with :ok <- RequestValidation.validate_uuid(uuid), do: {:ok, uuid}

      {:error, _} ->
        {:error, ElixirDB.Error.invalid_request("sentinel backend marker is missing")}
    end
  end

  defp marker_path(path), do: Path.join(path, "sentinel.backend")

  defp unsupported(operation) do
    {:error, ElixirDB.Error.invalid_request("sentinel backend does not implement #{operation}")}
  end
end
