defmodule VialKeeper.Storage.AdapterFacade do
  @moduledoc """
  Shared compatibility callbacks for storage adapters.

  The runtime uses `VialKeeper.Storage.Services` with an opaque
  `VialKeeper.Storage.BackendContext`; concrete adapters expose these callbacks
  only for the legacy adapter boundary and provide their own physical methods
  where required.
  """

  defmacro __using__(_opts) do
    definitions =
      [
        unary_callback(:retention_state, :retention_state),
        unary_callback(:list_peer_positions, :list_peer_positions),
        unary_callback(:list_views, :list_views),
        unary_callback(:get_derived_view, :get_derived_view),
        unary_callback(:list_derived_sources, :list_derived_sources),
        map_callback(:put_peer_position_cas, :put_peer_position_cas, "peer position"),
        map_callback(:read_boundary_pages, :read_boundary_pages, "boundary page"),
        map_callback(:install_boundary_pages, :install_boundary_pages, "boundary page"),
        map_callback(:resolve_attachment_ticket, :resolve_attachment_ticket, "attachment ticket"),
        map_callback(:resolve_blob_metadata, :resolve_blob_metadata, "blob metadata"),
        map_callback(:protect_pending_blob, :protect_pending_blob, "pending blob protection"),
        map_callback(
          :remove_pending_blob_protection,
          :remove_pending_blob_protection,
          "remove pending blob protection"
        ),
        map_callback(
          :list_live_attachment_digests,
          :list_live_attachment_digests,
          "live attachment digest"
        ),
        value_callback(:create_view, :create_view),
        value_callback(:delete_view, :delete_view),
        value_callback(:view_state, :view_state),
        value_callback(:apply_view_batch, :apply_view_batch),
        value_callback(:begin_view_rebuild, :begin_view_rebuild),
        value_callback(:append_view_rebuild_page, :append_view_rebuild_page),
        value_callback(:finish_view_rebuild, :finish_view_rebuild),
        value_callback(:query_view, :query_view),
        value_callback(:read_winning_documents_page, :read_winning_documents_page),
        value_callback(:set_derived_enabled, :set_derived_enabled),
        value_callback(:set_derived_source_error, :set_derived_source_error),
        value_callback(:apply_derived_source_batch, :apply_derived_source_batch),
        value_callback(:begin_derived_source_rebuild, :begin_derived_source_rebuild),
        value_callback(:apply_derived_rebuild_page, :apply_derived_rebuild_page),
        value_callback(:prune_derived_rebuild_stale_page, :prune_derived_rebuild_stale_page),
        value_callback(:finish_derived_source_rebuild, :finish_derived_source_rebuild),
        compact_callback(),
        cleanup_callback(),
        service_function(:diff_revisions, "revision diff"),
        service_function(:get_revision_chains, "revision chain")
      ]

    quote do
      alias VialKeeper.Storage.Services
      unquote_splicing(definitions)
    end
  end

  defp unary_callback(name, service) do
    quote do
      @impl true
      def unquote(name)(%__MODULE__{} = adapter),
        do: Services.unquote(service)(__MODULE__.to_context(adapter))
    end
  end

  defp map_callback(name, service, label) do
    quote do
      @impl true
      def unquote(name)(%__MODULE__{} = adapter, request) when is_map(request),
        do: Services.unquote(service)(__MODULE__.to_context(adapter), request)

      def unquote(name)(_adapter, _request),
        do:
          {:error, VialKeeper.Error.invalid_request(unquote(label) <> " request must be an object")}
    end
  end

  defp value_callback(name, service) do
    quote do
      @impl true
      def unquote(name)(%__MODULE__{} = adapter, value),
        do: Services.unquote(service)(__MODULE__.to_context(adapter), value)
    end
  end

  defp compact_callback do
    quote do
      @impl true
      def compact_retention(%__MODULE__{} = adapter, request \\ %{}) when is_map(request),
        do: Services.compact_retention(__MODULE__.to_context(adapter), request)
    end
  end

  defp cleanup_callback do
    quote do
      def cleanup_expired_pending_blobs(adapter, request \\ %{})

      @impl true
      def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, request) when is_map(request),
        do: Services.cleanup_expired_pending_blobs(__MODULE__.to_context(adapter), request)

      def cleanup_expired_pending_blobs(%__MODULE__{} = adapter, _request),
        do: cleanup_expired_pending_blobs(adapter, %{})
    end
  end

  defp service_function(name, label) do
    quote do
      def unquote(name)(%__MODULE__{} = adapter, request) when is_map(request),
        do: Services.unquote(name)(__MODULE__.to_context(adapter), request)

      def unquote(name)(_adapter, _request),
        do:
          {:error, VialKeeper.Error.invalid_request(unquote(label) <> " request must be an object")}
    end
  end
end
