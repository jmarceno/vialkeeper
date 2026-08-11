defmodule ElixirDB.FaultEndpoint do
  @moduledoc """
  Thin fault-injecting endpoint wrapper around `LocalEndpoint`.

  Schedules retryable failures at named points via `ElixirDB.FaultAdapter` without
  changing production endpoint modules. Injection points:

  * before each Endpoint callback (`:identity`, `:read_changes`, …)
  * after a successful callback (`:after_identity`, `:after_read_changes`, …)
  """
  @behaviour ElixirDB.Replication.Endpoint

  alias ElixirDB.FaultAdapter
  alias ElixirDB.Replication.BlobStream
  alias ElixirDB.Replication.LocalEndpoint

  defstruct [:inner, :agent]

  @type t :: %__MODULE__{inner: LocalEndpoint.t(), agent: pid()}

  @doc """
  Wraps a `LocalEndpoint` with an empty fault schedule held in an Agent.
  """
  @spec wrap(LocalEndpoint.t()) :: t()
  def wrap(%LocalEndpoint{} = inner) do
    {:ok, agent} = Agent.start_link(fn -> FaultAdapter.wrap(inner) end)
    %__MODULE__{inner: inner, agent: agent}
  end

  @doc """
  Schedules a fault at a named injection point on this endpoint.
  """
  @spec inject(t(), atom(), FaultAdapter.fault()) :: t()
  def inject(%__MODULE__{agent: agent} = endpoint, point, fault) when is_atom(point) do
    :ok = Agent.update(agent, &FaultAdapter.inject(&1, point, fault))
    endpoint
  end

  @doc """
  Returns remaining scheduled faults (empty after one-shot faults fire).
  """
  @spec pending_faults(t()) :: %{optional(atom()) => FaultAdapter.fault()}
  def pending_faults(%__MODULE__{agent: agent}) do
    Agent.get(agent, & &1.faults)
  end

  @doc """
  Returns hit counts for debugging failed fault histories.
  """
  @spec hits(t()) :: %{optional(atom()) => non_neg_integer()}
  def hits(%__MODULE__{agent: agent}) do
    Agent.get(agent, & &1.hits)
  end

  @impl true
  def identity(%__MODULE__{} = endpoint),
    do: invoke(endpoint, :identity, &LocalEndpoint.identity/1)

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint),
    do: invoke(endpoint, :has_local_origin_changes?, &LocalEndpoint.has_local_origin_changes?/1)

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint, peer_database_uuid),
    do:
      invoke(endpoint, :has_local_origin_changes?, fn ep ->
        LocalEndpoint.has_local_origin_changes?(ep, peer_database_uuid)
      end)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint),
    do: invoke(endpoint, :clear_pending_local_causal, &LocalEndpoint.clear_pending_local_causal/1)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint, peer_database_uuid),
    do:
      invoke(endpoint, :clear_pending_local_causal, fn ep ->
        LocalEndpoint.clear_pending_local_causal(ep, peer_database_uuid)
      end)

  @impl true
  def read_changes(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, :read_changes, &LocalEndpoint.read_changes(&1, request))

  @impl true
  def diff_revisions(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, :diff_revisions, &LocalEndpoint.diff_revisions(&1, request))

  @impl true
  def get_revision_chains(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, :get_revision_chains, &LocalEndpoint.get_revision_chains(&1, request))

  @impl true
  def import_revision_chains(%__MODULE__{} = endpoint, request),
    do:
      invoke(endpoint, :import_revision_chains, &LocalEndpoint.import_revision_chains(&1, request))

  @impl true
  def confirm_durable_commit(%__MODULE__{} = endpoint, request),
    do:
      invoke(endpoint, :confirm_durable_commit, &LocalEndpoint.confirm_durable_commit(&1, request))

  @impl true
  def get_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do: invoke(endpoint, :get_checkpoint, &LocalEndpoint.get_checkpoint(&1, replication_id))

  @impl true
  def get_local_record(%__MODULE__{} = endpoint, namespace, key),
    do: invoke(endpoint, :get_local_record, &LocalEndpoint.get_local_record(&1, namespace, key))

  @impl true
  def put_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      invoke(
        endpoint,
        :put_checkpoint,
        &LocalEndpoint.put_checkpoint(&1, replication_id, checkpoint)
      )

  @impl true
  def read_boundary_pages(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, :read_boundary_pages, &LocalEndpoint.read_boundary_pages(&1, request))

  @impl true
  def install_boundary_pages(%__MODULE__{} = endpoint, request),
    do:
      invoke(endpoint, :install_boundary_pages, &LocalEndpoint.install_boundary_pages(&1, request))

  @impl true
  def put_peer_position(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, :put_peer_position, &LocalEndpoint.put_peer_position(&1, request))

  @impl true
  def list_peer_positions(%__MODULE__{} = endpoint),
    do: invoke(endpoint, :list_peer_positions, &LocalEndpoint.list_peer_positions/1)

  @impl true
  def diff_blobs(%__MODULE__{} = endpoint, digests),
    do: invoke(endpoint, :diff_blobs, &LocalEndpoint.diff_blobs(&1, digests))

  @impl true
  def open_blob(%__MODULE__{} = endpoint, digest),
    do:
      endpoint
      |> invoke(:open_blob, &LocalEndpoint.open_blob(&1, digest))
      |> maybe_inject_stream_fault(endpoint, :mid_source_stream)

  @impl true
  def put_blob(%__MODULE__{} = endpoint, stream),
    do:
      invoke(endpoint, :put_blob, fn inner ->
        LocalEndpoint.put_blob(inner, maybe_inject_target_stream(endpoint, stream))
      end)

  defp invoke(%__MODULE__{inner: inner, agent: agent}, point, fun) when is_function(fun, 1) do
    case Agent.get_and_update(agent, &fault_before(&1, point)) do
      {:error, error} ->
        {:error, error}

      :ok ->
        case fun.(inner) do
          :ok -> after_success(agent, point, :ok)
          {:ok, _} = ok -> after_success(agent, point, ok)
          other -> other
        end
    end
  end

  defp fault_before(adapter, point) do
    case FaultAdapter.maybe_fail(adapter, point) do
      {:error, error, adapter} -> {{:error, error}, adapter}
      {:ok, adapter} -> {:ok, adapter}
    end
  end

  defp after_success(agent, point, ok) do
    case Agent.get_and_update(agent, &fault_after(&1, point)) do
      :ok -> ok
      {:error, error} -> {:error, error}
    end
  end

  defp fault_after(adapter, point) do
    case FaultAdapter.maybe_fail(adapter, after_point(point)) do
      {:ok, adapter} -> {:ok, adapter}
      {:error, error, adapter} -> {{:error, error}, adapter}
    end
  end

  defp after_point(point), do: :"after_#{point}"

  defp maybe_inject_stream_fault({:ok, %BlobStream{} = stream}, endpoint, point) do
    case Agent.get_and_update(endpoint.agent, &stream_fault(&1, point)) do
      {:none, _adapter} -> {:ok, stream}
      {{:fault, %ElixirDB.Error{} = error}, _adapter} -> {:ok, faulty_stream(stream, error)}
    end
  end

  defp maybe_inject_stream_fault(result, _endpoint, _point), do: result

  defp maybe_inject_target_stream(endpoint, stream) do
    case Agent.get_and_update(endpoint.agent, &stream_fault(&1, :mid_target_stream)) do
      {:none, _adapter} -> stream
      {{:fault, %ElixirDB.Error{} = error}, _adapter} -> faulty_stream(stream, error)
    end
  end

  defp stream_fault(adapter, point) do
    case FaultAdapter.maybe_fail(adapter, point) do
      {:ok, adapter} -> {{:none, adapter}, adapter}
      {:error, error, adapter} -> {{{:fault, error}, adapter}, adapter}
    end
  end

  defp faulty_stream(stream, error) do
    body =
      stream.body
      |> Stream.concat([:elixir_db_stream_fault])
      |> Stream.transform(:pending, fn
        chunk, :pending -> {[chunk], :fault}
        :elixir_db_stream_fault, :fault -> exit({:elixir_db_transfer_stream_error, error})
        _chunk, :fault -> exit({:elixir_db_transfer_stream_error, error})
      end)

    %{stream | body: body}
  end
end
