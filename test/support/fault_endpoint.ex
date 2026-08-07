defmodule ElixirDB.FaultEndpoint do
  @moduledoc """
  Thin fault-injecting endpoint wrapper around `LocalEndpoint` (Plan §12.4).

  Schedules retryable failures at named points via `ElixirDB.FaultAdapter` without
  changing production endpoint modules. Injection points:

  * before each Endpoint callback (`:identity`, `:read_changes`, …)
  * after a successful callback (`:after_identity`, `:after_read_changes`, …)
  """
  @behaviour ElixirDB.Replication.Endpoint

  alias ElixirDB.FaultAdapter
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
  Returns hit counts for debugging failed fault histories (Plan §12.7).
  """
  @spec hits(t()) :: %{optional(atom()) => non_neg_integer()}
  def hits(%__MODULE__{agent: agent}) do
    Agent.get(agent, & &1.hits)
  end

  @impl true
  def identity(%__MODULE__{} = endpoint),
    do: invoke(endpoint, :identity, &LocalEndpoint.identity/1)

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
  def put_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      invoke(
        endpoint,
        :put_checkpoint,
        &LocalEndpoint.put_checkpoint(&1, replication_id, checkpoint)
      )

  defp invoke(%__MODULE__{agent: agent}, point, fun) when is_function(fun, 1) do
    Agent.get_and_update(agent, &invoke_with_fault(&1, point, fun))
  end

  defp invoke_with_fault(adapter, point, fun) do
    case FaultAdapter.maybe_fail(adapter, point) do
      {:error, error, adapter} -> {{:error, error}, adapter}
      {:ok, adapter} -> invoke_success(adapter, point, fun)
    end
  end

  defp invoke_success(adapter, point, fun) do
    case fun.(adapter.inner) do
      {:ok, _} = ok -> invoke_after_success(adapter, point, ok)
      other -> {other, adapter}
    end
  end

  defp invoke_after_success(adapter, point, ok) do
    case FaultAdapter.maybe_fail(adapter, after_point(point)) do
      {:ok, adapter} -> {ok, adapter}
      {:error, error, adapter} -> {{:error, error}, adapter}
    end
  end

  defp after_point(point), do: :"after_#{point}"
end
