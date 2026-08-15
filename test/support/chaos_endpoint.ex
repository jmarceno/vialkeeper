defmodule VialKeeper.ChaosEndpoint do
  @moduledoc """
  Deterministic chaos wrapper around `VialKeeper.Replication.LocalEndpoint`.

  Each callback draws one action from endpoint-local `:rand` state held by an
  `Agent`. The default weights are 70% pass, 10% error, 10% delay, 5% duplicate,
  and 5% reorder. Custom weights replace the defaults; omitted actions then
  have zero weight.

  Endpoint callbacks are synchronous, so retaining an import until a later
  callback would deadlock the caller. `:reorder` instead reverses the current
  import request's `chains` list and performs one real import. This stresses
  arrival-order handling without acknowledging data that was not imported.

  Duplicate execution is limited to revision-chain imports and checkpoint
  reads. Checkpoint writes are never duplicated or reordered. An action that
  is not legal for a callback is normalized to `:pass` before it is counted.
  """

  @behaviour VialKeeper.Replication.Endpoint

  alias VialKeeper.Error
  alias VialKeeper.Replication.LocalEndpoint

  @actions [:pass, :error, :delay, :duplicate, :reorder]
  @ordinary_actions [:pass, :error, :delay]
  @default_weights %{pass: 70, error: 10, delay: 10, duplicate: 5, reorder: 5}
  @default_delay_ms 5..50

  defstruct [:inner, :agent]

  @type action :: :pass | :error | :delay | :duplicate | :reorder
  @type weights :: %{required(action()) => non_neg_integer()}
  @type t :: %__MODULE__{inner: LocalEndpoint.t(), agent: pid()}

  @doc """
  Wraps a local endpoint with deterministic chaos.

  Options:

    * `:seed` - integer seed, defaulting to `0`
    * `:weights` - map or keyword list of action weights
    * `:delay_ms` - inclusive range, `{minimum, maximum}`, or fixed integer
  """
  @spec wrap(LocalEndpoint.t(), keyword()) :: t()
  def wrap(%LocalEndpoint{} = inner, opts \\ []) when is_list(opts) do
    seed = Keyword.get(opts, :seed, 0)
    weights = opts |> Keyword.get(:weights, @default_weights) |> normalize_weights()
    delay_ms = opts |> Keyword.get(:delay_ms, @default_delay_ms) |> normalize_delay()

    unless is_integer(seed) do
      raise ArgumentError, "chaos seed must be an integer"
    end

    {:ok, agent} =
      Agent.start_link(fn ->
        %{
          seed: seed,
          random_state: :rand.seed_s(:exsss, {seed, 0, 0}),
          weights: weights,
          delay_ms: delay_ms,
          stats: Map.new(@actions, &{&1, 0}),
          effective_reorders: 0
        }
      end)

    %__MODULE__{inner: inner, agent: agent}
  end

  @doc "Returns action counts after callback-specific legality normalization."
  @spec stats(t()) :: weights()
  def stats(%__MODULE__{agent: agent}), do: Agent.get(agent, & &1.stats)

  @doc "Returns the deterministic seed used by this endpoint."
  @spec seed(t()) :: integer()
  def seed(%__MODULE__{agent: agent}), do: Agent.get(agent, & &1.seed)

  @doc "Returns reorder actions that changed the order of at least two imported chains."
  @spec effective_reorders(t()) :: non_neg_integer()
  def effective_reorders(%__MODULE__{agent: agent}),
    do: Agent.get(agent, & &1.effective_reorders)

  @impl true
  def identity(%__MODULE__{} = endpoint),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.identity/1)

  @impl true
  def read_changes(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.read_changes(&1, request))

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.has_local_origin_changes?/1)

  @impl true
  def has_local_origin_changes?(%__MODULE__{} = endpoint, peer_database_uuid),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.has_local_origin_changes?(&1, peer_database_uuid)
      )

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.clear_pending_local_causal/1)

  @impl true
  def clear_pending_local_causal(%__MODULE__{} = endpoint, peer_database_uuid),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.clear_pending_local_causal(&1, peer_database_uuid)
      )

  @impl true
  def diff_revisions(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.diff_revisions(&1, request))

  @impl true
  def get_revision_chains(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.get_revision_chains(&1, request))

  @impl true
  def import_revision_chains(%__MODULE__{} = endpoint, request) do
    legal_actions =
      if is_list(chains(request)),
        do: @actions,
        else: [:pass, :error, :delay, :duplicate]

    case choose(endpoint, legal_actions) do
      {:reorder, _delay_ms} ->
        record_effective_reorder(endpoint, request)
        LocalEndpoint.import_revision_chains(endpoint.inner, reverse_chains(request))

      decision ->
        execute(
          decision,
          endpoint.inner,
          &LocalEndpoint.import_revision_chains(&1, request)
        )
    end
  end

  @impl true
  def confirm_durable_commit(%__MODULE__{} = endpoint, request),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.confirm_durable_commit(&1, request)
      )

  @impl true
  def get_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do:
      invoke(
        endpoint,
        [:pass, :error, :delay, :duplicate],
        &LocalEndpoint.get_checkpoint(&1, replication_id)
      )

  @impl true
  def get_shadow_checkpoint(%__MODULE__{} = endpoint, replication_id),
    do:
      invoke(
        endpoint,
        [:pass, :error, :delay, :duplicate],
        &LocalEndpoint.get_shadow_checkpoint(&1, replication_id)
      )

  @impl true
  def get_local_record(%__MODULE__{} = endpoint, namespace, key) do
    legal_actions =
      if namespace in ["checkpoints", "shadow_checkpoints"],
        do: [:pass, :error, :delay, :duplicate],
        else: @ordinary_actions

    invoke(endpoint, legal_actions, &LocalEndpoint.get_local_record(&1, namespace, key))
  end

  @impl true
  def put_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.put_checkpoint(&1, replication_id, checkpoint)
      )

  @impl true
  def put_shadow_checkpoint(%__MODULE__{} = endpoint, replication_id, checkpoint),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.put_shadow_checkpoint(&1, replication_id, checkpoint)
      )

  @impl true
  def read_boundary_pages(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.read_boundary_pages(&1, request))

  @impl true
  def install_boundary_pages(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.install_boundary_pages(&1, request))

  @impl true
  def put_peer_position(%__MODULE__{} = endpoint, request),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.put_peer_position(&1, request))

  @impl true
  def list_peer_positions(%__MODULE__{} = endpoint),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.list_peer_positions/1)

  @impl true
  def diff_blobs(%__MODULE__{} = endpoint, digests),
    do: invoke(endpoint, @ordinary_actions, &LocalEndpoint.diff_blobs(&1, digests))

  @impl true
  def open_blob_representation(%__MODULE__{} = endpoint, digest),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.open_blob_representation(&1, digest)
      )

  @impl true
  def put_blob_representation(%__MODULE__{} = endpoint, stream),
    do:
      invoke(
        endpoint,
        @ordinary_actions,
        &LocalEndpoint.put_blob_representation(&1, stream)
      )

  defp invoke(%__MODULE__{} = endpoint, legal_actions, callback) do
    endpoint
    |> choose(legal_actions)
    |> execute(endpoint.inner, callback)
  end

  defp execute({:pass, _delay_ms}, inner, callback), do: callback.(inner)

  defp execute({:error, _delay_ms}, _inner, _callback),
    do: {:error, Error.database_unavailable("chaos injected")}

  defp execute({:delay, delay_ms}, inner, callback) do
    Process.sleep(delay_ms)
    callback.(inner)
  end

  defp execute({:duplicate, _delay_ms}, inner, callback) do
    case callback.(inner) do
      :ok -> callback.(inner)
      {:ok, _result} -> callback.(inner)
      first_result -> first_result
    end
  end

  defp choose(%__MODULE__{agent: agent}, legal_actions) do
    Agent.get_and_update(agent, fn state ->
      {raw_action, random_state} = draw_weighted(state.weights, state.random_state)
      action = if raw_action in legal_actions, do: raw_action, else: :pass
      {delay_ms, random_state} = draw_delay(action, state.delay_ms, random_state)
      stats = Map.update!(state.stats, action, &(&1 + 1))

      {{action, delay_ms}, %{state | random_state: random_state, stats: stats}}
    end)
  end

  defp draw_weighted(weights, random_state) do
    total = total_weight(weights)
    {ticket, random_state} = :rand.uniform_s(total, random_state)
    {pick_weighted(@actions, weights, ticket), random_state}
  end

  defp pick_weighted([action | actions], weights, ticket) do
    weight = Map.fetch!(weights, action)

    if ticket <= weight do
      action
    else
      pick_weighted(actions, weights, ticket - weight)
    end
  end

  defp draw_delay(:delay, {minimum, maximum}, random_state) do
    {offset, random_state} = :rand.uniform_s(maximum - minimum + 1, random_state)
    {minimum + offset - 1, random_state}
  end

  defp draw_delay(_action, _range, random_state), do: {nil, random_state}

  defp reverse_chains(request) do
    reversed = request |> chains() |> Enum.reverse()

    cond do
      Map.has_key?(request, :chains) -> Map.put(request, :chains, reversed)
      Map.has_key?(request, "chains") -> Map.put(request, "chains", reversed)
      true -> request
    end
  end

  defp chains(request) when is_map(request) do
    cond do
      Map.has_key?(request, :chains) -> Map.get(request, :chains)
      Map.has_key?(request, "chains") -> Map.get(request, "chains")
      true -> nil
    end
  end

  defp chains(_request), do: nil

  defp record_effective_reorder(%__MODULE__{agent: agent}, request) do
    case chains(request) do
      [_first, _second | _rest] ->
        Agent.update(agent, &Map.update!(&1, :effective_reorders, fn count -> count + 1 end))

      _chains ->
        :ok
    end
  end

  defp normalize_weights(weights) when is_list(weights),
    do: weights |> Map.new() |> normalize_weights()

  defp normalize_weights(weights) when is_map(weights) do
    unknown_actions = Map.keys(weights) -- @actions
    normalized = Map.new(@actions, &{&1, Map.get(weights, &1, 0)})

    unless unknown_actions == [] and
             Enum.all?(normalized, fn {_action, weight} ->
               is_integer(weight) and weight >= 0
             end) and total_weight(normalized) > 0 do
      raise ArgumentError,
            "chaos weights must contain known actions with non-negative integer values and a positive total"
    end

    normalized
  end

  defp normalize_weights(_weights) do
    raise ArgumentError, "chaos weights must be a map or keyword list"
  end

  defp total_weight(weights) do
    Enum.reduce(weights, 0, fn {_action, weight}, total -> total + weight end)
  end

  defp normalize_delay(%Range{first: minimum, last: maximum, step: 1}),
    do: normalize_delay({minimum, maximum})

  defp normalize_delay(delay_ms) when is_integer(delay_ms),
    do: normalize_delay({delay_ms, delay_ms})

  defp normalize_delay({minimum, maximum})
       when is_integer(minimum) and is_integer(maximum) and minimum >= 0 and maximum >= minimum,
       do: {minimum, maximum}

  defp normalize_delay(_delay_ms) do
    raise ArgumentError,
          "chaos delay_ms must be a non-negative integer, ascending range, or {minimum, maximum}"
  end
end
