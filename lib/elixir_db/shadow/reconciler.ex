defmodule ElixirDB.Shadow.Reconciler do
  @moduledoc "Isolated source-local reconciler for one managed shadow definition."
  use GenServer

  alias ElixirDB.JSON.Canonical
  alias ElixirDB.Runtime.DatabaseCatalog

  alias ElixirDB.Shadow.{
    ControllerSupervisor,
    ControlTaskSupervisor,
    Definition,
    LocalEndpoint,
    Observation,
    Protocol,
    RemoteEndpoint,
    RouteTable
  }

  alias ElixirDB.Shadow.Reconciler.EndpointCapabilities
  alias ElixirDB.Shadow.Registry, as: ShadowRegistry
  alias Registry, as: ControllerRegistry

  @retry_ms 1_000

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Enqueues a definition, starting one source-local reconciler if needed."
  @spec enqueue(Definition.t()) :: :ok | {:error, term()}
  def enqueue(%Definition{} = definition) do
    if controller_enabled?() do
      start_or_notify(definition)
    else
      :ok
    end
  end

  @doc "Starts reconcilers for durable enabled definitions and unresolved orphans."
  @spec recover_desired() :: :ok
  def recover_desired do
    if controller_enabled?() do
      Enum.each(ShadowRegistry.snapshot(), fn {_source_uuid, entry} -> recover_entry(entry) end)
    end

    :ok
  end

  @doc "Asks the source-local reconciler to re-inspect after a read-path invalidation."
  @spec notify_invalidation(binary(), map()) :: :ok
  def notify_invalidation(source_uuid, _snapshot) when is_binary(source_uuid) do
    notify(source_uuid, :invalidate)
  end

  @doc "Asks the source-local reconciler to suspend routing after source close."
  @spec notify_source_closed(binary()) :: :ok
  def notify_source_closed(source_uuid) when is_binary(source_uuid) do
    notify(source_uuid, :source_closed)
  end

  defp notify(source_uuid, message) do
    case ControllerRegistry.lookup(ElixirDB.Shadow.ControllerRegistry, source_uuid) do
      [{pid, _}] ->
        GenServer.cast(pid, message)
        :ok

      [] ->
        case ShadowRegistry.get(source_uuid) do
          {:ok, %{desired: %Definition{enabled: true} = definition}} ->
            enqueue(definition)

          {:ok, %{desired: %Definition{} = definition, orphans: orphans}}
          when is_list(orphans) and orphans != [] ->
            enqueue(definition)

          _ ->
            :ok
        end
    end
  end

  defp start_or_notify(%Definition{} = definition) do
    case ControllerRegistry.lookup(ElixirDB.Shadow.ControllerRegistry, definition.source_uuid) do
      [{pid, _}] ->
        GenServer.cast(pid, {:definition, definition})
        :ok

      [] ->
        case ControllerSupervisor.start_controller(%{definition: definition}) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp recover_entry(%{desired: %Definition{enabled: true} = definition}), do: enqueue(definition)

  defp recover_entry(%{desired: %Definition{} = definition, orphans: orphans})
       when is_list(orphans) and orphans != [],
       do: enqueue(definition)

  defp recover_entry(_entry), do: :ok

  defp controller_enabled?,
    do: Keyword.get(Application.get_env(:elixir_db, :shadow_controller, []), :enabled, false)

  @impl true
  def init(%{definition: %Definition{} = definition} = opts) do
    case ControllerRegistry.register(
           ElixirDB.Shadow.ControllerRegistry,
           definition.source_uuid,
           %{}
         ) do
      {:ok, _} ->
        send(self(), :reconcile)

        {:ok,
         %{
           definition: definition,
           endpoint: Map.get(opts, :endpoint),
           task: nil,
           dirty: false
         }}

      {:error, {:already_registered, _pid}} ->
        {:stop, :already_started}
    end
  end

  @impl true
  def handle_cast({:definition, %Definition{} = definition}, %{task: nil} = state) do
    send(self(), :reconcile)
    {:noreply, %{state | definition: definition, dirty: false}}
  end

  def handle_cast({:definition, %Definition{} = definition}, state) do
    {:noreply, %{state | definition: definition, dirty: true}}
  end

  def handle_cast(:invalidate, %{task: nil} = state) do
    send(self(), :reconcile)
    {:noreply, state}
  end

  def handle_cast(:invalidate, state), do: {:noreply, %{state | dirty: true}}

  def handle_cast(:source_closed, state) do
    _ = RouteTable.compare_delete(state.definition.source_uuid, Definition.token(state.definition))
    {:noreply, state}
  end

  @impl true
  def handle_info(:reconcile, %{task: %Task{}} = state), do: {:noreply, state}

  def handle_info(:reconcile, %{definition: %Definition{enabled: false} = definition} = state) do
    _ = RouteTable.compare_delete(definition.source_uuid, Definition.token(definition))
    observe(definition, :destroying, nil)
    start_control_task(state, fn -> {:destroyed, destroy(definition, state)} end)
  end

  def handle_info(:reconcile, state) do
    start_control_task(state, fn -> inspect_or_provision(state.definition, state) end)
  end

  def handle_info({ref, result}, %{task: %Task{ref: ref}} = state) do
    _ = Process.demonitor(ref, [:flush])
    apply_control_result(result, %{state | task: nil})
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{task: %Task{ref: ref}} = state) do
    observe_error(
      state.definition,
      ElixirDB.Error.database_unavailable("shadow control task stopped", %{cause: reason})
    )

    schedule_retry(%{state | task: nil})
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp start_control_task(state, fun) do
    {:noreply, %{state | task: ControlTaskSupervisor.async(fun), dirty: false}}
  end

  defp apply_control_result({:ready, generation, endpoint, inspected}, state) do
    if current_generation?(state.definition, generation) and state.definition.enabled do
      observe(state.definition, :ready, inspected)
      maybe_route(state.definition, endpoint, inspected)
    end

    schedule_retry(state)
  end

  defp apply_control_result({:bootstrapping, generation, inspected}, state) do
    if current_generation?(state.definition, generation) and state.definition.enabled do
      _ =
        RouteTable.compare_delete(state.definition.source_uuid, Definition.token(state.definition))

      observe(state.definition, :bootstrapping, inspected)
    end

    schedule_retry(state)
  end

  defp apply_control_result({:destroyed, _result}, state) do
    observe(state.definition, :absent, nil)
    continue(state)
  end

  defp apply_control_result({:error, generation, error}, state) do
    if current_generation?(state.definition, generation) do
      _ =
        RouteTable.compare_delete(state.definition.source_uuid, Definition.token(state.definition))

      observe_error(state.definition, error)
    end

    schedule_retry(state)
  end

  defp apply_control_result(_other, state), do: continue(state)

  defp continue(%{dirty: true} = state) do
    send(self(), :reconcile)
    {:noreply, %{state | dirty: false}}
  end

  defp continue(state), do: {:noreply, state}

  defp schedule_retry(state) do
    if state.definition.enabled, do: Process.send_after(self(), :reconcile, @retry_ms)
    continue(state)
  end

  defp current_generation?(%Definition{generation: generation}, generation), do: true
  defp current_generation?(_definition, _generation), do: false

  defp inspect_or_provision(definition, state) do
    generation = definition.generation

    case endpoint(definition, state) do
      {:ok, endpoint} ->
        inspect_endpoint(definition, endpoint, generation)

      {:error, error} ->
        {:error, generation, error}
    end
  end

  defp inspect_endpoint(definition, endpoint, generation) do
    request = provision_request(definition)
    timeout = control_timeout(endpoint)

    with {:ok, capabilities} <- EndpointCapabilities.capabilities(endpoint, timeout),
         :ok <- Protocol.ensure_compatible(capabilities),
         {:ok, inspected} <- EndpointCapabilities.inspect(endpoint, request, timeout) do
      case inspected["state"] do
        "ready" ->
          {:ready, generation, endpoint, inspected}

        "absent" ->
          provision_missing(definition, endpoint, request, timeout, generation)

        "bootstrapping" ->
          {:bootstrapping, generation, inspected}

        _other ->
          {:bootstrapping, generation, inspected}
      end
    else
      {:error, error} -> {:error, generation, error}
    end
  end

  defp provision_missing(definition, endpoint, request, timeout, generation) do
    observe(definition, :provisioning, nil)

    with {:ok, _provisioned} <- EndpointCapabilities.provision(endpoint, request, timeout),
         {:ok, inspected} <- EndpointCapabilities.inspect(endpoint, request, timeout) do
      case inspected["state"] do
        "ready" -> {:ready, generation, endpoint, inspected}
        _ -> {:bootstrapping, generation, inspected}
      end
    else
      {:error, error} -> {:error, generation, error}
    end
  end

  defp endpoint(_definition, %{endpoint: endpoint}) when not is_nil(endpoint), do: {:ok, endpoint}
  defp endpoint(definition, _state), do: configured_endpoint(definition)

  defp configured_endpoint(definition) do
    locations = Application.get_env(:elixir_db, :shadow_controller, [])[:location] || []

    case Enum.find(locations, &(&1[:name] == definition.location)) do
      %{kind: :local} -> LocalEndpoint.new(worker_options: [])
      %{kind: :remote} = location -> RemoteEndpoint.new(location)
      nil -> {:error, ElixirDB.Error.invalid_request("shadow location is not configured")}
    end
  end

  defp provision_request(definition) do
    source = Application.get_env(:elixir_db, :shadow_controller, [])
    source_base_url = Keyword.get(source, :source_base_url, "")
    source_bearer_token = Keyword.get(source, :source_bearer_token, "")

    digest_fields = %{
      "source_uuid" => definition.source_uuid,
      "shadow_uuid" => definition.shadow_uuid,
      "generation" => definition.generation,
      "operation_id" => definition.operation_id,
      "attachment_store_type" => "external_cas",
      "attachment_location" => definition.attachment_location,
      "source_base_url" => source_base_url
    }

    Map.merge(digest_fields, %{
      "source_bearer_token" => source_bearer_token,
      "specification_digest" => definition.specification_digest || digest(digest_fields)
    })
  end

  defp digest(value) do
    {:ok, encoded} = Canonical.encode(value)
    :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)
  end

  defp maybe_route(%Definition{} = definition, endpoint, %{"state" => "ready"} = inspected) do
    if DatabaseCatalog.ordinary_open?(definition.source_uuid) do
      {:ok, node_id} = Map.fetch(inspected, "worker_node_id")

      RouteTable.put(definition.source_uuid, %{
        endpoint: endpoint,
        source_uuid: definition.source_uuid,
        shadow_uuid: definition.shadow_uuid,
        generation: definition.generation,
        operation_id: definition.operation_id,
        worker_node_id: node_id,
        applied_source_sequence: inspected["applied_source_sequence"] || 0
      })
    else
      _ = RouteTable.compare_delete(definition.source_uuid, Definition.token(definition))
      :ok
    end
  end

  defp maybe_route(_definition, _endpoint, _inspected), do: :ok

  defp destroy(definition, options) do
    case endpoint(definition, options) do
      {:ok, endpoint} ->
        EndpointCapabilities.destroy(
          endpoint,
          Definition.token(definition),
          control_timeout(endpoint)
        )

      {:error, _} ->
        {:error, ElixirDB.Error.invalid_request("shadow location is not configured")}
    end
  end

  defp observe(definition, lifecycle, details) do
    {:ok, observation} =
      Observation.new(%{
        state: lifecycle,
        worker_node_id: if(is_map(details), do: details["worker_node_id"]),
        applied_source_sequence:
          if(is_map(details), do: details["applied_source_sequence"] || 0, else: 0),
        last_error_code: nil
      })

    _ =
      ShadowRegistry.apply_observation(
        definition.source_uuid,
        Definition.token(definition),
        observation
      )
  end

  defp observe_error(definition, %ElixirDB.Error{} = error) do
    {:ok, observation} =
      Observation.new(%{
        state: :unhealthy,
        last_error_code: error.code
      })

    _ =
      ShadowRegistry.apply_observation(
        definition.source_uuid,
        Definition.token(definition),
        observation
      )
  end

  defp observe_error(definition, _error),
    do: observe_error(definition, ElixirDB.Error.internal_error())

  defp control_timeout(%LocalEndpoint{}), do: ElixirDB.Config.request_timeout_ms()
  defp control_timeout(%RemoteEndpoint{control_timeout_ms: timeout}), do: timeout
  defp control_timeout(_), do: ElixirDB.Config.request_timeout_ms()

  defmodule EndpointCapabilities do
    @moduledoc "Dispatches the small callback set without making the reconciler depend on a transport."

    def capabilities(endpoint, timeout), do: endpoint.__struct__.capabilities(endpoint, timeout)

    def provision(endpoint, request, timeout),
      do: endpoint.__struct__.provision(endpoint, request, timeout)

    def inspect(endpoint, request, timeout),
      do: endpoint.__struct__.inspect(endpoint, request, timeout)

    def destroy(endpoint, request, timeout),
      do: endpoint.__struct__.destroy(endpoint, request, timeout)
  end
end
