defmodule ElixirDB.Shadow.Reconciler do
  @moduledoc "Isolated source-local reconciler for one managed shadow definition."
  use GenServer

  alias ElixirDB.JSON.Canonical

  alias ElixirDB.Shadow.{
    ControllerSupervisor,
    Definition,
    LocalEndpoint,
    Observation,
    Protocol,
    RemoteEndpoint,
    RouteTable
  }

  alias ElixirDB.Shadow.Registry, as: ShadowRegistry
  alias Registry, as: ControllerRegistry

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @doc "Enqueues a definition, starting one source-local reconciler if needed."
  @spec enqueue(Definition.t()) :: :ok | {:error, term()}
  def enqueue(%Definition{} = definition) do
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

  @impl true
  def init(%{definition: %Definition{} = definition} = opts) do
    case ControllerRegistry.register(
           ElixirDB.Shadow.ControllerRegistry,
           definition.source_uuid,
           %{}
         ) do
      {:ok, _} ->
        send(self(), :reconcile)
        {:ok, %{definition: definition, options: opts}}

      {:error, {:already_registered, _pid}} ->
        {:stop, :already_started}
    end
  end

  @impl true
  def handle_cast({:definition, %Definition{} = definition}, state) do
    send(self(), :reconcile)
    {:noreply, %{state | definition: definition}}
  end

  @impl true
  def handle_info(:reconcile, %{definition: %Definition{enabled: false} = definition} = state) do
    RouteTable.delete(definition.source_uuid)
    observe(definition, :destroying, nil)
    _ = destroy(definition, state)
    {:noreply, state}
  end

  def handle_info(:reconcile, %{definition: definition} = state) do
    RouteTable.delete(definition.source_uuid)
    reconcile_definition(definition)
    {:noreply, state}
  end

  defp reconcile_definition(definition) do
    case endpoint(definition) do
      {:ok, endpoint} ->
        reconcile_endpoint(definition, endpoint)

      {:error, error} ->
        observe_error(definition, error)
    end
  end

  defp reconcile_endpoint(definition, endpoint) do
    request = provision_request(definition)
    observe(definition, :provisioning, nil)

    case provision_and_inspect(endpoint, request) do
      {:ok, inspected} ->
        observation_state = if inspected["state"] == "ready", do: :ready, else: :bootstrapping
        observe(definition, observation_state, inspected)
        maybe_route(definition, endpoint, inspected)

      {:error, error} ->
        observe_error(definition, error)
    end
  end

  defp provision_and_inspect(endpoint, request) do
    timeout = control_timeout(endpoint)

    with {:ok, capabilities} <- __MODULE__.EndpointCapabilities.capabilities(endpoint, timeout),
         :ok <- Protocol.ensure_compatible(capabilities),
         {:ok, _provisioned} <-
           __MODULE__.EndpointCapabilities.provision(endpoint, request, timeout) do
      __MODULE__.EndpointCapabilities.inspect(endpoint, request, timeout)
    end
  end

  defp endpoint(definition) do
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

    base = %{
      "source_uuid" => definition.source_uuid,
      "shadow_uuid" => definition.shadow_uuid,
      "generation" => definition.generation,
      "operation_id" => definition.operation_id,
      "attachment_store_type" => "external_cas",
      "attachment_location" => definition.attachment_location,
      "source_base_url" => source_base_url,
      "source_bearer_token" => source_bearer_token
    }

    Map.put(base, "specification_digest", definition.specification_digest || digest(base))
  end

  defp digest(value) do
    {:ok, encoded} = Canonical.encode(value)
    :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)
  end

  defp maybe_route(%Definition{} = definition, endpoint, %{"state" => "ready"} = inspected) do
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
  end

  defp maybe_route(_definition, _endpoint, _inspected), do: :ok

  defp destroy(definition, _state) do
    case endpoint(definition) do
      {:ok, endpoint} ->
        __MODULE__.EndpointCapabilities.destroy(
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

  defp control_timeout(%LocalEndpoint{}), do: ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000
  defp control_timeout(%RemoteEndpoint{control_timeout_ms: timeout}), do: timeout

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
