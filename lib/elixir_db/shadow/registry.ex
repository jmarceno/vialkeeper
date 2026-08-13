defmodule ElixirDB.Shadow.Registry do
  @moduledoc "Durable host-local desired and observed shadow state."
  use GenServer

  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.Runtime.AtomicWrite
  alias ElixirDB.Shadow.{Definition, Observation}

  @version 1
  @default_name __MODULE__

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec path(Path.t()) :: Path.t()
  def path(root), do: Path.join(root, "shadow_control.json")

  @spec get(binary(), GenServer.server()) ::
          {:ok, map()} | :not_found | {:error, ElixirDB.Error.t()}
  def get(source_uuid, server \\ @default_name), do: GenServer.call(server, {:get, source_uuid})

  @spec put_desired(Definition.t(), GenServer.server()) ::
          {:ok, Definition.t()} | {:error, ElixirDB.Error.t()}
  def put_desired(definition, server \\ @default_name),
    do: GenServer.call(server, {:put_desired, definition})

  @spec disable(binary(), GenServer.server()) ::
          {:ok, Definition.t()} | {:error, ElixirDB.Error.t()}
  def disable(source_uuid, server \\ @default_name),
    do: GenServer.call(server, {:disable, source_uuid})

  @spec apply_observation(binary(), map(), Observation.t(), GenServer.server()) ::
          :ok | :stale | {:error, ElixirDB.Error.t()}
  def apply_observation(source_uuid, token, observation, server \\ @default_name),
    do: GenServer.call(server, {:apply_observation, source_uuid, token, observation})

  @spec record_orphan(binary(), map(), GenServer.server()) :: :ok | {:error, ElixirDB.Error.t()}
  def record_orphan(source_uuid, orphan, server \\ @default_name),
    do: GenServer.call(server, {:record_orphan, source_uuid, orphan})

  @spec snapshot(GenServer.server()) :: map()
  def snapshot(server \\ @default_name), do: GenServer.call(server, :snapshot)

  @spec integrity(GenServer.server()) :: :ok | {:error, ElixirDB.Error.t()}
  def integrity(server \\ @default_name), do: GenServer.call(server, :integrity)

  @impl true
  def init(opts) do
    root = Keyword.get(opts, :root, ElixirDB.Config.database_root())
    :ok = File.mkdir_p(root)

    case load(path(root)) do
      {:ok, sources} ->
        _ = ElixirDB.NodeIdentity.ensure(root)
        {:ok, %{root: root, path: path(root), sources: sources, integrity: :ok}}

      {:error, error} ->
        {:ok, %{root: root, path: path(root), sources: %{}, integrity: {:error, error}}}
    end
  end

  @impl true
  def handle_call({:get, source_uuid}, _from, state) do
    case Map.fetch(state.sources, source_uuid) do
      {:ok, entry} -> {:reply, {:ok, entry}, state}
      :error -> {:reply, :not_found, state}
    end
  end

  def handle_call({:put_desired, %Definition{} = definition}, _from, state) do
    with :ok <- ensure_integrity(state) do
      case persist_definition(state, definition) do
        {:ok, next} -> {:reply, {:ok, definition}, next}
        {:error, error} -> {:reply, {:error, error}, state}
      end
    end
  end

  def handle_call({:put_desired, _definition}, _from, state),
    do: {:reply, {:error, ElixirDB.Error.invalid_request("shadow desired state is invalid")}, state}

  def handle_call({:disable, source_uuid}, _from, state) do
    case Map.get(state.sources, source_uuid) do
      %{desired: %Definition{} = definition} ->
        disabled = Definition.disabled(definition)

        case persist_definition(state, disabled) do
          {:ok, next} -> {:reply, {:ok, disabled}, next}
          {:error, error} -> {:reply, {:error, error}, state}
        end

      _ ->
        {:reply,
         {:error, ElixirDB.Error.database_not_registered("shadow source is not configured")}, state}
    end
  end

  def handle_call(
        {:apply_observation, source_uuid, token, %Observation{} = observation},
        _from,
        state
      ) do
    apply_observation_state(state, source_uuid, token, observation)
  end

  def handle_call({:record_orphan, source_uuid, orphan}, _from, state) when is_map(orphan) do
    entry = Map.get(state.sources, source_uuid, empty_entry())
    orphans = Enum.uniq([orphan | entry.orphans])

    case persist_sources(state, Map.put(state.sources, source_uuid, %{entry | orphans: orphans})) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:record_orphan, _source_uuid, _orphan}, _from, state),
    do: {:reply, {:error, ElixirDB.Error.invalid_request("shadow orphan must be an object")}, state}

  def handle_call(:snapshot, _from, state), do: {:reply, state.sources, state}
  def handle_call(:integrity, _from, state), do: {:reply, state.integrity, state}

  defp apply_observation_state(state, source_uuid, token, observation) do
    case Map.get(state.sources, source_uuid) do
      %{desired: %Definition{} = definition} ->
        apply_observation_to_definition(state, source_uuid, token, observation, definition)

      _ ->
        {:reply, :stale, state}
    end
  end

  defp apply_observation_to_definition(state, source_uuid, token, observation, definition) do
    if token_matches?(definition, token),
      do: persist_observation(state, source_uuid, observation),
      else: {:reply, :stale, state}
  end

  defp persist_observation(state, source_uuid, observation) do
    entry = Map.put(Map.get(state.sources, source_uuid, empty_entry()), :observed, observation)

    case persist_sources(state, Map.put(state.sources, source_uuid, entry)) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  defp persist_definition(state, %Definition{} = definition) do
    entry =
      Map.put(Map.get(state.sources, definition.source_uuid, empty_entry()), :desired, definition)

    persist_sources(state, Map.put(state.sources, definition.source_uuid, entry))
  end

  defp persist_sources(state, sources) do
    with {:ok, json} <- encode(sources), :ok <- AtomicWrite.write(state.path, json) do
      {:ok, %{state | sources: sources, integrity: :ok}}
    else
      {:error, %ElixirDB.Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("shadow control state cannot be written", %{
           cause: inspect(reason)
         })}
    end
  end

  defp ensure_integrity(%{integrity: :ok}), do: :ok
  defp ensure_integrity(%{integrity: {:error, error}}), do: {:error, error}

  defp token_matches?(definition, token) when is_map(token) do
    token = string_keys(token)

    token["source_uuid"] == definition.source_uuid and
      token["generation"] == definition.generation and
      token["shadow_uuid"] == definition.shadow_uuid and
      token["operation_id"] == definition.operation_id
  end

  defp token_matches?(_, _), do: false

  defp empty_entry do
    {:ok, observation} = Observation.new()
    %{desired: nil, observed: observation, orphans: []}
  end

  defp encode(sources) do
    document = %{
      "version" => @version,
      "sources" =>
        Map.new(sources, fn {uuid, entry} ->
          {uuid,
           %{
             "desired" => if(entry.desired, do: Definition.to_map(entry.desired), else: nil),
             "observed" => Observation.to_map(entry.observed),
             "orphans" => entry.orphans
           }}
        end)
    }

    Canonical.encode(document)
  end

  defp load(path) do
    case File.read(path) do
      {:error, :enoent} ->
        {:ok, %{}}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.database_unavailable("shadow control state cannot be read", %{
           cause: inspect(reason)
         })}

      {:ok, body} ->
        with {:ok, %{"version" => @version, "sources" => sources}} <- StrictDecoder.decode(body),
             {:ok, parsed} <- parse_sources(sources) do
          {:ok, parsed}
        else
          _ -> {:error, ElixirDB.Error.integrity_violation("shadow control state is invalid")}
        end
    end
  end

  defp parse_sources(sources) when is_map(sources) do
    Enum.reduce_while(sources, {:ok, %{}}, &parse_source_entry/2)
  end

  defp parse_sources(_),
    do: {:error, ElixirDB.Error.integrity_violation("shadow control sources must be an object")}

  defp parse_definition(_source_uuid, nil), do: {:ok, nil}

  defp parse_definition(source_uuid, value),
    do: Definition.from_map(Map.put(value, "source_uuid", source_uuid))

  defp parse_source_entry({source_uuid, value}, {:ok, acc}) when is_map(value) do
    with {:ok, definition} <- parse_definition(source_uuid, value["desired"]),
         {:ok, observation} <- Observation.new(value["observed"] || %{}),
         true <- is_list(value["orphans"] || []) do
      {:cont,
       {:ok,
        Map.put(acc, source_uuid, %{
          desired: definition,
          observed: observation,
          orphans: value["orphans"] || []
        })}}
    else
      _ -> invalid_source_entry()
    end
  end

  defp parse_source_entry(_entry, _acc), do: invalid_source_entry()

  defp invalid_source_entry do
    {:halt,
     {:error, ElixirDB.Error.integrity_violation("shadow control state contains an invalid source")}}
  end

  defp string_keys(map),
    do:
      Map.new(map, fn {key, value} ->
        {if(is_atom(key), do: Atom.to_string(key), else: key), value}
      end)
end
