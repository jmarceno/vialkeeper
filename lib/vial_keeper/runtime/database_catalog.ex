defmodule VialKeeper.Runtime.DatabaseCatalog do
  @moduledoc "Registration catalog and lazy database runtime manager."
  use GenServer
  require Logger
  alias VialKeeper.DatabaseBundle
  alias VialKeeper.Deadline
  alias VialKeeper.DerivedView.Manager, as: DerivedViewManager
  alias VialKeeper.Error
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Database
  alias VialKeeper.PathSafety
  alias VialKeeper.Query.SubscriptionHub
  alias VialKeeper.Replication.JobManager

  alias VialKeeper.Runtime.{
    AttachmentCoordinator,
    CatalogIndex,
    ChangeNotifier,
    CommandContext,
    CommandIO,
    DatabaseAdmission,
    DatabaseOwner,
    DatabaseRuntimeSupervisor,
    ReadPool,
    RegistrationManifest
  }

  alias VialKeeper.Shadow.Reconciler
  alias VialKeeper.Shadow.Registry, as: ShadowRegistry
  alias VialKeeper.Shadow.RouteTable
  alias VialKeeper.Storage.Registry, as: StorageRegistry
  alias VialKeeper.View.Manager

  @type catalog_reply :: {:ok, term()} | {:error, Error.t()}

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_args), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc "Creates an ordinary database using the configured control-plane timeout."
  @spec create(binary(), map(), timeout()) :: term()
  def create(relative_path, options \\ %{}, timeout \\ VialKeeper.Config.request_timeout_ms()),
    do: GenServer.call(__MODULE__, {:create, relative_path, options}, timeout)

  @doc "Creates a derived bundle through the trusted materialization boundary."
  @spec create_internal(binary()) :: catalog_reply()
  @spec create_internal(binary(), map()) :: catalog_reply()
  def create_internal(relative_path, options \\ %{}),
    do:
      GenServer.call(
        __MODULE__,
        {:create_internal, relative_path, options},
        VialKeeper.Config.request_timeout_ms()
      )

  @doc "Creates a shadow bundle through the trusted shadow controller boundary."
  @spec create_shadow_internal(binary()) :: catalog_reply()
  @spec create_shadow_internal(binary(), map()) :: catalog_reply()
  def create_shadow_internal(relative_path, options \\ %{}),
    do:
      GenServer.call(
        __MODULE__,
        {:create_shadow_internal, relative_path, options},
        VialKeeper.Config.request_timeout_ms()
      )

  @spec register(binary()) :: catalog_reply()
  def register(relative_path),
    do:
      GenServer.call(__MODULE__, {:register, relative_path}, VialKeeper.Config.request_timeout_ms())

  @doc "Registers a shadow bundle through the trusted shadow controller boundary."
  @spec register_shadow_internal(binary()) :: catalog_reply()
  def register_shadow_internal(relative_path),
    do:
      GenServer.call(
        __MODULE__,
        {:register_shadow_internal, relative_path},
        VialKeeper.Config.request_timeout_ms()
      )

  @spec unregister(binary()) :: catalog_reply() | :ok
  def unregister(uuid),
    do: GenServer.call(__MODULE__, {:unregister, uuid}, VialKeeper.Config.request_timeout_ms())

  @spec list() :: {:ok, [map()]} | {:error, Error.t()}
  def list, do: GenServer.call(__MODULE__, :list, VialKeeper.Config.request_timeout_ms())

  @doc "Returns whether an ordinary source is currently open for public routing."
  @spec ordinary_open?(binary()) :: boolean()
  def ordinary_open?(uuid) when is_binary(uuid) do
    match?(
      [{_pid, :ordinary}],
      Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid})
    )
  end

  @spec info(binary()) :: catalog_reply()
  def info(uuid) do
    case GenServer.call(__MODULE__, {:info, uuid}, VialKeeper.Config.request_timeout_ms()) do
      {:route, ^uuid} -> command(uuid, {:command, :identity, %{}}, 30_000)
      result -> result
    end
  end

  @doc "Opens a shadow bundle through an internal control-plane boundary."
  @spec open_internal(binary()) :: catalog_reply()
  def open_internal(uuid), do: GenServer.call(__MODULE__, {:open, uuid, true}, 30_000)

  @spec close(binary()) :: catalog_reply() | :ok
  def close(uuid), do: GenServer.call(__MODULE__, {:close, uuid}, 30_000)

  @doc "Returns the absolute bundle root for a registered database UUID."
  @spec bundle_root(binary()) :: {:ok, binary()} | {:error, Error.t()}
  def bundle_root(uuid),
    do: GenServer.call(__MODULE__, {:bundle_root, uuid}, VialKeeper.Config.request_timeout_ms())

  @doc "Returns a shadow bundle root through an internal control-plane boundary."
  @spec bundle_root_internal(binary()) :: {:ok, binary()} | {:error, Error.t()}
  def bundle_root_internal(uuid),
    do:
      GenServer.call(
        __MODULE__,
        {:bundle_root, uuid, true},
        VialKeeper.Config.request_timeout_ms()
      )

  # The database.open span is created here, in the caller process, before the
  # GenServer.call — so the span is a child of the caller's trace (e.g. the
  # HTTP request that triggered the open) and covers call latency.
  @spec open(binary()) :: catalog_reply()
  def open(uuid) do
    Database.open(uuid, fn ->
      GenServer.call(__MODULE__, {:open, uuid}, 30_000)
    end)
  end

  @doc """
  Ensures a database is registered and its runtime is open for command routing.

  This is a short host-global catalog call. Queue wait and owner execution happen
  outside the catalog GenServer via per-database admission.
  """
  @spec ensure_command_target(binary()) :: :ok | {:error, Error.t()}
  @spec ensure_command_target(binary(), Deadline.t()) :: :ok | {:error, Error.t()}
  def ensure_command_target(uuid) when is_binary(uuid) do
    ensure_command_target(uuid, Deadline.from_timeout(30_000))
  end

  def ensure_command_target(uuid, :infinity) when is_binary(uuid) do
    case catalog_index_target(uuid, false) do
      :open -> :ok
      :closing -> {:error, Error.database_closed("database is closing")}
      :miss -> GenServer.call(__MODULE__, {:ensure_command_target, uuid}, :infinity)
    end
  end

  def ensure_command_target(uuid, deadline_ms) when is_binary(uuid) do
    if Deadline.exhausted?(deadline_ms) do
      {:error, command_deadline_error()}
    else
      case catalog_index_target(uuid, false) do
        :open ->
          :ok

        :closing ->
          {:error, Error.database_closed("database is closing")}

        :miss ->
          GenServer.call(
            __MODULE__,
            {:ensure_command_target, uuid},
            Deadline.call_timeout(deadline_ms)
          )
      end
    end
  catch
    :exit, :timeout ->
      {:error, command_deadline_error()}

    :exit, {:timeout, {GenServer, :call, _}} ->
      {:error, command_deadline_error()}
  end

  @spec command(binary(), term(), timeout()) :: term() | {:error, Error.t()}
  def command(uuid, command, timeout \\ 30_000)

  def command(uuid, command, :infinity) when is_binary(uuid) do
    command_as(uuid, :foreground, command, :infinity)
  end

  def command(uuid, command, timeout) when is_binary(uuid) do
    command_as(uuid, :foreground, command, timeout)
  end

  @doc "Routes an internal command with explicit shadow authority and admission."
  @spec command_with_context(binary(), CommandContext.t(), term(), timeout()) :: term()
  def command_with_context(uuid, context, command, timeout \\ 30_000)

  def command_with_context(uuid, %CommandContext{} = context, command, :infinity)
      when is_binary(uuid) do
    class = if context.class == :shadow_replication, do: :replication, else: :foreground

    with :ok <- ensure_command_target_internal(uuid, :infinity) do
      Database.command(uuid, {:command_context, context, command}, fn ->
        route_command(uuid, class, {:command_context, context, command}, :infinity)
      end)
    end
  end

  def command_with_context(uuid, %CommandContext{} = context, command, timeout)
      when is_binary(uuid) and is_integer(timeout) and timeout >= 0 do
    deadline_ms = Deadline.from_timeout(timeout)
    class = if context.class == :shadow_replication, do: :replication, else: :foreground

    with :ok <- ensure_command_target_internal(uuid, deadline_ms) do
      Database.command(uuid, {:command_context, context, command}, fn ->
        route_command(uuid, class, {:command_context, context, command}, deadline_ms)
      end)
    end
  end

  defp ensure_command_target_internal(uuid, :infinity) do
    case catalog_index_target(uuid, true) do
      :open -> :ok
      :closing -> {:error, Error.database_closed("database is closing")}
      :miss -> GenServer.call(__MODULE__, {:ensure_command_target, uuid, true}, :infinity)
    end
  end

  defp ensure_command_target_internal(uuid, deadline_ms) do
    if Deadline.exhausted?(deadline_ms) do
      {:error, command_deadline_error()}
    else
      case catalog_index_target(uuid, true) do
        :open ->
          :ok

        :closing ->
          {:error, Error.database_closed("database is closing")}

        :miss ->
          GenServer.call(
            __MODULE__,
            {:ensure_command_target, uuid, true},
            Deadline.call_timeout(deadline_ms)
          )
      end
    end
  end

  defp catalog_index_target(uuid, allow_shadow) do
    case CatalogIndex.fetch(uuid) do
      {:ok, :shadow, :open} when allow_shadow -> :open
      {:ok, :shadow, :open} -> :miss
      {:ok, _kind, :open} -> :open
      {:ok, _kind, :closing} -> :closing
      :error -> :miss
    end
  end

  @doc "Routes a command using an already-created absolute deadline."
  @spec command_with_deadline(binary(), term(), Deadline.t()) ::
          term() | {:error, Error.t()}
  def command_with_deadline(uuid, command, deadline) when is_binary(uuid) do
    with :ok <- ensure_command_target(uuid, deadline) do
      Database.command(uuid, command, fn ->
        route_command(uuid, :foreground, command, deadline)
      end)
    end
  end

  @doc "Trusted internal command routing with an explicit admission service class."
  @spec command_as(binary(), DatabaseAdmission.service_class(), term(), timeout()) ::
          term() | {:error, Error.t()}
  def command_as(uuid, class, command, timeout \\ 30_000)

  def command_as(uuid, class, command, :infinity) when is_binary(uuid) do
    with :ok <- ensure_command_target(uuid, :infinity) do
      Database.command(uuid, command, fn ->
        route_command(uuid, class, command, :infinity)
      end)
    end
  end

  def command_as(uuid, class, command, timeout)
      when is_binary(uuid) and is_integer(timeout) and timeout >= 0 do
    deadline_ms = Deadline.from_timeout(timeout)

    with :ok <- ensure_command_target(uuid, deadline_ms) do
      Database.command(uuid, command, fn ->
        route_command(uuid, class, command, deadline_ms)
      end)
    end
  end

  @impl true
  def init(_) do
    root = VialKeeper.Config.database_root()
    :ok = File.mkdir_p(root)
    :ok = CatalogIndex.setup()
    state = %{root: root, entries: load_entries(), close_operations: %{}}
    Process.send_after(self(), :resume_registered_jobs, 0)
    {:ok, state}
  end

  # The registration manifest is routing-only, reconstructible state. If it cannot
  # be read cleanly — corrupt JSON, an invalid entry, an unsupported version, or
  # duplicate uuid/path — the catalog degrades to an empty registration set rather
  # than crashing. A poisoned manifest must never take the server down; the fault
  # is surfaced loudly for operators instead.
  defp load_entries do
    case RegistrationManifest.read() do
      {:ok, entries} ->
        Map.new(entries, &{&1.uuid, &1})

      {:error, %Error{code: code}} ->
        Logger.warning("registration manifest unreadable (#{code}); starting with an empty catalog")

        %{}
    end
  end

  defp create_database(state, relative_path, options, database_kind) when is_map(options) do
    case safe_path(state.root, relative_path) do
      {:ok, bundle_root} ->
        if File.exists?(bundle_root) do
          {:reply, {:error, Error.invalid_request("database bundle already exists")}, state}
        else
          create_new_bundle(state, relative_path, bundle_root, options, database_kind)
        end

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp create_database(state, _relative_path, _options, _database_kind) do
    {:reply, {:error, Error.invalid_request("database creation options must be an object")}, state}
  end

  defp create_new_bundle(state, relative_path, bundle_root, options, database_kind) do
    backend = StorageRegistry.backend()

    case DatabaseBundle.create(bundle_root) do
      {:ok, bundle} ->
        result =
          with {:ok, config} <-
                 VialKeeper.Config.merge_and_bound(
                   MapAccess.get(options, :config, VialKeeper.Config.defaults())
                 ),
               adapter_options <-
                 options
                 |> Map.put(:config, config)
                 |> Map.put(:database_kind, database_kind),
               {:ok, adapter} <-
                 backend.create(backend.artifact_path(DatabaseBundle.root(bundle)), adapter_options),
               {:ok, identity} <- backend.identity(adapter),
               :ok <- backend.close(adapter),
               {:ok, next} <-
                 put_entry(
                   state,
                   identity_uuid(identity),
                   relative_path,
                   bundle,
                   identity_kind(identity)
                 ) do
            {:ok, identity, next}
          else
            {:error, %Error{} = error} ->
              {:error, error}

            {:error, reason} ->
              {:error, Error.internal_error("database creation failed", %{cause: inspect(reason)})}
          end

        case result do
          {:ok, identity, next} ->
            {:reply, {:ok, identity}, next}

          {:error, error} ->
            _ = File.rm_rf(bundle_root)
            {:reply, {:error, error}, state}
        end

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}
    end
  end

  defp register_database(state, relative_path, allow_shadow) do
    backend = StorageRegistry.backend()

    with {:ok, bundle_root} <- safe_path(state.root, relative_path),
         true <- File.dir?(bundle_root),
         :ok <- ensure_path_not_open(state, bundle_root),
         {:ok, bundle} <- DatabaseBundle.validate(bundle_root),
         {:ok, adapter} <- backend.open(backend.artifact_path(DatabaseBundle.root(bundle))),
         {:ok, identity} <- backend.identity(adapter),
         :ok <- backend.close(adapter),
         :ok <- allow_shadow_or_reject(identity_kind(identity), allow_shadow),
         :ok <- no_duplicate_uuid(state, identity_uuid(identity), bundle_root),
         {:ok, next} <-
           put_entry(
             state,
             identity_uuid(identity),
             relative_path,
             bundle,
             identity_kind(identity)
           ) do
      {:reply, {:ok, identity}, next}
    else
      false ->
        {:reply, {:error, Error.database_unavailable("registered database bundle is missing")},
         state}

      {:error, %Error{} = error} ->
        {:reply, {:error, error}, state}

      {:error, reason} ->
        {:reply,
         {:error,
          Error.database_unavailable("database could not be registered", %{
            cause: inspect(reason)
          })}, state}
    end
  end

  defp allow_shadow_or_reject(:shadow, true), do: :ok

  defp allow_shadow_or_reject(:shadow, false),
    do: {:error, Error.shadow_database_hidden("shadow databases are internal")}

  defp allow_shadow_or_reject(_kind, _allow_shadow), do: :ok

  @impl true
  def handle_call({:create, relative_path, options}, _from, state) do
    safe(fn -> create_database(state, relative_path, options, :ordinary) end, state)
  end

  @impl true
  def handle_call({:create_internal, relative_path, options}, _from, state) do
    safe(
      fn ->
        if is_map(options) and MapAccess.get(options, :database_kind) == :derived do
          create_database(state, relative_path, options, :derived)
        else
          {:reply,
           {:error,
            Error.invalid_request("internal database creation requires the derived database kind")},
           state}
        end
      end,
      state
    )
  end

  @impl true
  def handle_call({:create_shadow_internal, relative_path, options}, _from, state) do
    safe(
      fn ->
        if is_map(options) and MapAccess.get(options, :database_kind) == :shadow do
          create_database(state, relative_path, options, :shadow)
        else
          {:reply,
           {:error,
            Error.invalid_request("internal shadow creation requires the shadow database kind")},
           state}
        end
      end,
      state
    )
  end

  @impl true
  def handle_call({:register, relative_path}, _from, state) do
    safe(fn -> register_database(state, relative_path, false) end, state)
  end

  @impl true
  def handle_call({:register_shadow_internal, relative_path}, _from, state) do
    safe(fn -> register_database(state, relative_path, true) end, state)
  end

  @impl true
  def handle_call({:unregister, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:reply, {:error, Error.database_not_registered("database is not registered")}, state}

      _entry ->
        case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [] ->
            drop_source_shadow_route(uuid, state)
            disable_source_shadow(uuid)
            CatalogIndex.delete(uuid)
            next = %{state | entries: Map.delete(state.entries, uuid)}
            unregister_entry(next, state)

          _ ->
            {:reply,
             {:error, Error.database_not_closable("database must be closed before unregistering")},
             state}
        end
    end
  end

  @impl true
  def handle_call(:list, _from, state),
    do:
      {:reply,
       {:ok,
        state.entries
        |> Map.values()
        |> Enum.reject(&shadow_entry?/1)
        |> Enum.map(&entry_status/1)}, state}

  @impl true
  def handle_call({:bundle_root, uuid}, _from, state),
    do: handle_bundle_root(state, uuid, false)

  @impl true
  def handle_call({:bundle_root, uuid, true}, _from, state),
    do: handle_bundle_root(state, uuid, true)

  @impl true
  def handle_call({:info, uuid}, _from, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:reply, {:error, Error.database_not_registered("database is not registered")}, state}

      %{database_kind: :shadow} ->
        {:reply, {:error, Error.shadow_database_hidden("shadow database is internal")}, state}

      entry ->
        case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [{_pid, _}] ->
            {:reply, {:route, uuid}, state}

          [] ->
            inspect_unregistered_entry(state, uuid, entry)
        end
    end
  end

  @impl true
  def handle_call({:open, uuid}, _from, state) do
    handle_open(state, uuid, false)
  end

  @impl true
  def handle_call({:open, uuid, true}, _from, state) do
    handle_open(state, uuid, true)
  end

  @impl true
  def handle_call({:close, uuid}, from, state) do
    drop_source_shadow_route(uuid, state)
    CatalogIndex.mark_closing(uuid)

    case Map.get(state.close_operations, uuid) do
      waiters when is_list(waiters) ->
        {:noreply,
         %{state | close_operations: Map.put(state.close_operations, uuid, [from | waiters])}}

      nil ->
        catalog = self()

        case start_close_operation(uuid, catalog) do
          {:ok, _pid} ->
            {:noreply, %{state | close_operations: Map.put(state.close_operations, uuid, [from])}}

          {:error, reason} ->
            restore_open_index(uuid, state)

            {:reply,
             {:error,
              Error.internal_error("database close could not be scheduled", %{
                cause: inspect(reason)
              })}, state}
        end
    end
  end

  @impl true
  def handle_call({:ensure_command_target, uuid}, _from, state) do
    safe(fn -> reply_ensure_command_target(uuid, state, false) end, state)
  end

  @impl true
  def handle_call({:ensure_command_target, uuid, true}, _from, state) do
    safe(fn -> reply_ensure_command_target(uuid, state, true) end, state)
  end

  defp reply_ensure_command_target(uuid, state, allow_shadow) do
    if Map.has_key?(state.close_operations, uuid) do
      {:reply, {:error, Error.database_closed("database is closing")}, state}
    else
      case open_runtime(state, uuid, allow_shadow) do
        {:ok, _info, new_state} -> {:reply, :ok, new_state}
        {:error, error, new_state} -> {:reply, {:error, error}, new_state}
      end
    end
  end

  defp unregister_entry(next, state) do
    case RegistrationManifest.write(Map.values(next.entries)) do
      :ok -> {:reply, :ok, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  defp inspect_unregistered_entry(state, uuid, entry) do
    case inspect_entry(entry) do
      {:ok, _} = ok ->
        {:reply, ok, mark_status(state, uuid, :registered)}

      {:error, %Error{} = error} = err ->
        {:reply, err, maybe_mark_unavailable(state, uuid, error)}
    end
  end

  defp command_deadline_error do
    Error.new(
      :internal_error,
      "database command timed out",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end

  defp route_command(uuid, class, command, deadline) do
    case {command_io_class(command), ReadPool.enabled?(uuid)} do
      {:read, true} -> ReadPool.execute(uuid, class, command, deadline)
      {:exclusive, true} -> exclusive_command(uuid, class, command, deadline)
      _ -> DatabaseAdmission.execute_with_deadline(uuid, class, command, deadline)
    end
  end

  defp exclusive_command(uuid, class, command, deadline) do
    ReadPool.with_quiesce(uuid, deadline, fn ->
      DatabaseAdmission.execute_with_deadline(uuid, class, command, deadline)
    end)
  end

  defp command_io_class({:command_context, _authority, inner}), do: command_io_class(inner)

  defp command_io_class(command) do
    case VialKeeper.Commands.normalize(command) do
      %module{} -> Map.get(CommandIO.classes(), module, :write)
      _ -> :write
    end
  end

  # SAFETY: final catch-all net for handle_call clauses that touch the adapter. An
  # unanticipated raise/throw is converted to a typed internal_error reply so the shared
  # catalog GenServer never crashes from a poisoned input. `state` is returned unchanged
  # because the operation failed before (or outside of) any state mutation.
  defp safe(fun, state) do
    fun.()
  catch
    kind, reason ->
      Logger.error("catalog operation raised",
        kind: kind,
        reason: inspect(reason)
      )

      {:reply,
       {:error,
        Error.internal_error("database operation failed", %{
          cause: inspect(reason),
          kind: kind
        })}, state}
  end

  @impl true
  def handle_info({:close_finished, uuid, result}, state) do
    case Map.pop(state.close_operations, uuid) do
      {nil, _operations} ->
        {:noreply, state}

      {waiters, operations} ->
        Enum.each(waiters, &GenServer.reply(&1, result))
        finalize_close_index(uuid, result, state)
        {:noreply, %{state | close_operations: operations}}
    end
  end

  @impl true
  def handle_info(:resume_registered_jobs, state) do
    uuids = Map.keys(state.entries)

    _ =
      Task.Supervisor.start_child(VialKeeper.TaskSupervisor, fn ->
        Enum.each(uuids, &resume_registered_jobs/1)
      end)

    {:noreply, state}
  end

  defp handle_bundle_root(state, uuid, allow_shadow) do
    case Map.get(state.entries, uuid) do
      %{database_kind: :shadow} when not allow_shadow ->
        {:reply, {:error, Error.shadow_database_hidden("shadow database is internal")}, state}

      %{bundle_root: bundle_root} when is_binary(bundle_root) ->
        {:reply, {:ok, bundle_root}, state}

      nil ->
        {:reply, {:error, Error.database_not_registered("database is not registered")}, state}

      _entry ->
        {:reply, {:error, Error.database_unavailable("database bundle root is missing")}, state}
    end
  end

  defp handle_open(state, uuid, allow_shadow) do
    if Map.has_key?(state.close_operations, uuid) do
      {:reply, {:error, Error.database_closed("database is closing")}, state}
    else
      case open_runtime(state, uuid, allow_shadow) do
        {:ok, info, new_state} -> {:reply, {:ok, info}, new_state}
        {:error, error, new_state} -> {:reply, {:error, error}, new_state}
      end
    end
  end

  defp open_runtime(state, uuid, allow_shadow) do
    case Map.get(state.entries, uuid) do
      nil ->
        {:error, Error.database_not_registered("database is not registered"), state}

      %{database_kind: :shadow} when not allow_shadow ->
        {:error, Error.shadow_database_hidden("shadow database is internal"), state}

      %{bundle_root: _root} = entry ->
        open_registered_entry(state, uuid, entry)
    end
  end

  defp open_registered_entry(state, uuid, entry) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{_pid, _}] ->
        case ensure_derived_manager(uuid, entry) do
          :ok ->
            index_open(uuid, entry)

            {:ok, %{database_uuid: uuid, runtime_state: :open},
             mark_status(state, uuid, :registered)}

          {:error, reason} ->
            error = open_failure_error(reason, entry)
            {:error, error, maybe_mark_unavailable(state, uuid, error)}
        end

      [] ->
        start_registered_entry(state, uuid, entry)
    end
  end

  defp start_registered_entry(state, uuid, %{bundle_root: bundle_root} = entry) do
    cond do
      not File.dir?(bundle_root) ->
        error = Error.database_unavailable("registered database bundle is missing")
        {:error, error, mark_status(state, uuid, :unavailable)}

      open_count() >= (VialKeeper.Config.host_limits()[:max_open_databases] || 64) ->
        {:error, Error.resource_limit("maximum open database count reached", %{}), state}

      true ->
        start_database_runtime(state, uuid, entry, bundle_root)
    end
  end

  defp start_database_runtime(state, uuid, entry, bundle_root) do
    case DatabaseBundle.prepare_for_open(bundle_root) do
      {:ok, bundle} ->
        start_database_runtime_child(state, uuid, entry, bundle)

      {:error, error} ->
        {:error, error, maybe_mark_unavailable(state, uuid, error)}
    end
  end

  defp start_database_runtime_child(state, uuid, entry, bundle) do
    args = %{
      uuid: uuid,
      bundle: bundle,
      database_kind: Map.get(entry, :database_kind, :ordinary)
    }

    case DynamicSupervisor.start_child(
           VialKeeper.Runtime.DatabaseSupervisor,
           {DatabaseRuntimeSupervisor, args}
         ) do
      {:ok, _pid} -> finish_database_runtime_start(state, uuid, entry)
      {:error, reason} -> open_runtime_child_error(state, uuid, entry, reason)
    end
  end

  defp finish_database_runtime_start(state, uuid, entry) do
    case ensure_derived_manager(uuid, entry) do
      :ok ->
        index_open(uuid, entry)
        {:ok, %{database_uuid: uuid, runtime_state: :open}, mark_status(state, uuid, :registered)}

      {:error, reason} ->
        _ = stop_runtime_after_failed_open(uuid)
        error = open_failure_error(reason, entry)
        {:error, error, maybe_mark_unavailable(state, uuid, error)}
    end
  end

  defp open_runtime_child_error(state, uuid, entry, reason) do
    error = open_failure_error(reason, entry)
    {:error, error, maybe_mark_unavailable(state, uuid, error)}
  end

  # LIFE-007: missing file or UUID mismatch MUST mark registration unavailable.
  defp maybe_mark_unavailable(
         state,
         uuid,
         %Error{code: code} = error
       )
       when code in [:database_unavailable, :integrity_violation] do
    if uuid_mismatch?(error) or missing_file?(error) or kind_mismatch?(error),
      do: mark_status(state, uuid, :unavailable),
      else: state
  end

  defp maybe_mark_unavailable(state, _uuid, _error), do: state

  defp mark_status(state, uuid, status) do
    case Map.get(state.entries, uuid) do
      nil ->
        state

      entry ->
        %{state | entries: Map.put(state.entries, uuid, Map.put(entry, :status, status))}
    end
  end

  defp open_failure_error(reason, _entry) do
    case extract_error(reason) do
      %Error{} = error ->
        error

      _ ->
        Error.database_unavailable("database could not be opened", %{
          cause: inspect(reason)
        })
    end
  end

  defp extract_error(%Error{} = error), do: error
  defp extract_error({:shutdown, reason}), do: extract_error(reason)
  defp extract_error({:failed_to_start_child, _id, reason}), do: extract_error(reason)
  defp extract_error(_), do: nil

  defp uuid_mismatch?(%Error{details: %{reason: :uuid_mismatch}}), do: true
  defp uuid_mismatch?(_), do: false

  defp kind_mismatch?(%Error{details: %{reason: :database_kind_mismatch}}), do: true
  defp kind_mismatch?(_), do: false

  defp missing_file?(%Error{message: message}) when is_binary(message),
    do: String.contains?(message, "missing")

  defp missing_file?(_), do: false

  defp runtime_pid(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:runtime, uuid}) do
      [{pid, _}] -> pid
      [] -> nil
    end
  end

  defp put_entry(state, uuid, relative, %DatabaseBundle{} = bundle, database_kind) do
    entry = %{
      uuid: uuid,
      path: relative,
      bundle_root: DatabaseBundle.root(bundle),
      database_kind: database_kind,
      status: :registered
    }

    next = %{state | entries: Map.put(state.entries, uuid, entry)}

    case RegistrationManifest.write(Map.values(next.entries)) do
      :ok -> {:ok, next}
      {:error, error} -> {:error, error}
    end
  end

  defp no_duplicate_uuid(state, uuid, bundle_root) do
    case Map.get(state.entries, uuid) do
      nil -> :ok
      %{bundle_root: ^bundle_root} -> :ok
      _ -> {:error, Error.duplicate_database_uuid("database UUID is already registered")}
    end
  end

  defp ensure_path_not_open(state, bundle_root) do
    case Enum.find(state.entries, fn {_uuid, entry} -> entry.bundle_root == bundle_root end) do
      {_uuid, %{uuid: uuid}} ->
        case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [] ->
            :ok

          _ ->
            {:error, Error.database_in_use("database must be closed before registration")}
        end

      nil ->
        :ok
    end
  end

  defp safe_path(root, relative) when is_binary(relative) do
    expanded = Path.expand(relative, root)
    root = Path.expand(root)
    relative_to_root = Path.relative_to(expanded, root)
    components = Path.split(relative)

    cond do
      Path.type(relative) == :absolute or relative == "" or
        Enum.any?(components, &(&1 in [".", ".."])) or
        String.contains?(relative, "\\") or relative_to_root == expanded or
          String.starts_with?(relative_to_root, "../") ->
        {:error, Error.invalid_request("database path escapes the database root")}

      not PathSafety.no_symlink_components?(expanded) ->
        {:error, Error.invalid_request("database path contains a symlink")}

      true ->
        {:ok, expanded}
    end
  end

  defp safe_path(_, _),
    do: {:error, Error.invalid_request("database path must be relative")}

  defp index_open(uuid, entry) do
    CatalogIndex.put_open(uuid, Map.get(entry, :database_kind, :ordinary))
  end

  defp restore_open_index(uuid, state) do
    case Map.get(state.entries, uuid) do
      nil ->
        CatalogIndex.delete(uuid)

      entry ->
        case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
          [{_pid, _}] -> index_open(uuid, entry)
          [] -> CatalogIndex.delete(uuid)
        end
    end
  end

  defp finalize_close_index(uuid, :ok, _state), do: CatalogIndex.delete(uuid)
  defp finalize_close_index(uuid, _result, state), do: restore_open_index(uuid, state)

  defp entry_status(entry) do
    state =
      case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, entry.uuid}) do
        [{_pid, _}] ->
          :open

        [] ->
          cond do
            Map.get(entry, :status) == :unavailable -> :unavailable
            File.dir?(entry.bundle_root) -> :registered
            true -> :unavailable
          end
      end

    %{
      database_uuid: entry.uuid,
      path: entry.path,
      database_kind: Map.get(entry, :database_kind, :ordinary),
      state: state
    }
  end

  defp inspect_entry(entry) do
    backend = StorageRegistry.backend()

    if File.dir?(entry.bundle_root) do
      case backend.open(backend.artifact_path(entry.bundle_root)) do
        {:ok, adapter} ->
          inspect_open_adapter(backend, adapter, entry)

        {:error, error} ->
          {:error, error}
      end
    else
      {:error, Error.database_unavailable("registered database bundle is missing")}
    end
  end

  defp inspect_open_adapter(backend, adapter, entry) do
    result = backend.identity(adapter)
    _ = backend.close(adapter)

    case result do
      {:ok, identity} ->
        uuid = identity_uuid(identity)
        database_kind = identity_kind(identity)

        cond do
          uuid == entry.uuid and database_kind == Map.get(entry, :database_kind, :ordinary) ->
            {:ok, identity}

          uuid == entry.uuid ->
            {:error,
             Error.integrity_violation(
               "database kind does not match registration hint",
               Error.identity_mismatch_details(
                 :database_kind_mismatch,
                 Map.get(entry, :database_kind, :ordinary),
                 database_kind
               )
             )}

          true ->
            {:error,
             Error.database_unavailable(
               "database UUID mismatch",
               Error.identity_mismatch_details(:uuid_mismatch, entry.uuid, uuid)
             )}
        end

      other ->
        other
    end
  end

  defp identity_uuid(identity) when is_map(identity), do: MapAccess.get(identity, :database_uuid)

  defp identity_kind(identity) when is_map(identity) do
    MapAccess.get(identity, :database_kind, :ordinary)
  end

  defp shadow_entry?(%{database_kind: :shadow}), do: true
  defp shadow_entry?(_entry), do: false

  defp resume_registered_jobs(uuid) do
    case open(uuid) do
      {:ok, _} ->
        :ok = JobManager.resume(uuid)

        if not JobManager.active?(uuid), do: _ = close(uuid)

      {:error, _} ->
        :ok
    end
  end

  defp close_runtime(uuid) do
    # Prefer checking external close blockers before begin_close. Active
    # replication remains a hard blocker; callers must disable jobs first.
    with :ok <- ensure_kind_closeable(uuid),
         false <- JobManager.active?(uuid),
         :ok <- drain_read_pool(uuid),
         :ok <- drain_admission(uuid) do
      close_runtime_after_admission_drain(uuid)
    else
      true ->
        _ = ReadPool.cancel_close(uuid)
        {:error, Error.database_not_closable("database has active replication jobs")}

      {:error, _} = error ->
        _ = ReadPool.cancel_close(uuid)
        error
    end
  end

  defp ensure_kind_closeable(uuid) do
    case DatabaseOwner.command_with_context(
           uuid,
           CommandContext.shadow_control(shadow_database_uuid: uuid),
           {:command, :identity, %{}}
         ) do
      {:ok, identity} ->
        if identity_kind(identity) == :shadow,
          do: :ok,
          else: DerivedViewManager.ensure_closable(uuid)

      {:error, _} = error ->
        error
    end
  end

  defp close_runtime_after_admission_drain(uuid) do
    try do
      with :ok <- close_external_services(uuid),
           :ok <- ReadPool.close_readers(uuid),
           :ok <- stop_read_pool(uuid) do
        stop_owner(uuid)
        stop_runtime_supervisor(uuid)
      end
    catch
      :exit, reason ->
        {:error,
         Error.internal_error("database runtime shutdown failed during close", %{
           cause: inspect(reason)
         })}
    end
    |> abort_close_on_error(uuid)
  end

  defp stop_owner(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{pid, _}] ->
        if Process.alive?(pid),
          do: GenServer.stop(pid, :shutdown, VialKeeper.Config.shutdown_timeout()),
          else: :ok

      [] ->
        :ok
    end
  end

  defp stop_runtime_supervisor(uuid) do
    case runtime_pid(uuid) do
      pid when is_pid(pid) ->
        if Process.alive?(pid),
          do: Supervisor.stop(pid, :shutdown, VialKeeper.Config.shutdown_timeout()),
          else: :ok

      nil ->
        :ok
    end
  end

  defp abort_close_on_error({:error, _} = error, uuid) do
    _ = DatabaseAdmission.cancel_close(uuid)
    _ = ReadPool.cancel_close(uuid)
    error
  end

  defp abort_close_on_error(other, _uuid), do: other

  defp close_external_services(uuid) do
    with :ok <- close_derived_manager(uuid),
         :ok <- close_view_manager(uuid),
         :ok <- close_attachment_coordinator(uuid),
         :ok <- close_subscription_hub(uuid) do
      close_change_notifier(uuid)
    end
  end

  defp ensure_derived_manager(uuid, %{database_kind: :derived}) do
    case DatabaseOwner.command(uuid, {:command, :get_derived_view, %{}}) do
      {:ok, %{enabled: true, definition: %{sources: sources}}} ->
        DerivedViewManager.start(uuid, sources)

      {:ok, %{enabled: false}} ->
        DerivedViewManager.close(uuid)

      {:ok, _metadata} ->
        {:error, Error.integrity_violation("derived enabled state is invalid")}

      {:error, _} = error ->
        error
    end
  end

  defp ensure_derived_manager(_uuid, _entry), do: :ok

  defp stop_runtime_after_failed_open(uuid) do
    case runtime_pid(uuid) do
      pid when is_pid(pid) -> Supervisor.stop(pid, :shutdown, VialKeeper.Config.shutdown_timeout())
      _ -> :ok
    end
  end

  defp close_derived_manager(uuid) do
    case DerivedViewManager.close(uuid) do
      :ok -> :ok
      {:error, %Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp close_view_manager(uuid) do
    Manager.close(uuid)
  end

  defp close_attachment_coordinator(uuid) do
    case AttachmentCoordinator.begin_close(uuid) do
      :ok -> :ok
      {:error, %Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp close_subscription_hub(uuid) do
    case SubscriptionHub.close(uuid) do
      :ok -> :ok
      {:error, %Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  end

  defp close_change_notifier(uuid), do: ChangeNotifier.close(uuid)

  defp drain_read_pool(uuid) do
    case ReadPool.begin_close(uuid) do
      :ok -> :ok
      {:error, %Error{code: :database_closed}} -> :ok
      {:error, _} = error -> error
    end
  catch
    :exit, reason ->
      _ = ReadPool.cancel_close(uuid)

      {:error,
       Error.internal_error("database read pool drain failed during close", %{
         cause: inspect(reason)
       })}
  end

  defp stop_read_pool(uuid) do
    case runtime_pid(uuid) do
      pid when is_pid(pid) ->
        case Supervisor.terminate_child(pid, {:read_pool_supervisor, uuid}) do
          :ok ->
            delete_read_pool_child(pid, uuid)

          {:error, :not_found} ->
            stop_read_pool_process(uuid)
        end

      nil ->
        stop_read_pool_process(uuid)
    end
  end

  defp delete_read_pool_child(runtime, uuid) do
    case Supervisor.delete_child(runtime, {:read_pool_supervisor, uuid}) do
      :ok ->
        :ok

      {:error, :not_found} ->
        :ok

      {:error, reason} ->
        {:error,
         Error.internal_error("database read pool shutdown failed during close", %{
           cause: inspect(reason)
         })}
    end
  end

  defp stop_read_pool_process(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:read_pool_supervisor, uuid}) do
      [{pid, _}] ->
        if Process.alive?(pid),
          do: Supervisor.stop(pid, :shutdown, VialKeeper.Config.shutdown_timeout()),
          else: :ok

      [] ->
        :ok
    end
  end

  defp drain_admission(uuid) do
    with :ok <- DatabaseAdmission.begin_close(uuid) do
      await_admission_idle(uuid)
    end
  end

  defp await_admission_idle(uuid) do
    case DatabaseAdmission.await_idle(uuid, VialKeeper.Config.shutdown_timeout()) do
      :ok ->
        :ok

      other ->
        _ = DatabaseAdmission.cancel_close(uuid)
        other
    end
  catch
    :exit, reason ->
      _ = DatabaseAdmission.cancel_close(uuid)

      {:error,
       Error.internal_error("database admission drain failed during close", %{
         cause: inspect(reason)
       })}
  end

  defp drop_source_shadow_route(uuid, state) do
    case Map.get(state.entries, uuid) do
      %{database_kind: :shadow} ->
        :ok

      _entry ->
        case RouteTable.get(uuid) do
          {:ok, snapshot} ->
            _ = RouteTable.compare_delete(uuid, snapshot)
            _ = Reconciler.notify_source_closed(uuid)

          :not_found ->
            :ok
        end
    end
  end

  defp disable_source_shadow(uuid) do
    case ShadowRegistry.disable(uuid) do
      {:ok, definition} ->
        _ = Reconciler.enqueue(definition)
        :ok

      _ ->
        :ok
    end
  end

  defp start_close_operation(uuid, catalog) do
    Task.Supervisor.start_child(VialKeeper.TaskSupervisor, fn ->
      send(catalog, {:close_finished, uuid, run_close_operation(uuid)})
    end)
  end

  defp run_close_operation(uuid) do
    case Registry.lookup(VialKeeper.Runtime.DatabaseRegistry, {:owner, uuid}) do
      [{_pid, _}] -> close_runtime(uuid)
      [] -> :ok
    end
  catch
    kind, reason ->
      _ = rollback_aborted_close(uuid)

      {:error,
       Error.internal_error("database close failed", %{
         kind: kind,
         cause: inspect(reason)
       })}
  end

  defp rollback_aborted_close(uuid) do
    _ = ReadPool.cancel_close(uuid)

    case DatabaseAdmission.closing?(uuid) do
      {:ok, true} -> DatabaseAdmission.cancel_close(uuid)
      _ -> :ok
    end
  end

  defp open_count do
    Registry.select(VialKeeper.Runtime.DatabaseRegistry, [
      {{{:owner, :"$1"}, :"$2", :"$3"}, [], [true]}
    ])
    |> length()
  end
end
