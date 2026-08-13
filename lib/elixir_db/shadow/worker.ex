defmodule ElixirDB.Shadow.Worker do
  @moduledoc "Generation-fenced worker service for managed shadow bundles."
  import Kernel, except: [inspect: 1, inspect: 2]
  use GenServer

  alias ElixirDB.Attachments.StoreRef
  alias ElixirDB.Error
  alias ElixirDB.JSON.{Canonical, StrictDecoder}
  alias ElixirDB.NodeIdentity
  alias ElixirDB.PathSafety
  alias ElixirDB.Runtime.{AtomicWrite, CommandContext, DatabaseCatalog}
  alias ElixirDB.Shadow.Protocol

  @journal_filename "managed_shadows.json"
  @journal_version 1
  @provision_fields ~w(source_uuid shadow_uuid generation operation_id attachment_store_type attachment_location specification_digest source_base_url source_bearer_token)a
  @generation_fields ~w(source_uuid shadow_uuid generation operation_id)a

  @type options :: keyword()

  @spec start_link(options()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, type: :worker}

  @spec capabilities(options()) :: {:ok, map()} | {:error, Error.t()}
  def capabilities(opts \\ []), do: call_server(opts, :capabilities)

  @spec provision(map(), options()) :: {:ok, map()} | {:error, Error.t()}
  def provision(request, opts \\ []), do: call_server(opts, {:provision, request})

  @spec inspect(map(), options()) :: {:ok, map()} | {:error, Error.t()}
  def inspect(request, opts \\ []), do: call_server(opts, {:inspect, request})

  @spec destroy(map(), options()) :: {:ok, map()} | {:error, Error.t()}
  def destroy(request, opts \\ []), do: call_server(opts, {:destroy, request})

  @spec read_document(map(), keyword(), options()) :: {:ok, term()} | {:error, Error.t()}
  def read_document(request, read_opts \\ [], opts \\ []),
    do: call_server(opts, {:read_document, request, read_opts})

  @spec bulk_read_documents(map(), keyword(), options()) :: {:ok, term()} | {:error, Error.t()}
  def bulk_read_documents(request, read_opts \\ [], opts \\ []),
    do: call_server(opts, {:bulk_read_documents, request, read_opts})

  @spec open_attachment_representation(map(), keyword(), options()) ::
          {:ok, term()} | {:error, Error.t()}
  def open_attachment_representation(request, read_opts \\ [], opts \\ []),
    do: call_server(opts, {:open_attachment_representation, request, read_opts})

  @doc "Marks an exact generation ready after a later replication proof."
  @spec mark_ready(map(), non_neg_integer(), options()) :: :ok | {:error, Error.t()}
  def mark_ready(request, watermark \\ 0, opts \\ []),
    do: call_server(opts, {:mark_ready, request, watermark})

  @impl true
  def init(opts) do
    root = root(opts)
    :ok = File.mkdir_p(root)
    {:ok, %{root: root, options: opts, journal_path: journal_path(root)}}
  end

  @impl true
  def handle_call(:capabilities, _from, state) do
    {:reply, capability_response(state), state}
  end

  def handle_call({:provision, request}, _from, state),
    do: reply_mutation(state, provision_state(request, state), state)

  def handle_call({:inspect, request}, _from, state),
    do: {:reply, inspect_state(request, state), state}

  def handle_call({:destroy, request}, _from, state) do
    case destroy_state(request, state) do
      {:ok, result, next} -> {:reply, {:ok, result}, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:mark_ready, request, watermark}, _from, state) do
    case mark_ready_state(request, watermark, state) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, error} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:read_document, request, read_opts}, _from, state),
    do: {:reply, read_document_state(request, read_opts, state), state}

  def handle_call({:bulk_read_documents, request, read_opts}, _from, state),
    do: {:reply, bulk_read_state(request, read_opts, state), state}

  def handle_call({:open_attachment_representation, request, _read_opts}, _from, state),
    do: {:reply, attachment_unavailable(request, state), state}

  defp call_server(opts, message) do
    case Keyword.get(opts, :server) do
      nil ->
        case Process.whereis(__MODULE__) do
          nil -> direct_call(message, opts)
          pid -> GenServer.call(pid, message, call_timeout(opts))
        end

      server ->
        GenServer.call(server, message, call_timeout(opts))
    end
  end

  defp direct_call(:capabilities, opts), do: capability_response(%{root: root(opts)})

  defp direct_call({:provision, request}, opts),
    do:
      provision_state(request, %{
        root: root(opts),
        options: opts,
        journal_path: journal_path(root(opts))
      })
      |> result_only()

  defp direct_call({:inspect, request}, opts),
    do:
      inspect_state(request, %{
        root: root(opts),
        options: opts,
        journal_path: journal_path(root(opts))
      })

  defp direct_call({:destroy, request}, opts) do
    state = %{root: root(opts), options: opts, journal_path: journal_path(root(opts))}

    case destroy_state(request, state) do
      {:ok, result, _next} -> {:ok, result}
      {:error, error} -> {:error, error}
    end
  end

  defp direct_call({:mark_ready, request, watermark}, opts) do
    state = %{root: root(opts), options: opts, journal_path: journal_path(root(opts))}

    case mark_ready_state(request, watermark, state) do
      {:ok, _next} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp direct_call({:read_document, request, read_opts}, opts),
    do:
      read_document_state(request, read_opts, %{
        root: root(opts),
        options: opts,
        journal_path: journal_path(root(opts))
      })

  defp direct_call({:bulk_read_documents, request, read_opts}, opts),
    do:
      bulk_read_state(request, read_opts, %{
        root: root(opts),
        options: opts,
        journal_path: journal_path(root(opts))
      })

  defp direct_call({:open_attachment_representation, request, _read_opts}, opts),
    do:
      attachment_unavailable(request, %{
        root: root(opts),
        options: opts,
        journal_path: journal_path(root(opts))
      })

  defp reply_mutation(_state, {:ok, result, next}, _old), do: {:reply, {:ok, result}, next}
  defp reply_mutation(_state, {:error, error}, old), do: {:reply, {:error, error}, old}

  defp result_only({:ok, result, _state}), do: {:ok, result}
  defp result_only({:error, error}), do: {:error, error}

  defp capability_response(state) do
    case NodeIdentity.ensure(state.root) do
      {:ok, node_id} -> {:ok, Protocol.response(node_id)}
      {:error, error} -> {:error, error}
    end
  end

  defp provision_state(request, state) do
    with {:ok, request} <- normalize_provision(request),
         :ok <- validate_attachment_root(request, state),
         {:ok, journal} <- read_journal(state.journal_path),
         {:ok, result, next_journal} <- provision_existing_or_new(request, journal, state),
         :ok <- write_journal(state.journal_path, next_journal) do
      {:ok, result, %{state | journal_path: state.journal_path}}
    end
  end

  defp provision_existing_or_new(request, journal, state) do
    entries = journal["shadows"] || []

    same_generation =
      Enum.find(
        entries,
        &same_source_generation?(&1, request["source_uuid"], request["generation"])
      )

    cond do
      same_generation && same_identity?(same_generation, request) ->
        with :ok <- validate_managed_entry(same_generation, request, state),
             {:ok, _identity} <- DatabaseCatalog.open_internal(request["shadow_uuid"]) do
          {:ok, public_entry(same_generation, true), journal}
        end

      same_generation ->
        {:error,
         Error.shadow_generation_conflict("shadow generation is already bound to another operation")}

      Enum.any?(entries, &(&1["shadow_uuid"] == request["shadow_uuid"])) ->
        {:error, Error.shadow_identity_conflict("shadow UUID is already bound to another source")}

      Enum.any?(
        entries,
        &(&1["source_uuid"] == request["source_uuid"] && &1["generation"] > request["generation"])
      ) ->
        {:error,
         Error.shadow_generation_stale("shadow generation is older than the managed generation")}

      true ->
        with :ok <- destroy_older_generations(entries, request, state),
             {:ok, entry} <- create_entry(request, state) do
          {:ok, public_entry(entry, false),
           %{journal | "shadows" => [entry | reject_source(entries, request["source_uuid"])]}}
        end
    end
  end

  defp create_entry(request, state) do
    with {:ok, bundle_path} <- managed_path(state.root, request),
         :ok <- ensure_storage_root(state.root),
         {:ok, relative_path} <- relative_to_database_root(bundle_path),
         {:ok, node_id} <- NodeIdentity.ensure(state.root),
         {:ok, identity} <-
           DatabaseCatalog.create_shadow_internal(
             relative_path,
             %{
               database_uuid: request["shadow_uuid"],
               database_kind: :shadow,
               shadow_metadata: shadow_metadata(request),
               config: ElixirDB.Config.defaults()
             }
           ),
         true <- identity_uuid(identity) == request["shadow_uuid"] do
      {:ok,
       request
       |> Map.take(@provision_fields |> Enum.map(&Atom.to_string/1))
       |> Map.merge(%{
         "bundle_path" => bundle_path,
         "state" => "bootstrapping",
         "created_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
         "worker_node_id" => node_id,
         "source_base_url" => request["source_base_url"]
       })}
    else
      false ->
        {:error, Error.shadow_identity_conflict("created shadow identity does not match request")}

      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.database_unavailable("shadow bundle could not be created", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp inspect_state(request, state) do
    with {:ok, request} <- normalize_inspect(request),
         {:ok, journal} <- read_journal(state.journal_path),
         {:ok, entry} <- find_lookup_entry(journal, request),
         :ok <- validate_managed_entry(entry, entry, state),
         {:ok, _identity} <- DatabaseCatalog.open_internal(entry["shadow_uuid"]) do
      {:ok, public_entry(entry, false)}
    else
      {:error, %Error{code: :database_not_registered}} -> {:ok, %{"state" => "absent"}}
      {:error, %Error{} = error} -> {:error, error}
      :not_found -> {:ok, %{"state" => "absent"}}
    end
  end

  defp destroy_state(request, state) do
    with {:ok, request} <- normalize_destroy(request),
         {:ok, journal} <- read_journal(state.journal_path) do
      case find_lookup_entry(journal, request) do
        :not_found -> {:ok, %{"state" => "absent"}, state}
        {:ok, entry} -> destroy_entry(entry, journal, state)
      end
    end
  end

  defp destroy_entry(entry, journal, state) do
    with :ok <- validate_managed_entry(entry, entry, state),
         :ok <- close_and_unregister(entry["shadow_uuid"]),
         :ok <- remove_managed_bundle(entry, state),
         next_journal = %{journal | "shadows" => Enum.reject(journal["shadows"], &(&1 == entry))},
         :ok <- write_journal(state.journal_path, next_journal) do
      {:ok, %{"state" => "absent", "shadow_uuid" => entry["shadow_uuid"]}, state}
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp destroy_older_generations(entries, request, state) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      destroy_older_generation(entry, request, entries, state)
    end)
  end

  defp destroy_older_generation(entry, request, entries, state) do
    if entry["source_uuid"] == request["source_uuid"] and
         entry["generation"] < request["generation"] do
      case destroy_entry(entry, %{"version" => @journal_version, "shadows" => entries}, state) do
        {:ok, _result, _state} -> {:cont, :ok}
        {:error, error} -> {:halt, {:error, error}}
      end
    else
      {:cont, :ok}
    end
  end

  defp mark_ready_state(request, watermark, state)
       when is_integer(watermark) and watermark >= 0 do
    with {:ok, request} <- normalize_generation(request),
         {:ok, journal} <- read_journal(state.journal_path),
         {:ok, entry} <- find_entry(journal, request),
         next_entry =
           Map.merge(entry, %{"state" => "ready", "applied_source_sequence" => watermark}),
         next_journal = %{
           journal
           | "shadows" => [next_entry | Enum.reject(journal["shadows"], &(&1 == entry))]
         },
         :ok <- write_journal(state.journal_path, next_journal) do
      {:ok, state}
    end
  end

  defp mark_ready_state(_request, _watermark, _state),
    do: {:error, Error.invalid_request("shadow watermark must be non-negative")}

  defp read_document_state(request, _read_opts, state) do
    with {:ok, request} <- normalize_read(request),
         {:ok, journal} <- read_journal(state.journal_path),
         {:ok, entry} <- find_entry(journal, request),
         :ok <- ready_entry(entry),
         {:ok, document_id} <- document_id(request) do
      context = shadow_read_context(request)

      DatabaseCatalog.command_with_context(
        request["shadow_uuid"],
        context,
        {:command, :get_document,
         %{
           document_id: document_id,
           revision: request["revision"],
           include_conflicts: request["include_conflicts"] || false
         }}
      )
    end
  end

  defp bulk_read_state(request, _read_opts, state) do
    with {:ok, request} <- normalize_read(request),
         {:ok, journal} <- read_journal(state.journal_path),
         {:ok, entry} <- find_entry(journal, request),
         :ok <- ready_entry(entry),
         requests when is_list(requests) <- request["requests"] do
      context = shadow_read_context(request)

      {:ok,
       Enum.map(requests, fn item ->
         normalized = Protocol.string_keys(item)
         id = normalized["id"] || normalized["document_id"]

         DatabaseCatalog.command_with_context(
           request["shadow_uuid"],
           context,
           {:command, :get_document,
            %{
              document_id: id,
              revision: normalized["revision"],
              include_conflicts: normalized["include_conflicts"] || false
            }}
         )
       end)}
    else
      nil -> {:error, Error.invalid_request("shadow bulk read requests are required")}
      _ -> {:error, Error.invalid_request("shadow bulk read requests must be an array")}
    end
  end

  defp attachment_unavailable(_request, _state),
    do: {:error, Error.shadow_attachment_unavailable("shadow attachment reads are not ready")}

  defp normalize_provision(value) when is_map(value) do
    value = Protocol.string_keys(value)

    with :ok <- reject_unknown(value, @provision_fields),
         {:ok, request} <-
           Protocol.generation_request(value, Enum.map(@provision_fields, &Atom.to_string/1)),
         :ok <- required_store_type(request["attachment_store_type"]),
         :ok <- required_text(request["attachment_location"], "attachment_location"),
         :ok <- digest(request["specification_digest"]) do
      {:ok, request}
    end
  end

  defp normalize_provision(_),
    do: {:error, Error.invalid_request("shadow provision request must be an object")}

  defp normalize_generation(value) when is_map(value) do
    value = Protocol.string_keys(value)
    Protocol.generation_request(value, Enum.map(@generation_fields, &Atom.to_string/1))
  end

  defp normalize_generation(_),
    do: {:error, Error.invalid_request("shadow generation request must be an object")}

  defp normalize_read(value) when is_map(value) do
    value = Protocol.string_keys(value)

    Protocol.generation_request(
      value,
      ~w(source_uuid shadow_uuid generation operation_id id document_id revision include_conflicts requests)
    )
  end

  defp normalize_read(_),
    do: {:error, Error.invalid_request("shadow read request must be an object")}

  defp normalize_lookup(value) when is_map(value) do
    value = Protocol.string_keys(value)

    with :ok <- reject_unknown(value, ~w(source_uuid generation)a),
         :ok <- uuid(value["source_uuid"], "source_uuid"),
         {:ok, generation} <- positive_generation(value["generation"]) do
      {:ok, %{"source_uuid" => String.downcase(value["source_uuid"]), "generation" => generation}}
    end
  end

  defp normalize_destroy(value) when is_map(value) do
    value = Protocol.string_keys(value)

    if Map.has_key?(value, "shadow_uuid") or Map.has_key?(value, "operation_id"),
      do: normalize_generation(value),
      else: normalize_lookup(value)
  end

  defp normalize_destroy(_),
    do: {:error, Error.invalid_request("shadow generation request must be an object")}

  defp normalize_inspect(value) when is_map(value) do
    value = Protocol.string_keys(value)

    if Map.has_key?(value, "shadow_uuid") or Map.has_key?(value, "operation_id") do
      with :ok <- reject_unknown(value, @provision_fields),
           do:
             normalize_generation(Map.take(value, Enum.map(@generation_fields, &Atom.to_string/1)))
    else
      normalize_lookup(value)
    end
  end

  defp normalize_inspect(_),
    do: {:error, Error.invalid_request("shadow generation request must be an object")}

  defp reject_unknown(value, allowed) do
    allowed = Enum.map(allowed, &Atom.to_string/1)

    if Enum.any?(Map.keys(value), &(&1 not in allowed)),
      do: {:error, Error.invalid_request("shadow provision request contains an unknown field")},
      else: :ok
  end

  defp uuid(value, field) when is_binary(value) do
    if Regex.match?(
         ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
         value
       ),
       do: :ok,
       else: {:error, Error.invalid_request("shadow #{field} must be a UUID")}
  end

  defp uuid(_, field), do: {:error, Error.invalid_request("shadow #{field} must be a UUID")}

  defp positive_generation(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp positive_generation(_),
    do: {:error, Error.invalid_request("shadow generation must be positive")}

  defp required_store_type("external_cas"), do: :ok

  defp required_store_type(_),
    do: {:error, Error.invalid_request("shadow attachment store type is invalid")}

  defp required_text(value, field) when is_binary(value) do
    if String.trim(value) != "",
      do: :ok,
      else: {:error, Error.invalid_request("shadow #{field} is required")}
  end

  defp required_text(_, field), do: {:error, Error.invalid_request("shadow #{field} is required")}

  defp digest(value) when is_binary(value) do
    if byte_size(value) == 64 and Regex.match?(~r/^[0-9a-f]+$/i, value),
      do: :ok,
      else: {:error, Error.invalid_request("shadow specification digest is invalid")}
  end

  defp digest(_), do: {:error, Error.invalid_request("shadow specification digest is invalid")}

  defp validate_attachment_root(request, state) do
    ref = StoreRef.external_read_only(request["attachment_location"], attachment_roots(state))

    with {:ok, ref} <- StoreRef.normalize(ref),
         true <- File.dir?(ref.blobs_path) do
      :ok
    else
      false ->
        {:error, Error.shadow_attachment_unavailable("external attachment root is unavailable")}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp validate_managed_entry(entry, request, state) do
    with true <- same_identity?(entry, request),
         {:ok, path} <- managed_path(state.root, request),
         true <- entry["bundle_path"] == path,
         true <- File.dir?(path) and PathSafety.no_symlink_components?(path) do
      :ok
    else
      false ->
        {:error, Error.shadow_identity_conflict("managed shadow identity or path is invalid")}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp ready_entry(%{"state" => "ready"}), do: :ok
  defp ready_entry(_), do: {:error, Error.shadow_not_ready("shadow generation is not ready")}

  defp find_entry(journal, request) do
    case Enum.find(journal["shadows"] || [], &same_identity?(&1, request)) do
      nil -> :not_found
      entry -> {:ok, entry}
    end
  end

  defp find_lookup_entry(journal, request) do
    case Enum.find(
           journal["shadows"] || [],
           &same_source_generation?(&1, request["source_uuid"], request["generation"])
         ) do
      nil -> :not_found
      entry -> {:ok, entry}
    end
  end

  defp same_source_generation?(entry, source_uuid, generation),
    do: entry["source_uuid"] == source_uuid and entry["generation"] == generation

  defp same_identity?(entry, request) do
    Enum.all?(
      ["source_uuid", "shadow_uuid", "generation", "operation_id"],
      &(entry[&1] == request[&1])
    )
  end

  defp public_entry(entry, idempotent) do
    entry
    |> Map.take([
      "source_uuid",
      "shadow_uuid",
      "generation",
      "operation_id",
      "state",
      "created_at",
      "applied_source_sequence",
      "worker_node_id"
    ])
    |> Map.put("idempotent", idempotent)
  end

  defp managed_path(root, request) do
    source = request["source_uuid"]
    filename = "#{request["generation"]}-#{request["shadow_uuid"]}.shadow.elixirdb"
    source_root = Path.join(root, source)
    path = Path.join(source_root, filename)

    if PathSafety.within_root?(path, root) and Path.basename(path) == filename and
         PathSafety.no_symlink_components?(root) and PathSafety.no_symlink_components?(source_root),
       do: {:ok, Path.expand(path)},
       else: {:error, Error.integrity_violation("managed shadow path is unsafe")}
  end

  defp relative_to_database_root(path) do
    root = ElixirDB.Config.database_root()

    if PathSafety.within_root?(path, root),
      do: {:ok, Path.relative_to(path, root)},
      else:
        {:error, Error.invalid_request("shadow storage root must remain beneath the database root")}
  end

  defp ensure_storage_root(root) do
    case File.mkdir_p(root) do
      :ok ->
        if PathSafety.no_symlink_components?(root),
          do: :ok,
          else: {:error, Error.integrity_violation("shadow storage root contains a symlink")}

      {:error, reason} ->
        {:error,
         Error.database_unavailable("shadow storage root cannot be created", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp remove_managed_bundle(entry, state) do
    path = entry["bundle_path"]

    case managed_path(state.root, entry) do
      {:ok, ^path} ->
        remove_existing_bundle(path)

      _ ->
        {:error, Error.integrity_violation("shadow destroy path does not match managed identity")}
    end
  end

  defp remove_existing_bundle(path) do
    if File.dir?(path), do: remove_bundle(path), else: missing_bundle_error()
  end

  defp remove_bundle(path) do
    case File.rm_rf(path) do
      {:ok, _} ->
        maybe_remove_empty_source_root(Path.dirname(path))

      {:error, reason, _} ->
        {:error,
         Error.database_unavailable("shadow bundle cannot be removed", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp missing_bundle_error,
    do: {:error, Error.integrity_violation("managed shadow bundle is missing")}

  defp maybe_remove_empty_source_root(path) do
    case File.ls(path) do
      {:ok, []} ->
        case File.rmdir(path) do
          :ok ->
            :ok

          {:error, :enoent} ->
            :ok

          {:error, reason} ->
            {:error,
             Error.database_unavailable("shadow source directory cannot be removed", %{
               cause: Kernel.inspect(reason)
             })}
        end

      {:ok, _entries} ->
        :ok

      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        {:error,
         Error.database_unavailable("shadow source directory cannot be inspected", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp close_and_unregister(uuid) do
    with :ok <- accept_absent(DatabaseCatalog.close(uuid)),
         :ok <- accept_absent(DatabaseCatalog.unregister(uuid)) do
      :ok
    else
      {:error, %Error{} = error} ->
        {:error, error}

      other ->
        {:error,
         Error.database_unavailable("shadow database cannot be unregistered", %{
           cause: Kernel.inspect(other)
         })}
    end
  end

  defp accept_absent(:ok), do: :ok
  defp accept_absent({:error, %Error{code: :database_not_registered}}), do: :ok
  defp accept_absent({:error, %Error{} = error}), do: {:error, error}
  defp accept_absent(other), do: {:error, other}

  defp shadow_metadata(request) do
    %{
      source_database_uuid: request["source_uuid"],
      shadow_database_uuid: request["shadow_uuid"],
      generation: request["generation"],
      operation_id: request["operation_id"],
      attachment_store_type: "external_cas",
      attachment_location: request["attachment_location"],
      specification_digest: request["specification_digest"],
      created_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }
  end

  defp document_id(request) do
    id = request["id"] || request["document_id"]

    if is_binary(id) and id != "",
      do: {:ok, id},
      else: {:error, Error.invalid_request("shadow document id is required")}
  end

  defp shadow_read_context(request) do
    CommandContext.shadow_read(
      source_database_uuid: request["source_uuid"],
      shadow_database_uuid: request["shadow_uuid"],
      generation: request["generation"],
      operation_id: request["operation_id"]
    )
  end

  defp root(opts) do
    configured = Keyword.get(opts, :root)

    case configured do
      value when is_binary(value) ->
        Path.expand(value)

      _ ->
        worker = Application.get_env(:elixir_db, :shadow_worker, [])
        Path.expand(Keyword.get(worker, :storage_root, "shadows"), ElixirDB.Config.database_root())
    end
  end

  defp attachment_roots(state) do
    Keyword.get(state.options, :allowed_attachment_roots) ||
      Application.get_env(:elixir_db, :shadow_worker, [])[:allowed_attachment_roots] ||
      []
  end

  defp call_timeout(opts),
    do: Keyword.get(opts, :timeout, ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000)

  defp journal_path(root), do: Path.join(root, @journal_filename)

  defp read_journal(path) do
    case File.read(path) do
      {:error, :enoent} ->
        {:ok, %{"version" => @journal_version, "shadows" => []}}

      {:error, reason} ->
        {:error,
         Error.database_unavailable("managed shadow journal cannot be read", %{
           cause: Kernel.inspect(reason)
         })}

      {:ok, body} ->
        with {:ok, %{"version" => @journal_version, "shadows" => shadows}} <-
               StrictDecoder.decode(body),
             true <- is_list(shadows) do
          {:ok, %{"version" => @journal_version, "shadows" => shadows}}
        else
          _ -> {:error, Error.integrity_violation("managed shadow journal is invalid")}
        end
    end
  end

  defp write_journal(path, journal) do
    with {:ok, encoded} <- Canonical.encode(journal),
         :ok <- AtomicWrite.write(path, encoded),
         :ok <- File.chmod(path, 0o600) do
      :ok
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         Error.database_unavailable("managed shadow journal cannot be written", %{
           cause: Kernel.inspect(reason)
         })}
    end
  end

  defp reject_source(entries, source), do: Enum.reject(entries, &(&1["source_uuid"] == source))

  defp identity_uuid(identity) when is_map(identity),
    do: Map.get(identity, :database_uuid, Map.get(identity, "database_uuid"))

  defp identity_uuid(_), do: nil
end
