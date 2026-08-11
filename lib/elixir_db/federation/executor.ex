defmodule ElixirDB.Federation.Executor do
  @moduledoc "Executes bounded, stateless federation queries over registered sources."

  alias ElixirDB.Error

  alias ElixirDB.Federation.{
    BookmarkCodec,
    Normalizer,
    Ordering,
    SourceCursor,
    SourceDocument
  }

  alias ElixirDB.MapAccess
  alias ElixirDB.Query
  alias ElixirDB.Query.Projection
  alias ElixirDB.Runtime.Deadline

  @default_max_sources 16
  @default_max_concurrent_sources 8
  @default_max_candidates 10_000
  @default_max_execution_ms 10_000
  @default_max_query_results 500
  @page_size_cap 64

  @type settings :: %{
          max_sources: pos_integer(),
          max_concurrent_sources: pos_integer(),
          max_candidates: non_neg_integer(),
          max_execution_ms: pos_integer(),
          max_query_results: pos_integer(),
          source_fetcher: function()
        }

  @type source_entry :: {non_neg_integer(), binary(), map()}

  @doc "Executes one normalized federation request with a private task supervisor."
  @spec run(map(), keyword()) :: {:ok, map()} | {:error, Error.t()}
  def run(request, opts \\ [])

  def run(request, opts) when is_map(request) and is_list(opts) do
    settings = runtime_settings(opts)
    deadline = Deadline.from_timeout(settings.max_execution_ms)

    with :ok <- check_deadline(deadline),
         {:ok, normalized} <- normalize_request(request, settings),
         {:ok, bookmark} <- decode_bookmark(normalized),
         {:ok, supervisor} <- start_supervisor() do
      try do
        execute(normalized, bookmark, settings, supervisor, deadline)
      after
        stop_supervisor(supervisor)
      end
    end
  end

  def run(_request, _opts),
    do: {:error, Error.invalid_request("federation request must be an object")}

  defp normalize_request(request, settings) do
    Normalizer.normalize(request,
      max_sources: settings.max_sources,
      max_query_results: settings.max_query_results
    )
  end

  defp decode_bookmark(%{databases: databases, query: query, fingerprint: fingerprint}) do
    case MapAccess.get(query, :bookmark) do
      nil ->
        {:ok, nil}

      bookmark when is_binary(bookmark) ->
        expected = %{
          "query_fingerprint" => fingerprint,
          "sort" => wire_sort(MapAccess.get(query, :sort, []))
        }

        with {:ok, decoded} <- BookmarkCodec.decode(bookmark, expected),
             :ok <- validate_bookmark_sources(decoded, databases) do
          {:ok, decoded}
        end

      _ ->
        {:error, Error.invalid_bookmark("bookmark must be an opaque string")}
    end
  end

  defp validate_bookmark_sources(decoded, databases) do
    source_ids = Enum.map(decoded["sources"], & &1["database_uuid"])

    if source_ids == databases do
      :ok
    else
      {:error, Error.bookmark_stale("federation bookmark sources are stale")}
    end
  end

  defp start_supervisor do
    case Task.Supervisor.start_link() do
      {:ok, supervisor} ->
        {:ok, supervisor}

      {:error, reason} ->
        {:error, internal_error("federation task supervisor could not start", reason)}
    end
  end

  defp stop_supervisor(supervisor) do
    _ = Supervisor.stop(supervisor, :normal, :infinity)
    :ok
  end

  defp execute(normalized, bookmark, settings, supervisor, deadline) do
    source_count = length(normalized.databases)

    with {:ok, page_size} <- initial_page_size(source_count, settings.max_candidates),
         {:ok, cursors, candidate_count} <-
           fetch_initial_pages(normalized, page_size, settings, supervisor, deadline),
         :ok <- validate_resume_sequences(cursors, bookmark),
         {:ok, state} <-
           merge_pages(
             normalized,
             bookmark,
             cursors,
             candidate_count,
             page_size,
             settings,
             supervisor,
             deadline
           ),
         {:ok, documents} <- project_documents(state.selected, normalized.query),
         {:ok, next_bookmark} <-
           next_bookmark(state, normalized, bookmark, settings.max_candidates) do
      {:ok,
       %{
         documents: documents,
         bookmark: next_bookmark,
         sources: source_vector(state.cursors)
       }}
    end
  end

  defp initial_page_size(source_count, max_candidates)
       when is_integer(source_count) and source_count > 0 and is_integer(max_candidates) do
    page_size = min(@page_size_cap, div(max_candidates, source_count))

    if page_size > 0 do
      {:ok, page_size}
    else
      {:error, Error.resource_limit("federation candidate budget is too small for its sources")}
    end
  end

  defp fetch_initial_pages(normalized, page_size, settings, supervisor, deadline) do
    entries =
      normalized.databases
      |> Enum.with_index()
      |> Enum.map(fn {source_uuid, index} ->
        {index, source_uuid, source_request(normalized.query, page_size, nil)}
      end)

    with {:ok, pages} <- fetch_in_batches(entries, settings, supervisor, deadline) do
      cursors_from_pages(entries, pages)
    end
  end

  defp fetch_in_batches([], _settings, _supervisor, _deadline), do: {:ok, %{}}

  defp fetch_in_batches(entries, settings, supervisor, deadline) do
    with :ok <- check_deadline(deadline) do
      {batch, rest} = Enum.split(entries, settings.max_concurrent_sources)

      with {:ok, pages} <- fetch_batch(batch, settings, supervisor, deadline),
           {:ok, remaining_pages} <- fetch_in_batches(rest, settings, supervisor, deadline) do
        {:ok, Map.merge(pages, remaining_pages)}
      end
    end
  end

  defp fetch_batch(entries, settings, supervisor, deadline) do
    trace_context = OpenTelemetry.Ctx.get_current()

    case start_tasks(entries, settings, supervisor, deadline, trace_context, []) do
      {:ok, tasks} ->
        await_tasks(tasks, deadline)

      {:error, reason, tasks} ->
        cancel_task_entries(tasks)
        {:error, internal_error("federation source tasks could not start", reason)}
    end
  end

  defp start_tasks([], _settings, _supervisor, _deadline, _trace_context, acc),
    do: {:ok, Enum.reverse(acc)}

  defp start_tasks(
         [{index, source_uuid, request} | rest],
         settings,
         supervisor,
         deadline,
         trace_context,
         acc
       ) do
    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        with_trace_context(trace_context, fn ->
          fetch_source(source_uuid, request, deadline, settings.source_fetcher)
        end)
      end)

    start_tasks(rest, settings, supervisor, deadline, trace_context, [
      %{index: index, task: task} | acc
    ])
  catch
    :exit, reason -> {:error, reason, acc}
  end

  defp cancel_task_entries(tasks) do
    pending = Map.new(tasks, fn %{task: task} -> {task.ref, %{task: task}} end)
    cancel_tasks(pending, :brutal_kill)
  end

  defp await_tasks(tasks, deadline) do
    pending =
      Map.new(tasks, fn %{index: index, task: task} -> {task.ref, %{index: index, task: task}} end)

    await_tasks(pending, %{}, deadline)
  end

  defp await_tasks(pending, results, _deadline) when map_size(pending) == 0,
    do: {:ok, results}

  defp await_tasks(pending, results, deadline) do
    receive do
      {ref, result} when is_reference(ref) ->
        case Map.pop(pending, ref) do
          {nil, _pending} ->
            await_tasks(pending, results, deadline)

          {%{index: index}, remaining} ->
            Process.demonitor(ref, [:flush])

            case result do
              {:ok, page} ->
                await_tasks(remaining, Map.put(results, index, page), deadline)

              {:error, %Error{} = error} ->
                stop_after_error(pending, error)

              {:error, reason} ->
                stop_after_error(pending, internal_error("federation source failed", reason))

              _ ->
                stop_after_error(
                  pending,
                  internal_error("federation source returned an invalid result", result)
                )
            end
        end

      {:DOWN, ref, :process, _pid, reason} ->
        case Map.has_key?(pending, ref) do
          true ->
            stop_after_error(pending, internal_error("federation source task stopped", reason))

          false ->
            await_tasks(pending, results, deadline)
        end
    after
      Deadline.call_timeout(deadline) ->
        cancel_tasks(pending, :brutal_kill)
        {:error, deadline_error()}
    end
  end

  defp stop_after_error(pending, error) do
    cancel_tasks(pending, :brutal_kill)
    {:error, error}
  end

  defp cancel_tasks(tasks, reason) when is_map(tasks) do
    Enum.each(tasks, fn {ref, %{task: task}} ->
      _ = Task.shutdown(task, reason)
      Process.demonitor(ref, [:flush])
    end)

    :ok
  end

  defp fetch_source(source_uuid, request, deadline, fetcher) do
    fetcher
    |> invoke_fetcher(source_uuid, request, deadline)
    |> normalize_source_page()
    |> validate_page_size(MapAccess.get(request, :limit))
  catch
    kind, reason ->
      {:error,
       Error.internal_error("federation source execution failed", %{
         kind: kind,
         reason: inspect(reason)
       })}
  end

  defp invoke_fetcher(fetcher, source_uuid, request, deadline) do
    case :erlang.fun_info(fetcher, :arity) do
      {:arity, 3} -> fetcher.(source_uuid, request, deadline)
      {:arity, 2} -> fetcher.(source_uuid, request)
      {:arity, 1} -> fetcher.(request)
      _ -> {:error, Error.invalid_request("federation source fetcher has an unsupported arity")}
    end
  end

  defp normalize_source_page({:ok, page}) when is_map(page) do
    sequence = MapAccess.get(page, :sequence, :missing)
    documents = MapAccess.get_first(page, [:documents, :results])
    bookmark = MapAccess.get(page, :bookmark)
    has_more = MapAccess.get(page, :has_more, :missing)

    with :ok <- validate_sequence(sequence),
         {:ok, documents} <- normalize_documents(documents),
         {:ok, has_more} <- normalize_has_more(has_more, bookmark),
         :ok <- validate_source_bookmark(bookmark, has_more) do
      {:ok,
       %{
         sequence: sequence,
         documents: documents,
         source_bookmark: if(has_more, do: bookmark, else: nil),
         has_more: has_more
       }}
    end
  end

  defp normalize_source_page({:error, %Error{} = error}), do: {:error, error}

  defp normalize_source_page({:error, reason}),
    do: {:error, internal_error("federation source failed", reason)}

  defp normalize_source_page(_),
    do: {:error, internal_error("federation source returned an invalid page", :invalid_page)}

  defp validate_page_size({:ok, page}, limit)
       when is_integer(limit) and limit > 0 and length(page.documents) <= limit,
       do: {:ok, page}

  defp validate_page_size({:ok, _page}, _limit),
    do: {:error, invalid_page(:page_exceeds_requested_limit)}

  defp validate_page_size({:error, _} = error, _limit), do: error

  defp validate_sequence(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_sequence(_), do: {:error, invalid_page(:invalid_sequence)}

  defp normalize_documents(documents) when is_list(documents) do
    Enum.reduce_while(documents, {:ok, []}, fn document, {:ok, acc} ->
      case normalize_document(document) do
        {:ok, document} -> {:cont, {:ok, [document | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, documents} -> {:ok, Enum.reverse(documents)}
      {:error, _} = error -> error
    end)
  end

  defp normalize_documents(_), do: {:error, invalid_page(:invalid_documents)}

  defp normalize_document(document) when is_map(document) do
    id = MapAccess.get(document, :id)
    revision = MapAccess.get(document, :revision)
    body = MapAccess.get(document, :body)

    if is_binary(id) and id != "" and is_binary(revision) and (is_map(body) or is_nil(body)) do
      {:ok, SourceDocument.new(id, revision, body)}
    else
      {:error, invalid_page(:invalid_document)}
    end
  end

  defp normalize_document(_), do: {:error, invalid_page(:invalid_document)}

  defp normalize_has_more(:missing, bookmark), do: {:ok, is_binary(bookmark)}
  defp normalize_has_more(value, _bookmark) when is_boolean(value), do: {:ok, value}
  defp normalize_has_more(_value, _bookmark), do: {:error, invalid_page(:invalid_has_more)}

  defp validate_source_bookmark(bookmark, true) when is_binary(bookmark) and bookmark != "", do: :ok
  defp validate_source_bookmark(_bookmark, true), do: {:error, invalid_page(:missing_bookmark)}
  defp validate_source_bookmark(_bookmark, false), do: :ok

  defp cursors_from_pages(entries, pages) do
    Enum.reduce_while(entries, {:ok, [], 0}, fn {index, source_uuid, _request},
                                                {:ok, cursors, candidate_count} ->
      page = Map.fetch!(pages, index)

      cursor =
        SourceCursor.new(source_uuid)
        |> SourceCursor.put_page(
          page.sequence,
          page.documents,
          page.source_bookmark,
          page.has_more
        )

      {:cont, {:ok, [cursor | cursors], candidate_count + length(page.documents)}}
    end)
    |> then(fn {:ok, cursors, candidate_count} ->
      {:ok, Enum.reverse(cursors), candidate_count}
    end)
  end

  defp validate_resume_sequences(_cursors, nil), do: :ok

  defp validate_resume_sequences(cursors, bookmark) do
    if source_vector(cursors) == decoded_source_vector(bookmark) do
      :ok
    else
      {:error, Error.bookmark_stale("federation bookmark source sequence is stale")}
    end
  end

  defp decoded_source_vector(bookmark) do
    Enum.map(bookmark["sources"], fn source ->
      %{database_uuid: source["database_uuid"], sequence: source["sequence"]}
    end)
  end

  defp merge_pages(
         normalized,
         bookmark,
         cursors,
         candidate_count,
         page_size,
         settings,
         supervisor,
         deadline
       ) do
    state = %{cursors: cursors, candidate_count: candidate_count, selected: [], last_entry: nil}

    merge_pages(state, normalized, bookmark, page_size, settings, supervisor, deadline)
  end

  defp merge_pages(state, normalized, bookmark, page_size, settings, supervisor, deadline) do
    if state.selected |> length() >= normalized.query.limit do
      {:ok, state}
    else
      merge_more(state, normalized, bookmark, page_size, settings, supervisor, deadline)
    end
  end

  defp merge_more(state, normalized, bookmark, page_size, settings, supervisor, deadline) do
    with :ok <- check_deadline(deadline),
         {:ok, entry, state} <-
           next_entry(state, normalized.query, page_size, settings, supervisor, deadline) do
      merge_or_skip(
        state,
        entry,
        normalized,
        bookmark,
        page_size,
        settings,
        supervisor,
        deadline
      )
    else
      :done -> {:ok, state}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp merge_or_skip(state, entry, normalized, nil, page_size, settings, supervisor, deadline) do
    merge_selected(state, entry, normalized, nil, page_size, settings, supervisor, deadline)
  end

  defp merge_or_skip(state, entry, normalized, bookmark, page_size, settings, supervisor, deadline) do
    if Ordering.compare_cursor(entry, bookmark, normalized.query.sort) == :gt do
      merge_selected(state, entry, normalized, bookmark, page_size, settings, supervisor, deadline)
    else
      merge_pages(state, normalized, bookmark, page_size, settings, supervisor, deadline)
    end
  end

  defp merge_selected(
         state,
         {source_uuid, document} = entry,
         normalized,
         bookmark,
         page_size,
         settings,
         supervisor,
         deadline
       ) do
    next_state = %{state | selected: [entry | state.selected], last_entry: {source_uuid, document}}
    merge_pages(next_state, normalized, bookmark, page_size, settings, supervisor, deadline)
  end

  defp next_entry(state, query, page_size, settings, supervisor, deadline) do
    with {:ok, state} <-
           ensure_heads(state, query, page_size, settings, supervisor, deadline),
         {:ok, index, _document} <- best_head(state.cursors, query.sort) do
      cursor = Enum.at(state.cursors, index)
      {{:ok, document}, cursor} = SourceCursor.pop(cursor)
      cursors = List.replace_at(state.cursors, index, cursor)
      {:ok, {cursor.source_uuid, document}, %{state | cursors: cursors}}
    else
      :empty -> :done
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp ensure_heads(state, query, page_size, settings, supervisor, deadline) do
    case Enum.find_index(state.cursors, fn cursor ->
           SourceCursor.empty?(cursor) and not cursor.exhausted?
         end) do
      nil ->
        {:ok, state}

      index ->
        case fetch_next_page(state, index, query, page_size, settings, supervisor, deadline) do
          {:ok, state} -> ensure_heads(state, query, page_size, settings, supervisor, deadline)
          {:error, %Error{} = error} -> {:error, error}
        end
    end
  end

  defp fetch_next_page(state, index, query, page_size, settings, supervisor, deadline) do
    remaining = settings.max_candidates - state.candidate_count

    if remaining <= 0 do
      {:error, Error.resource_limit("federation candidate budget exhausted")}
    else
      cursor = Enum.at(state.cursors, index)
      limit = min(page_size, remaining)
      request = source_request(query, limit, cursor.source_bookmark)

      with :ok <- check_deadline(deadline),
           {:ok, pages} <-
             fetch_batch(
               [{index, cursor.source_uuid, request}],
               settings,
               supervisor,
               deadline
             ),
           {:ok, page} <- Map.fetch(pages, index),
           :ok <- validate_page_sequence(page.sequence, cursor.sequence) do
        next_cursor =
          SourceCursor.put_page(
            cursor,
            page.sequence,
            page.documents,
            page.source_bookmark,
            page.has_more
          )

        {:ok,
         %{
           state
           | cursors: List.replace_at(state.cursors, index, next_cursor),
             candidate_count: state.candidate_count + length(page.documents)
         }}
      else
        :error -> {:error, internal_error("federation source page was unavailable", :missing_page)}
        {:error, %Error{} = error} -> {:error, error}
      end
    end
  end

  defp validate_page_sequence(sequence, sequence), do: :ok

  defp validate_page_sequence(_sequence, _expected),
    do: {:error, Error.bookmark_stale("federation source sequence changed")}

  defp best_head(cursors, sort) do
    {best, _next_index} =
      Enum.reduce(cursors, {nil, 0}, fn cursor, {best, index} ->
        best =
          case SourceCursor.head(cursor) do
            :empty -> best
            {:ok, document} -> choose_head(best, {index, cursor.source_uuid, document}, sort)
          end

        {best, index + 1}
      end)

    case best do
      nil -> :empty
      {index, _source_uuid, document} -> {:ok, index, document}
    end
  end

  defp choose_head(nil, candidate, _sort), do: candidate

  defp choose_head({best_index, best_source, best_document}, candidate, sort) do
    {candidate_index, candidate_source, candidate_document} = candidate

    if Ordering.compare_documents(
         {candidate_source, candidate_document},
         {best_source, best_document},
         sort
       ) == :lt do
      {candidate_index, candidate_source, candidate_document}
    else
      {best_index, best_source, best_document}
    end
  end

  defp project_documents(entries, query) do
    documents =
      entries
      |> Enum.reverse()
      |> Enum.map(fn {source_uuid, document} ->
        document
        |> Projection.project(query)
        |> Map.put(:source_database_uuid, source_uuid)
      end)

    {:ok, documents}
  end

  defp next_bookmark(%{last_entry: nil}, _normalized, _bookmark, _max_candidates), do: {:ok, nil}

  defp next_bookmark(state, normalized, _bookmark, _max_candidates) do
    if more_available?(state.cursors) do
      {source_uuid, document} = state.last_entry
      query = normalized.query

      payload = %{
        "query_fingerprint" => normalized.fingerprint,
        "sources" => wire_source_vector(state.cursors),
        "sort" => wire_sort(MapAccess.get(query, :sort, [])),
        "ordering_key" => Ordering.ordering_key(document, MapAccess.get(query, :sort, [])),
        "last_source_uuid" => source_uuid,
        "last_document_id" => MapAccess.get(document, :id)
      }

      case BookmarkCodec.encode(payload) do
        {:ok, bookmark} ->
          {:ok, bookmark}

        {:error, reason} ->
          {:error, internal_error("federation bookmark could not be encoded", reason)}
      end
    else
      {:ok, nil}
    end
  end

  defp more_available?(cursors),
    do:
      Enum.any?(cursors, fn cursor -> not SourceCursor.empty?(cursor) or not cursor.exhausted? end)

  defp source_request(query, limit, bookmark) do
    %{
      selector: MapAccess.get(query, :selector, %{}),
      sort: MapAccess.get(query, :sort, []),
      limit: limit,
      bookmark: bookmark
    }
  end

  defp source_vector(cursors),
    do:
      Enum.map(cursors, fn cursor ->
        %{database_uuid: cursor.source_uuid, sequence: cursor.sequence}
      end)

  defp wire_source_vector(cursors) do
    Enum.map(cursors, fn cursor ->
      %{"database_uuid" => cursor.source_uuid, "sequence" => cursor.sequence}
    end)
  end

  defp wire_sort(sort) do
    Enum.map(sort, fn field ->
      %{
        "path" => MapAccess.get(field, :path),
        "direction" => MapAccess.get(field, :direction, "asc")
      }
    end)
  end

  defp with_trace_context(trace_context, fun) do
    token = OpenTelemetry.Ctx.attach(trace_context)

    try do
      fun.()
    after
      OpenTelemetry.Ctx.detach(token)
    end
  end

  defp runtime_settings(opts) do
    configured = Application.get_env(:elixir_db, :federation, []) || []
    host_limits = ElixirDB.Config.host_limits()

    max_sources = positive_setting(opts, configured, :max_sources, @default_max_sources)

    max_concurrent_sources =
      positive_setting(opts, configured, :max_concurrent_sources, @default_max_concurrent_sources)

    %{
      max_sources: max_sources,
      max_concurrent_sources: min(max_concurrent_sources, max_sources),
      max_candidates:
        nonnegative_setting(opts, configured, :max_candidates, @default_max_candidates),
      max_execution_ms:
        positive_setting(opts, configured, :max_execution_ms, @default_max_execution_ms),
      max_query_results:
        positive_setting(opts, host_limits, :max_query_results, @default_max_query_results),
      source_fetcher: Keyword.get(opts, :source_fetcher, &Query.execute_with_deadline/3)
    }
  end

  defp positive_setting(opts, configured, key, default) do
    value = Keyword.get(opts, key, setting_value(configured, key, default))
    if is_integer(value) and value > 0, do: value, else: default
  end

  defp nonnegative_setting(opts, configured, key, default) do
    value = Keyword.get(opts, key, setting_value(configured, key, default))
    if is_integer(value) and value >= 0, do: value, else: default
  end

  defp setting_value(values, key, default) when is_list(values),
    do: Keyword.get(values, key, default)

  defp setting_value(values, key, default) when is_map(values), do: Map.get(values, key, default)
  defp setting_value(_values, _key, default), do: default

  defp check_deadline(deadline) do
    if Deadline.exhausted?(deadline),
      do: {:error, deadline_error()},
      else: :ok
  end

  defp deadline_error do
    Error.new(
      :internal_error,
      "federation execution deadline exhausted",
      %{reason: :deadline_exhausted},
      retryable: true
    )
  end

  defp invalid_page(reason),
    do: Error.internal_error("federation source returned an invalid page", %{reason: reason})

  defp internal_error(message, reason),
    do: Error.internal_error(message, %{reason: inspect(reason)})
end
