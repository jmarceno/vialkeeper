defmodule ElixirDB.Changes do
  @moduledoc "Bounded and race-free changes feed operations."
  alias ElixirDB.Runtime.{ChangeNotifier, DatabaseCatalog}

  def read(uuid, request \\ %{}) do
    ElixirDB.Observability.Instrumentation.Changes.read(uuid, 0, fn ->
      with {:ok, normalized} <- normalize_request(request) do
        DatabaseCatalog.command(uuid, {:command, :read_changes, normalized})
      end
    end)
  end

  def wait(uuid, request) do
    with {:ok, normalized} <- normalize_request(request) do
      wait_request(uuid, normalized)
    end
  end

  defp wait_request(uuid, request) do
    wait_ms = request.wait_ms
    since = request.since

    case read(uuid, request) do
      {:ok, %{results: [_ | _]} = result} ->
        {:ok, result}

      {:ok, result} when wait_ms == 0 ->
        {:ok, result}

      {:ok, _result} ->
        with {:ok, ref, current} <- ChangeNotifier.subscribe(uuid, since) do
          result = read(uuid, request)

          if has_newer?(result, current) do
            unsubscribe(uuid, ref)
            result
          else
            receive do
              {:database_changed, ^uuid, _sequence} ->
                unsubscribe(uuid, ref)
                read(uuid, request)

              {:database_closed, ^uuid} ->
                unsubscribe(uuid, ref)

                {:error,
                 ElixirDB.Error.database_closed("database closed while waiting for changes")}
            after
              min(wait_ms, ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000) ->
                unsubscribe(uuid, ref)
                read(uuid, request)
            end
          end
        end

      error ->
        error
    end
  end

  defp normalize_request(request) when is_map(request) do
    allowed = [:since, :limit, :wait_ms, "since", "limit", "wait_ms"]

    if Enum.all?(Map.keys(request), &(&1 in allowed)) do
      since = request[:since] || request["since"] || 0
      limit = request[:limit] || request["limit"] || 100
      wait_ms = request[:wait_ms] || request["wait_ms"] || 0
      max_batch = ElixirDB.Config.host_limits()[:max_changes_batch] || 500
      max_wait = ElixirDB.Config.host_limits()[:max_wait_ms] || 30_000

      cond do
        not is_integer(since) or since < 0 ->
          {:error, ElixirDB.Error.invalid_request("since must be a non-negative integer")}

        not is_integer(limit) or limit <= 0 ->
          {:error, ElixirDB.Error.invalid_request("limit must be a positive integer")}

        limit > max_batch ->
          {:error, ElixirDB.Error.resource_limit("changes limit exceeds the host limit")}

        not is_integer(wait_ms) or wait_ms < 0 ->
          {:error, ElixirDB.Error.invalid_request("wait_ms must be a non-negative integer")}

        wait_ms > max_wait ->
          {:error, ElixirDB.Error.resource_limit("wait_ms exceeds the host limit")}

        true ->
          {:ok, %{since: since, limit: limit, wait_ms: wait_ms}}
      end
    else
      {:error, ElixirDB.Error.invalid_request("changes request contains an unknown field")}
    end
  end

  defp normalize_request(_),
    do: {:error, ElixirDB.Error.invalid_request("changes request must be an object")}

  defp has_newer?({:ok, %{results: [_ | _]}}, _), do: true
  defp has_newer?(_, _), do: false

  defp unsubscribe(uuid, ref) do
    ElixirDB.Runtime.ChangeNotifier.unsubscribe(uuid, ref)
    Process.demonitor(ref, [:flush])
  end
end
