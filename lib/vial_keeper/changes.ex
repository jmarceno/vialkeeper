defmodule VialKeeper.Changes do
  @moduledoc "Bounded and race-free changes feed operations."
  alias VialKeeper.Changes.Request
  alias VialKeeper.MapAccess
  alias VialKeeper.Observability.Instrumentation.Changes, as: ChangesModule
  alias VialKeeper.Runtime.{ChangeNotifier, DatabaseCatalog}

  @type admission_class :: :foreground | :replication

  def read(uuid, request \\ %{}, opts \\ []) do
    admission_class = Keyword.get(opts, :admission_class, :foreground)

    ChangesModule.read(uuid, 0, fn ->
      with {:ok, normalized} <- normalize_request(request) do
        catalog_read(uuid, normalized, admission_class)
      end
    end)
  end

  def wait(uuid, request, opts \\ []) do
    with {:ok, normalized} <- normalize_request(request) do
      wait_request(uuid, normalized, Keyword.get(opts, :admission_class, :foreground))
    end
  end

  defp catalog_read(uuid, normalized, :foreground) do
    DatabaseCatalog.command(uuid, {:command, :read_changes, normalized})
  end

  defp catalog_read(uuid, normalized, admission_class) do
    DatabaseCatalog.command_as(uuid, admission_class, {:command, :read_changes, normalized})
  end

  defp wait_request(uuid, request, admission_class) do
    wait_ms = request.wait_ms
    since = request.since

    case read(uuid, request, admission_class: admission_class) do
      {:ok, %{results: [_ | _]} = result} ->
        {:ok, result}

      {:ok, result} ->
        wait_result(uuid, request, since, wait_ms, result, admission_class)

      error ->
        error
    end
  end

  defp wait_result(_uuid, _request, _since, 0, result, _admission_class), do: {:ok, result}

  defp wait_result(uuid, request, since, wait_ms, _result, admission_class),
    do: wait_for_change(uuid, request, since, wait_ms, admission_class)

  defp normalize_request(%Request{} = request), do: validate_values(request)

  defp normalize_request(request) when is_map(request) do
    allowed = [:since, :limit, :wait_ms, "since", "limit", "wait_ms"]

    case Enum.all?(Map.keys(request), &(&1 in allowed)) do
      true ->
        normalize_values(request)

      false ->
        {:error, VialKeeper.Error.invalid_request("changes request contains an unknown field")}
    end
  end

  defp normalize_request(_),
    do: {:error, VialKeeper.Error.invalid_request("changes request must be an object")}

  defp has_newer?({:ok, %{results: [_ | _]}}, _), do: true
  defp has_newer?(_, _), do: false

  defp normalize_values(request) do
    values = %Request{
      since: MapAccess.get(request, :since, 0),
      limit: MapAccess.get(request, :limit, 100),
      wait_ms: MapAccess.get(request, :wait_ms, 0)
    }

    validate_values(values)
  end

  defp validate_values(%{since: since, limit: limit, wait_ms: wait_ms} = values) do
    max_batch = VialKeeper.Config.host_limits()[:max_changes_batch] || 500
    max_wait = VialKeeper.Config.host_limits()[:max_wait_ms] || 30_000

    validators = [
      fn -> validate_since(since) end,
      fn -> validate_limit_value(limit) end,
      fn -> validate_limit_max(limit, max_batch) end,
      fn -> validate_wait_ms(wait_ms) end,
      fn -> validate_wait_max(wait_ms, max_wait) end
    ]

    case Enum.find_value(validators, & &1.()) do
      nil -> {:ok, values}
      error -> {:error, error}
    end
  end

  defp validate_since(value) when is_integer(value) and value >= 0, do: nil

  defp validate_since(_),
    do: VialKeeper.Error.invalid_request("since must be a non-negative integer")

  defp validate_limit_value(value) when is_integer(value) and value > 0, do: nil

  defp validate_limit_value(_),
    do: VialKeeper.Error.invalid_request("limit must be a positive integer")

  defp validate_limit_max(value, max) when value <= max, do: nil

  defp validate_limit_max(_value, _max),
    do: VialKeeper.Error.resource_limit("changes limit exceeds the host limit")

  defp validate_wait_ms(value) when is_integer(value) and value >= 0, do: nil

  defp validate_wait_ms(_),
    do: VialKeeper.Error.invalid_request("wait_ms must be a non-negative integer")

  defp validate_wait_max(value, max) when value <= max, do: nil

  defp validate_wait_max(_value, _max),
    do: VialKeeper.Error.resource_limit("wait_ms exceeds the host limit")

  defp wait_for_change(uuid, request, since, wait_ms, admission_class) do
    with {:ok, ref, current} <- ChangeNotifier.subscribe(uuid, since) do
      read_after_subscription(uuid, request, ref, current, wait_ms, admission_class)
    end
  end

  defp read_after_subscription(uuid, request, ref, current, wait_ms, admission_class) do
    result = read(uuid, request, admission_class: admission_class)

    if has_newer?(result, current) do
      unsubscribe(uuid, ref)
      result
    else
      receive_change(uuid, request, ref, wait_ms, admission_class)
    end
  end

  defp receive_change(uuid, request, ref, wait_ms, admission_class) do
    receive do
      {:database_changed, ^uuid, _sequence} ->
        unsubscribe(uuid, ref)
        read(uuid, request, admission_class: admission_class)

      {:database_maintenance, ^uuid, _event} ->
        unsubscribe(uuid, ref)
        read(uuid, request, admission_class: admission_class)

      {:database_closed, ^uuid} ->
        unsubscribe(uuid, ref)

        {:error, VialKeeper.Error.database_closed("database closed while waiting for changes")}
    after
      min(wait_ms, VialKeeper.Config.host_limits()[:max_wait_ms] || 30_000) ->
        unsubscribe(uuid, ref)
        read(uuid, request, admission_class: admission_class)
    end
  end

  defp unsubscribe(uuid, ref) do
    ChangeNotifier.unsubscribe(uuid, ref)
    Process.demonitor(ref, [:flush])
  end
end
