defmodule ElixirDB.Config do
  @moduledoc "Host and database configuration with host-enforced safety limits."
  alias ElixirDB.JSON.Stringify

  @defaults %{
    "version" => 1,
    "documents" => %{"max_document_bytes" => 1_048_576, "max_document_id_bytes" => 512},
    "queries" => %{
      "default_limit" => 50,
      "max_limit" => 500,
      "scan_threshold" => 1_000,
      "max_execution_ms" => 5_000
    },
    "changes" => %{"default_batch" => 100, "max_batch" => 500, "max_wait_ms" => 30_000},
    "replication" => %{
      "batch_documents" => 100,
      "batch_bytes" => 4_194_304,
      "retry" => %{
        "max_attempts" => 8,
        "base_delay_ms" => 100,
        "max_delay_ms" => 30_000,
        "jitter_ms" => 250
      }
    }
  }

  @spec defaults() :: map()
  def defaults, do: @defaults

  @spec host_limits() :: map()
  def host_limits do
    Application.get_env(:elixir_db, :host_limits, []) |> Map.new()
  end

  @spec shutdown_timeout() :: pos_integer()
  def shutdown_timeout,
    do: Application.get_env(:elixir_db, :shutdown_timeout, 30_000)

  @spec database_root() :: binary()
  def database_root,
    do: Application.get_env(:elixir_db, :database_root, Path.expand("data", File.cwd!()))

  @spec database_defaults() :: map()
  def database_defaults, do: @defaults

  @spec merge_and_bound(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def merge_and_bound(config) when is_map(config) do
    config = Stringify.keys(config)

    with :ok <- validate_shape(config),
         merged <- deep_merge(@defaults, config),
         :ok <- validate_values(merged) do
      bound(merged)
    end
  end

  def merge_and_bound(_),
    do: {:error, ElixirDB.Error.invalid_request("configuration must be an object")}

  @spec validate(map()) :: {:ok, map()} | {:error, ElixirDB.Error.t()}
  def validate(config) when is_map(config), do: merge_and_bound(config)
  def validate(_), do: {:error, ElixirDB.Error.invalid_request("configuration must be an object")}

  defp bound(merged) do
    limits = host_limits()

    with :ok <-
           ensure_integer_limit(
             merged,
             ["documents", "max_document_bytes"],
             limits[:max_document_bytes]
           ),
         :ok <-
           ensure_integer_limit(
             merged,
             ["documents", "max_document_id_bytes"],
             limits[:max_document_id_bytes]
           ),
         :ok <- ensure_integer_limit(merged, ["queries", "max_limit"], limits[:max_query_results]),
         :ok <- ensure_integer_limit(merged, ["changes", "max_batch"], limits[:max_changes_batch]),
         :ok <-
           ensure_integer_limit(
             merged,
             ["queries", "scan_threshold"],
             limits[:max_full_scan_documents]
           ),
         :ok <-
           ensure_integer_limit(
             merged,
             ["replication", "batch_documents"],
             limits[:max_replication_batch_documents]
           ),
         :ok <-
           ensure_integer_limit(
             merged,
             ["replication", "batch_bytes"],
             limits[:max_replication_batch_bytes]
           ),
         :ok <-
           ensure_integer_limit(
             merged,
             ["queries", "max_execution_ms"],
             limits[:max_query_execution_ms]
           ),
         :ok <-
           ensure_integer_limit(merged, ["changes", "max_wait_ms"], limits[:max_wait_ms]),
         :ok <-
           ensure_integer_limit(
             merged,
             ["replication", "retry", "max_attempts"],
             limits[:max_replication_attempts]
           ),
         :ok <-
           ensure_integer_limit(
             merged,
             ["replication", "retry", "max_delay_ms"],
             limits[:max_replication_delay_ms]
           ),
         :ok <- validate_retry_order(merged) do
      {:ok, merged}
    end
  end

  defp validate_retry_order(config) do
    base = get_in(config, ["replication", "retry", "base_delay_ms"])
    maximum = get_in(config, ["replication", "retry", "max_delay_ms"])

    if maximum >= base,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.invalid_request(
           "replication retry max_delay_ms must be at least base_delay_ms"
         )}
  end

  defp deep_merge(left, right) do
    Map.merge(left, right, fn _key, a, b ->
      if is_map(a) and is_map(b), do: deep_merge(a, b), else: b
    end)
  end

  defp validate_shape(config) do
    known = ["version", "documents", "queries", "changes", "replication"]

    if Enum.all?(Map.keys(config), &(&1 in known)) do
      validate_nested_shape(config)
    else
      {:error, ElixirDB.Error.invalid_request("configuration contains an unknown field")}
    end
  end

  defp validate_nested_shape(config) do
    allowed = %{
      "documents" => ["max_document_bytes", "max_document_id_bytes"],
      "queries" => ["default_limit", "max_limit", "scan_threshold", "max_execution_ms"],
      "changes" => ["default_batch", "max_batch", "max_wait_ms"],
      "replication" => ["batch_documents", "batch_bytes", "retry"],
      "retry" => ["max_attempts", "base_delay_ms", "max_delay_ms", "jitter_ms"]
    }

    Enum.reduce_while(config, :ok, fn {section, value}, :ok ->
      validate_section_shape(section, value, allowed)
    end)
  end

  defp validate_section_shape("version", 1, _allowed), do: {:cont, :ok}

  defp validate_section_shape("version", _value, _allowed),
    do: {:halt, {:error, ElixirDB.Error.invalid_request("unsupported configuration version")}}

  defp validate_section_shape(section, value, allowed) do
    accepted = Map.get(allowed, section, [])

    if is_map(value) and Enum.all?(Map.keys(value), &(&1 in accepted)) do
      validate_retry_shape(section, value, allowed)
    else
      {:halt, {:error, ElixirDB.Error.invalid_request("configuration sections must be objects")}}
    end
  end

  defp validate_retry_shape("replication", %{"retry" => retry}, allowed) when is_map(retry) do
    if Enum.all?(Map.keys(retry), &(&1 in allowed["retry"])),
      do: {:cont, :ok},
      else: unknown_configuration_field()
  end

  defp validate_retry_shape(_section, _value, _allowed), do: {:cont, :ok}

  defp unknown_configuration_field do
    {:halt, {:error, ElixirDB.Error.invalid_request("configuration contains an unknown field")}}
  end

  defp validate_values(config) do
    values = [
      ["documents", "max_document_bytes"],
      ["documents", "max_document_id_bytes"],
      ["queries", "default_limit"],
      ["queries", "max_limit"],
      ["queries", "scan_threshold"],
      ["queries", "max_execution_ms"],
      ["changes", "default_batch"],
      ["changes", "max_batch"],
      ["changes", "max_wait_ms"],
      ["replication", "batch_documents"],
      ["replication", "batch_bytes"],
      ["replication", "retry", "max_attempts"],
      ["replication", "retry", "base_delay_ms"],
      ["replication", "retry", "max_delay_ms"],
      ["replication", "retry", "jitter_ms"]
    ]

    Enum.reduce_while(values, :ok, fn path, :ok ->
      case get_in(config, path) do
        value when is_integer(value) and value > 0 ->
          {:cont, :ok}

        _ ->
          {:halt,
           {:error,
            ElixirDB.Error.invalid_request("configuration limits must be positive integers", %{
              path: path
            })}}
      end
    end)
  end

  defp ensure_integer_limit(value, path, maximum) when is_integer(maximum) do
    current = get_in(value, path)

    if is_integer(current) and current > 0 and current <= maximum,
      do: :ok,
      else:
        {:error,
         ElixirDB.Error.resource_limit("configuration exceeds host limit", %{
           path: path,
           maximum: maximum
         })}
  end

  defp ensure_integer_limit(_value, _path, _maximum), do: :ok
end
