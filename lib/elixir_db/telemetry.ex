defmodule ElixirDB.Telemetry do
  @moduledoc false

  @events [
    [:elixir_db, :database, :open],
    [:elixir_db, :database, :command],
    [:elixir_db, :database, :overload],
    [:elixir_db, :changes, :read],
    [:elixir_db, :query, :execute],
    [:elixir_db, :index, :build],
    [:elixir_db, :replication, :batch],
    [:elixir_db, :replication, :checkpoint],
    [:elixir_db, :http, :request]
  ]

  def events, do: @events

  def span(event, metadata, fun) when is_list(event) and is_map(metadata) and is_function(fun, 0) do
    start = System.monotonic_time()

    try do
      result = fun.()
      :telemetry.execute(event, %{duration: System.monotonic_time() - start}, metadata)
      result
    rescue
      exception ->
        :telemetry.execute(
          event,
          %{duration: System.monotonic_time() - start},
          Map.put(metadata, :error, ElixirDB.Error.internal_error("operation failed"))
        )

        reraise exception, __STACKTRACE__
    end
  end
end
