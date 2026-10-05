# The multi-container replication drill is opt-in. Default `mix test` and every
# check alias skip `:container_replication`. Run it with
# `mix test.container_replication`.
ExUnit.start(exclude: [:container_replication])

# `VIALKEEPER_TEST_MAX_WRITERS=N` lets the SQLite backend report N writer
# connections (test-only `:sqlite_max_writers`), so the whole suite runs
# through the writer pool, its barrier and out-of-order sequence completion.
case System.get_env("VIALKEEPER_TEST_MAX_WRITERS") do
  nil ->
    :ok

  value ->
    case Integer.parse(value) do
      {writers, ""} when writers > 0 ->
        Application.put_env(:vial_keeper, :sqlite_max_writers, writers)

      _invalid ->
        raise ArgumentError, "VIALKEEPER_TEST_MAX_WRITERS must be a positive integer"
    end
end
