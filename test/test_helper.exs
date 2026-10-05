# The multi-container replication drill is opt-in. Default `mix test` and every
# check alias skip `:container_replication`. Run it with
# `mix test.container_replication`.
ExUnit.start(exclude: [:container_replication])
