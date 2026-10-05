defmodule VialKeeper.Manual.ContainerReplicationGateTest do
  @moduledoc """
  Locks the container replication drill out of the default suite and the
  check aliases.
  """

  use ExUnit.Case, async: true

  test "the container replication drill is outside every default suite and check gate" do
    exclude = ExUnit.configuration() |> Keyword.get(:exclude, [])
    assert :container_replication in List.wrap(exclude)

    helper = File.read!("test/test_helper.exs")
    assert helper =~ "exclude: [:container_replication]"

    drill = File.read!("test/manual/container_replication_test.exs")
    assert drill =~ "@moduletag :container_replication"
    refute drill =~ ":integration"
    refute drill =~ ":slow"
    assert drill =~ "VIAL_KEEPER_CONTAINER_REPLICATION"

    aliases = Mix.Project.config()[:aliases]
    fast = aliases |> Keyword.fetch!(:"check.fast") |> Enum.join("\n")
    full = aliases |> Keyword.fetch!(:"check.full") |> Enum.join("\n")
    integration = aliases |> Keyword.fetch!(:"check.integration") |> Enum.join("\n")

    assert fast =~ "--exclude container_replication"
    assert full =~ "--exclude container_replication"
    refute fast =~ "test.container_replication"
    refute full =~ "test.container_replication"
    refute integration =~ "container_replication"
    assert integration =~ "--only integration"

    preferred = VialKeeper.MixProject.cli()[:preferred_envs]
    assert preferred[:"test.container_replication"] == :test
  end
end
