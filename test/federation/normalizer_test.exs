defmodule VialKeeper.Federation.NormalizerTest do
  @moduledoc "Covers strict federation request normalization and fingerprints."

  use ExUnit.Case, async: false

  alias VialKeeper.Federation.Normalizer

  @source "123e4567-e89b-12d3-a456-426614174000"
  @other_source "123e4567-e89b-12d3-a456-426614174001"

  test "rejects unknown request and query fields and federation-only search/index" do
    assert invalid(%{databases: [@source], query: %{}, extra: true})
    assert invalid(%{databases: [@source], query: %{selector: %{}, extra: true}})
    assert invalid(%{databases: [@source], query: %{search: %{text: "x"}}})
    assert invalid(%{databases: [@source], query: %{index: "idx"}})
  end

  test "rejects atom and string key collisions recursively" do
    assert invalid(%{"databases" => [@other_source], databases: [@source], query: %{}})

    query = %{
      selector: %{"status" => "stale", status: "ready"},
      sort: [%{"status" => :desc, status: :asc}],
      fields: %{"status" => false, status: true}
    }

    assert invalid(%{databases: [@source], query: query})
  end

  test "accepts atom-only and string-only request maps" do
    assert {:ok, _} = Normalizer.normalize(%{databases: [@source], query: %{selector: %{}}})

    assert {:ok, _} =
             Normalizer.normalize(%{"databases" => [@source], "query" => %{"selector" => %{}}})
  end

  test "canonicalizes UUID case and rejects case-insensitive duplicates" do
    uppercase = String.upcase(@source)

    assert {:ok, normalized} = Normalizer.normalize(%{databases: [uppercase], query: %{}})
    assert normalized.databases == [@source]

    assert invalid(%{databases: [@source, uppercase], query: %{}})
  end

  test "enforces non-empty, unique, valid, and bounded sources" do
    assert invalid(%{databases: [], query: %{}})
    assert invalid(%{databases: [@source, @source], query: %{}})
    assert invalid(%{databases: ["not-a-uuid"], query: %{}})

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             Normalizer.normalize(%{databases: List.duplicate(@source, 17), query: %{}})
  end

  test "reads the validated host federation source limit" do
    previous = Application.get_env(:vial_keeper, :federation)
    on_exit(fn -> Application.put_env(:vial_keeper, :federation, previous) end)
    Application.put_env(:vial_keeper, :federation, max_sources: 1)

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             Normalizer.normalize(%{databases: [@source, @other_source], query: %{}})
  end

  test "applies the default limit and enforces the configured result ceiling" do
    assert {:ok, normalized} = Normalizer.normalize(%{databases: [@source], query: %{}})
    assert normalized.query.limit == 50

    previous = Application.get_env(:vial_keeper, :host_limits)
    on_exit(fn -> Application.put_env(:vial_keeper, :host_limits, previous) end)
    Application.put_env(:vial_keeper, :host_limits, max_query_results: 10)

    assert {:error, %VialKeeper.Error{code: :resource_limit}} =
             Normalizer.normalize(%{databases: [@source], query: %{limit: 11}})
  end

  test "fingerprints are deterministic and bind source order" do
    request = %{databases: [@source, @other_source], query: %{selector: %{}}}
    assert {:ok, first} = Normalizer.normalize(request)
    assert {:ok, second} = Normalizer.normalize(request)
    assert first.fingerprint == second.fingerprint

    assert {:ok, reversed} =
             Normalizer.normalize(%{request | databases: Enum.reverse(request.databases)})

    refute first.fingerprint == reversed.fingerprint
  end

  defp invalid(request) do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} = Normalizer.normalize(request)
  end
end
