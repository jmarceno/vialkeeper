defmodule ElixirDB.Query.SubscriptionRequestTest do
  @moduledoc "Covers subscription request validation and limits."

  use ExUnit.Case, async: true

  @moduletag :integration

  alias ElixirDB.Config
  alias ElixirDB.Query.SubscriptionRequest

  @config Config.defaults()

  test "accepts selector-only subscription query" do
    assert {:ok, %{selector: _, predicate: _, fields: nil, heartbeat_ms: 15_000}} =
             SubscriptionRequest.normalize(
               %{"query" => %{"selector" => %{"/type" => "task"}}},
               @config
             )
  end

  test "accepts selector and fields" do
    assert {:ok, %{fields: ["/title"]}} =
             SubscriptionRequest.normalize(
               %{"query" => %{"selector" => %{"/type" => "task"}, "fields" => ["/title"]}},
               @config
             )
  end

  for rejected <- ~w(sort limit bookmark index search) do
    test "rejects #{rejected} in subscription query" do
      request = %{
        "query" => %{
          "selector" => %{"/type" => "task"},
          unquote(rejected) => "value"
        }
      }

      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               SubscriptionRequest.normalize(request, @config)
    end
  end

  test "rejects unknown outer fields" do
    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             SubscriptionRequest.normalize(
               %{"query" => %{"selector" => %{}}, "extra" => true},
               @config
             )
  end

  test "heartbeat defaults and max boundaries" do
    assert {:ok, %{heartbeat_ms: 15_000}} =
             SubscriptionRequest.normalize(%{"query" => %{"selector" => %{}}}, @config)

    assert {:ok, %{heartbeat_ms: 30_000}} =
             SubscriptionRequest.normalize(
               %{"query" => %{"selector" => %{}}, "heartbeat_ms" => 30_000},
               @config
             )

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             SubscriptionRequest.normalize(
               %{"query" => %{"selector" => %{}}, "heartbeat_ms" => 120_000},
               @config
             )
  end

  test "prepare_snapshot rejects forbidden query fields" do
    assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
             SubscriptionRequest.prepare_snapshot(
               %{selector: %{"/type" => "task"}, limit: 10},
               @config
             )

    assert message =~ "limit"
  end

  test "prepare_snapshot validates max_members" do
    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             SubscriptionRequest.prepare_snapshot(
               %{selector: %{"/type" => "task"}, max_members: 10_000},
               @config
             )

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             SubscriptionRequest.prepare_snapshot(
               %{selector: %{"/type" => "task"}, max_members: "bad"},
               @config
             )
  end

  test "prepare_snapshot accepts query-wrapped max_members" do
    assert {:ok, %{max_members: 10}} =
             SubscriptionRequest.prepare_snapshot(
               %{
                 "query" => %{"selector" => %{"/type" => "task"}},
                 "max_members" => 10
               },
               @config
             )
  end

  test "prepare_snapshot rejects invalid field names" do
    request = Map.put(%{selector: %{"/type" => "task"}}, {1, 2, 3}, true)

    assert {:error, %ElixirDB.Error{code: :invalid_request}} =
             SubscriptionRequest.prepare_snapshot(request, @config)
  end
end
