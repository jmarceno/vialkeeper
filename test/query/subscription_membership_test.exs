defmodule ElixirDB.Query.Subscription.MembershipTest do
  @moduledoc "Covers subscription membership transitions for document changes."

  use ExUnit.Case, async: true

  @moduletag :integration

  alias ElixirDB.Query.Normalizer
  alias ElixirDB.Query.Subscription.Membership

  setup do
    {:ok, request} = Normalizer.normalize(%{"selector" => %{"/type" => "task"}})
    {:ok, request: request}
  end

  test "nonmember to nonmember emits nothing", %{request: request} do
    envelope = %{id: "a", revision: "r1", deleted: false, body: %{"type" => "note"}}

    assert {:ok, nil, membership} =
             Membership.transition(envelope, request, MapSet.new(), 1, 10)

    assert MapSet.size(membership) == 0
  end

  test "nonmember to member emits upsert", %{request: request} do
    envelope = %{id: "a", revision: "r1", deleted: false, body: %{"type" => "task"}}

    assert {:ok, %{type: :upsert, sequence: 1, document: document}, membership} =
             Membership.transition(envelope, request, MapSet.new(), 1, 10)

    assert document.id == "a"
    assert MapSet.member?(membership, "a")
  end

  test "member to member emits upsert", %{request: request} do
    envelope = %{id: "a", revision: "r2", deleted: false, body: %{"type" => "task", "x" => 1}}

    assert {:ok, %{type: :upsert}, membership} =
             Membership.transition(envelope, request, MapSet.new(["a"]), 2, 10)

    assert MapSet.member?(membership, "a")
  end

  test "member to nonmember emits remove", %{request: request} do
    envelope = %{id: "a", revision: "r3", deleted: false, body: %{"type" => "note"}}

    assert {:ok, %{type: :remove, id: "a", revision: "r3"}, membership} =
             Membership.transition(envelope, request, MapSet.new(["a"]), 3, 10)

    refute MapSet.member?(membership, "a")
  end

  test "member to tombstone emits remove", %{request: request} do
    envelope = %{id: "a", revision: "r4", deleted: true, body: nil}

    assert {:ok, %{type: :remove, id: "a"}, membership} =
             Membership.transition(envelope, request, MapSet.new(["a"]), 4, 10)

    refute MapSet.member?(membership, "a")
  end

  test "membership bound rejects oversized add", %{request: request} do
    envelope = %{id: "b", revision: "r1", deleted: false, body: %{"type" => "task"}}

    assert {:error, %ElixirDB.Error{code: :resource_limit}} =
             Membership.transition(envelope, request, MapSet.new(["a"]), 1, 1)
  end
end
