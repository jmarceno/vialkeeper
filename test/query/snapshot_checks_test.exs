defmodule VialKeeper.Query.SnapshotChecksTest do
  @moduledoc """
  Covers bookmark staleness in `SnapshotChecks`: a bookmark is current in its
  own run while the data version holds, and across runs while nothing changed
  above its visible sequence and the retention floor has not passed it.
  """
  use ExUnit.Case, async: true

  alias VialKeeper.Error
  alias VialKeeper.Query.{BookmarkCodec, SnapshotChecks}

  @fingerprint "fingerprint"

  defp request(sequence, visible) do
    {:ok, bookmark} =
      BookmarkCodec.encode(%{
        "query_fingerprint" => @fingerprint,
        "plan_digest" => String.duplicate("a", 64),
        "index_bindings" => [],
        "sequence" => sequence,
        "visible" => visible,
        "sort_direction" => "asc",
        "ordering_key" => "b",
        "last_id" => "b"
      })

    %{fingerprint: @fingerprint, bookmark: bookmark, limit: 2}
  end

  defp identity(overrides) do
    Map.merge(%{data_version: 20, data_version_base: 15, retention_floor_sequence: 0}, overrides)
  end

  defp unchanged(_visible), do: {:ok, false}
  defp changed(_visible), do: {:ok, true}
  defp must_not_read(_visible), do: flunk("the change log must not be read")

  test "a bookmark from the current run is current only at the same data version" do
    assert {:ok, %{after_id: "b"}} =
             SnapshotChecks.admit(request(20, 7), identity(%{}), &must_not_read/1)

    assert {:error, %Error{code: :bookmark_stale}} =
             SnapshotChecks.admit(request(17, 7), identity(%{}), &must_not_read/1)
  end

  test "a bookmark from an earlier run is current while nothing changed above it" do
    test_pid = self()

    changed_after? = fn visible ->
      send(test_pid, {:read_above, visible})
      {:ok, false}
    end

    assert {:ok, %{after_id: "b"}} =
             SnapshotChecks.admit(request(9, 7), identity(%{}), changed_after?)

    assert_received {:read_above, 7}
  end

  test "a bookmark from an earlier run is stale once a change exists above it" do
    assert {:error, %Error{code: :bookmark_stale}} =
             SnapshotChecks.admit(request(9, 7), identity(%{}), &changed/1)
  end

  test "a bookmark from an earlier run is stale once the floor passes its visible sequence" do
    assert {:error, %Error{code: :bookmark_stale}} =
             SnapshotChecks.admit(
               request(9, 7),
               identity(%{retention_floor_sequence: 8}),
               &must_not_read/1
             )

    assert {:ok, _} =
             SnapshotChecks.admit(
               request(9, 7),
               identity(%{retention_floor_sequence: 7}),
               &unchanged/1
             )
  end

  test "a bookmark without a visible sequence does not outlive its run" do
    assert {:error, %Error{code: :bookmark_stale}} =
             SnapshotChecks.admit(request(9, nil), identity(%{}), &must_not_read/1)
  end

  test "a change log error is returned" do
    failure = {:error, Error.internal_error("read failed")}

    assert ^failure =
             SnapshotChecks.admit(request(9, 7), identity(%{}), fn _visible -> failure end)
  end
end
