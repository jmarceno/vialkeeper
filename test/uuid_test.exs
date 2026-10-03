defmodule VialKeeper.UUIDTest do
  @moduledoc "Covers UUID formatting."
  use ExUnit.Case, async: true

  alias VialKeeper.UUID

  test "v4 renders a lowercase RFC 4122 version 4 UUID" do
    for _ <- 1..200 do
      uuid = UUID.v4()

      assert uuid =~
               ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
    end

    assert UUID.v4() != UUID.v4()
  end

  test "document history ids are stable" do
    # Values produced by the earlier :io_lib-based formatter; history IDs feed
    # revision IDs, so they must never change.
    assert UUID.document_history_id("doc") == "87fede2b-e676-4d1c-ba44-d69de445d36a"
    assert UUID.document_history_id("") == "0f181c15-9083-4e85-9038-8b97cf16fd88"
  end
end
