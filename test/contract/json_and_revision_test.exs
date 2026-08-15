defmodule VialKeeper.Contract.JSONAndRevisionTest do
  use ExUnit.Case, async: true

  alias VialKeeper.JSON.{Canonical, StrictDecoder}
  alias VialKeeper.RevisionFixtures
  alias VialKeeper.Revisions.Id

  test "strict decoder rejects duplicate keys and unsafe numbers" do
    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             StrictDecoder.decode(~s({"a":1,"a":2}))

    assert {:error, %VialKeeper.Error{code: :invalid_request}} =
             StrictDecoder.decode("9007199254740992")

    assert {:ok, %{"a" => 1}} = StrictDecoder.decode(~s({"a":1}))
  end

  test "canonical encoding sorts object keys and normalizes negative zero" do
    assert {:ok, "{\"a\":1,\"b\":2}"} = Canonical.encode(%{"b" => 2, "a" => 1})
    assert {:ok, "0"} = Canonical.encode(-0.0)
  end

  test "revision identity is independent of local sequence" do
    history_id = RevisionFixtures.shared_history_id()
    assert {:ok, first} = Id.calculate("doc", history_id, nil, false, %{"x" => 1}, %{})
    assert {:ok, second} = Id.calculate("doc", history_id, nil, false, %{"x" => 1}, %{})
    assert first == second
    assert String.starts_with?(first, "1-")
  end
end
