defmodule VialKeeper.Bench.TortureTest do
  use ExUnit.Case, async: true

  alias VialKeeper.Bench.Torture

  test "select_images honors --limit and bounded profiles" do
    images = Enum.map(1..5, &%{"image_id" => "img-#{&1}"})
    manifest = %{"images" => images}
    first = hd(images)

    assert {:ok, taken} = Torture.select_images(manifest, limit: 2)
    assert taken == Enum.take(images, 2)

    assert {:ok, [^first]} = Torture.select_images(manifest, profile: :smoke)
    assert {:error, message} = Torture.select_images(manifest, profile: :k1)
    assert message =~ "need 1000"

    assert {:ok, ^images} = Torture.select_images(manifest, profile: :standard)
  end

  test "retry_retryable waits out retryable attachment overload" do
    hits = :atomics.new(1, signed: false)

    assert {:ok, :ready} =
             Torture.retry_retryable(fn ->
               n = :atomics.add_get(hits, 1, 1)

               if n < 3 do
                 {:error, VialKeeper.Error.attachment_overloaded("attachment write limit reached")}
               else
                 {:ok, :ready}
               end
             end)

    assert :atomics.get(hits, 1) == 3
  end

  test "retry_retryable does not retry a non-retryable error" do
    hits = :atomics.new(1, signed: false)
    error = VialKeeper.Error.invalid_request("bad")

    assert {:error, ^error} =
             Torture.retry_retryable(fn ->
               _ = :atomics.add_get(hits, 1, 1)
               {:error, error}
             end)

    assert :atomics.get(hits, 1) == 1
  end
end
