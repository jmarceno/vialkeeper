defmodule VialKeeper.Revisions.IdMatchingTest do
  @moduledoc "Revision and history ID matching against the regexes it replaced."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Revisions.Id

  @revision_regex ~r/^(\d+)-[0-9a-f]{64}$/
  @history_regex ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

  property "generation/1 accepts exactly what the revision regex accepted" do
    check all(candidate <- revision_candidate(), max_runs: 1_000) do
      assert normalize(Id.generation(candidate)) == revision_regex_result(candidate),
             inspect(candidate)
    end
  end

  property "validate_history_id/1 accepts exactly what the UUID regex accepted" do
    check all(candidate <- history_candidate(), max_runs: 1_000) do
      assert normalize(Id.validate_history_id(candidate)) == history_regex_result(candidate),
             inspect(candidate)
    end
  end

  test "revision ID edge cases match the regex" do
    digest = String.duplicate("ab", 32)

    for candidate <- [
          "1-" <> digest,
          "1-" <> digest <> "\n",
          "1-" <> digest <> "\n\n",
          "1-" <> digest <> "\r",
          "007-" <> digest,
          "-" <> digest,
          "1--" <> digest,
          "1-" <> String.upcase(digest),
          "1-" <> binary_part(digest, 0, 63),
          "\n1-" <> digest
        ] do
      assert normalize(Id.generation(candidate)) == revision_regex_result(candidate),
             inspect(candidate)
    end

    assert {:ok, 7} = Id.generation("007-" <> digest)
    assert {:ok, 1} = Id.generation("1-" <> digest <> "\n")
  end

  test "history ID edge cases match the regex" do
    uuid = "0f8fad5b-d9cb-469f-a165-70867728950e"

    for candidate <- [
          uuid,
          String.upcase(uuid),
          uuid <> "\n",
          uuid <> "\n\n",
          String.replace(uuid, "-469f-", "-669f-"),
          String.replace(uuid, "-a165-", "-c165-"),
          String.replace(uuid, "-a165-", "-B165-"),
          String.replace(uuid, "-", "_")
        ] do
      assert normalize(Id.validate_history_id(candidate)) == history_regex_result(candidate),
             inspect(candidate)
    end
  end

  defp revision_regex_result(candidate) do
    case Regex.run(@revision_regex, candidate) do
      [_, generation] -> {:ok, String.to_integer(generation)}
      _ -> :error
    end
  end

  defp history_regex_result(candidate),
    do: if(Regex.match?(@history_regex, candidate), do: :ok, else: :error)

  defp normalize({:ok, generation}), do: {:ok, generation}
  defp normalize(:ok), do: :ok
  defp normalize({:error, _error}), do: :error

  defp revision_candidate do
    digest_chars = Enum.concat([?0..?9, ?a..?f, ?A..?F, [?g, ?-, ?\n]])

    StreamData.one_of([
      StreamData.map(
        StreamData.tuple({
          StreamData.string(Enum.concat(?0..?9, [?a, ?-]), max_length: 4),
          StreamData.string(digest_chars, min_length: 62, max_length: 66)
        }),
        fn {generation, digest} -> generation <> "-" <> digest end
      ),
      StreamData.map(
        StreamData.tuple({
          StreamData.integer(1..999),
          StreamData.string(Enum.concat(?0..?9, ?a..?f), length: 64),
          StreamData.member_of(["", "\n", "\n\n", "x"])
        }),
        fn {generation, digest, suffix} -> "#{generation}-#{digest}#{suffix}" end
      ),
      StreamData.binary(max_length: 70)
    ])
  end

  defp history_candidate do
    chars = Enum.concat([?0..?9, ?a..?f, ?A..?F, [?g, ?-, ?\n]])

    uuid =
      StreamData.map(
        StreamData.tuple({
          StreamData.string(Enum.concat(?0..?9, ?a..?f), length: 32),
          StreamData.member_of(Enum.concat(?0..?9, [?a, ?f])),
          StreamData.member_of([?0, ?7, ?8, ?9, ?a, ?b, ?c, ?A, ?B]),
          StreamData.boolean(),
          StreamData.member_of(["", "\n", "\n\n", " "])
        }),
        fn {hex, version, variant, upcase?, suffix} ->
          <<a::binary-size(8), b::binary-size(4), _v, c::binary-size(3), _w, d::binary-size(3),
            e::binary-size(12)>> = hex

          uuid = "#{a}-#{b}-#{<<version>>}#{c}-#{<<variant>>}#{d}-#{e}"
          if(upcase?, do: String.upcase(uuid), else: uuid) <> suffix
        end
      )

    StreamData.one_of([
      uuid,
      StreamData.string(chars, min_length: 35, max_length: 38),
      StreamData.binary(max_length: 40)
    ])
  end
end
