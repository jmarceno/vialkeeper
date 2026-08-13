defmodule ElixirDB.Attachments.TicketTest do
  @moduledoc "Adversarial coverage for immutable attachment stream tickets."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.Attachments.Ticket
  alias ElixirDB.Error
  alias ElixirDB.TestSupport.GarbageGenerators

  @digest String.duplicate("a", 64)
  @required_keys [
    :database_uuid,
    :bundle_path,
    :blob_digest,
    :logical_size,
    :content_type,
    :document_id,
    :revision_id,
    :attachment_name
  ]

  describe "build/8" do
    test "builds a validated ticket and expands its bundle path" do
      args = valid_args()
      assert {:ok, %Ticket{} = ticket} = apply(Ticket, :build, args)

      assert ticket.database_uuid == "database-1"
      assert ticket.bundle_path == Path.expand("tmp/../bundle.elixirdb")
      assert ticket.blob_digest == @digest
      assert ticket.logical_size == 10
      assert ticket.content_type == "text/plain"
      assert ticket.document_id == "doc-1"
      assert ticket.revision_id == "1-revision"
      assert ticket.attachment_name == "source.txt"
    end

    test "rejects invalid plain-guard fields" do
      invalid_fields = [
        {0, [nil, 42]},
        {1, [nil, 42]},
        {3, [-1, 1.5, "10"]},
        {5, [nil, 42]},
        {6, [nil, 42]}
      ]

      for {index, invalid_values} <- invalid_fields, value <- invalid_values do
        result = valid_args() |> List.replace_at(index, value) |> then(&apply(Ticket, :build, &1))

        assert {:error,
                %Error{
                  code: :invalid_request,
                  message: "invalid attachment ticket fields"
                }} = result
      end
    end

    test "surfaces manifest validation errors" do
      invalid_fields = [
        {2, String.duplicate("A", 64), "attachment digest must be lowercase SHA-256 hex"},
        {2, "abc", "attachment digest must be lowercase SHA-256 hex"},
        {2, String.duplicate("z", 64), "attachment digest must be lowercase SHA-256 hex"},
        {4, "", "attachment content_type must be non-empty"},
        {4, <<0xFF>>, "attachment content_type must be valid UTF-8"},
        {7, "bad\0name", "attachment name must not contain NUL"},
        {7, <<0xFF, 0xFE>>, "attachment name must be valid UTF-8"}
      ]

      for {index, value, message} <- invalid_fields do
        result = valid_args() |> List.replace_at(index, value) |> then(&apply(Ticket, :build, &1))
        assert {:error, %Error{code: :invalid_request, message: ^message}} = result
      end
    end
  end

  describe "new/1" do
    test "rejects every missing required key" do
      for key <- @required_keys do
        assert_invalid(Ticket.new(Map.delete(valid_attrs(), key)))
      end
    end

    test "rejects string-keyed attributes" do
      attrs = Map.new(valid_attrs(), fn {key, value} -> {Atom.to_string(key), value} end)
      assert_invalid(Ticket.new(attrs))
    end

    test "rejects non-map input" do
      for input <- [nil, [], "x"] do
        assert_invalid(Ticket.new(input))
      end
    end
  end

  @tag :slow
  property "new/1 never raises for adversarial attributes" do
    attrs = valid_attrs()

    input_generator =
      StreamData.one_of([
        GarbageGenerators.junk_map(),
        GarbageGenerators.junk_term(),
        GarbageGenerators.near_valid(attrs, @required_keys)
      ])

    check all(input <- input_generator, max_runs: 400) do
      result = Ticket.new(input)
      assert match?({:ok, %Ticket{}}, result) or match?({:error, %Error{}}, result)
    end
  end

  defp valid_args do
    [
      "database-1",
      "tmp/../bundle.elixirdb",
      @digest,
      10,
      "text/plain",
      "doc-1",
      "1-revision",
      "source.txt"
    ]
  end

  defp valid_attrs, do: Map.new(Enum.zip(@required_keys, valid_args()))

  defp assert_invalid(result) do
    assert {:error,
            %Error{
              code: :invalid_request,
              message: "invalid attachment ticket fields"
            }} = result
  end
end
