defmodule VialKeeper.Documents.BulkWriteUnknownFieldsTest do
  @moduledoc "Covers unknown-field rejection for bulk-write operations."

  use ExUnit.Case, async: true

  alias VialKeeper.Documents
  alias VialKeeper.Error

  @uuid "00000000-0000-0000-0000-000000000000"
  @unknown_message "request contains an unknown field"

  test "rejects unknown fields on put and delete" do
    assert_unknown_field(%{"type" => "put", "id" => "x", "body" => %{}, "nope" => 1})

    assert_unknown_field(%{
      "type" => "delete",
      "id" => "x",
      "if_revision" => "revision",
      "if_revison" => "misspelled"
    })
  end

  test "rejects unknown fields on both resolve shapes" do
    assert_unknown_field(%{
      "type" => "resolve",
      "id" => "x",
      "expected_live_revisions" => ["revision"],
      "chosen_parent_revision" => "revision",
      "body" => %{},
      "nope" => true
    })

    assert_unknown_field(%{
      "type" => "resolve",
      "id" => "x",
      "expected_live_revisions" => [],
      "delete_all" => true,
      "nope" => true
    })
  end

  test "legal puts are not classified as unknown fields" do
    result = Documents.bulk_write(@uuid, [%{"type" => "put", "id" => "x", "body" => %{}}])
    refute unknown_field_error?(result)

    result =
      Documents.bulk_write(@uuid, [
        %{"type" => "put", "id" => "x", "body" => %{}, "if_revision" => "revision"}
      ])

    refute unknown_field_error?(result)
  end

  defp assert_unknown_field(operation) do
    assert {:error, %Error{code: :invalid_request, message: @unknown_message}} =
             Documents.bulk_write(@uuid, [operation])
  end

  defp unknown_field_error?({:error, %Error{code: :invalid_request, message: @unknown_message}}),
    do: true

  defp unknown_field_error?(_result), do: false
end
