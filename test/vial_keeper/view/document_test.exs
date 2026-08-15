defmodule VialKeeper.View.DocumentTest do
  @moduledoc "Behavioral tests for view document mapping and missing path handling."
  use ExUnit.Case, async: true

  alias VialKeeper.View.{Definition, Document}

  test "a missing key path removes the document while explicit null remains a key" do
    assert {:ok, definition} =
             Definition.normalize(%{
               "name" => "by-kind",
               "key" => [%{"path" => "/kind"}],
               "reducer" => "_count"
             })

    assert {:ok, :remove} = Document.map(definition, "missing", "revision", %{})
    assert {:ok, row} = Document.map(definition, "null", "revision", %{"kind" => nil})
    assert row.key == [nil]
  end

  test "an empty JSON pointer is accepted for a value expression" do
    assert {:ok, definition} =
             Definition.normalize(%{
               "name" => "root-value",
               "key" => [%{"literal" => "all"}],
               "value" => %{"path" => ""}
             })

    assert {:ok, row} = Document.map(definition, "document", "revision", %{"value" => 1})
    assert row.value == %{"value" => 1}
  end
end
