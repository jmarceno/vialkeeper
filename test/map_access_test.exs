defmodule ElixirDB.MapAccessTest do
  use ExUnit.Case, async: true

  alias ElixirDB.MapAccess

  test "prefers the atom key when both key forms exist" do
    assert MapAccess.get(%{:name => "atom", "name" => "string"}, :name) == "atom"
  end

  test "falls back to the string key and preserves false and nil values" do
    assert MapAccess.get(%{"enabled" => false}, :enabled, true) == false
    assert MapAccess.get(%{:enabled => nil, "enabled" => true}, :enabled, false) == nil
    assert MapAccess.get(%{}, :missing, :default) == :default
  end

  test "returns the default for non-map input" do
    assert MapAccess.get(:not_a_map, :key, :default) == :default
  end
end
