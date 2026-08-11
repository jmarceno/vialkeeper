defmodule ElixirDB.Federation.SavedQueries do
  @moduledoc "Read-only access to normalized host-configured federation queries."
  def list do
    Application.get_env(:elixir_db, :federation_saved_queries, [])
  end

  def get(name) when is_binary(name), do: Enum.find(list(), &(&1.name == name))
end
