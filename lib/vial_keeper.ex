defmodule VialKeeper do
  @moduledoc """
  A portable, revisioned document database with pluggable storage backends.
  """

  @protocol_major 1
  @revision_algorithm_version 1
  @canonicalization_version 1

  @spec protocol_major() :: pos_integer()
  def protocol_major, do: @protocol_major

  @spec revision_algorithm_version() :: pos_integer()
  def revision_algorithm_version, do: @revision_algorithm_version

  @spec canonicalization_version() :: pos_integer()
  def canonicalization_version, do: @canonicalization_version
end
