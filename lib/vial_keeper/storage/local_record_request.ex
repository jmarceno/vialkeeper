defmodule VialKeeper.Storage.LocalRecordRequest do
  @moduledoc "Builds backend-neutral compare-and-swap requests for local records."

  @spec new(binary(), binary(), non_neg_integer(), term()) :: map()
  def new(namespace, key, expected_version, value) do
    %{
      namespace: namespace,
      key: key,
      expected_version: expected_version,
      value: value
    }
  end
end
