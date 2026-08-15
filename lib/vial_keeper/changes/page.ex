defmodule VialKeeper.Changes.Page do
  @moduledoc "Stable storage-neutral shape for one page of change-feed entries."

  @type t :: %{
          required(:results) => [term()],
          required(:last_sequence) => non_neg_integer(),
          required(:has_more) => boolean()
        }

  @spec new([term()], non_neg_integer(), boolean()) :: t()
  def new(results, last_sequence, has_more)
      when is_list(results) and is_integer(last_sequence) and last_sequence >= 0 and
             is_boolean(has_more) do
    %{results: results, last_sequence: last_sequence, has_more: has_more}
  end
end
