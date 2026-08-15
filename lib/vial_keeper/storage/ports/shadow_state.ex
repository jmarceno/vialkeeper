defmodule VialKeeper.Storage.Ports.ShadowState do
  @moduledoc "Backend-neutral storage contract for shadow identity and source origins."

  alias VialKeeper.Storage.BackendContext

  @type result(ok) :: {:ok, ok} | {:error, VialKeeper.Error.t()}

  @callback metadata(BackendContext.t()) :: result(map() | nil)
  @callback put_metadata(BackendContext.t(), map()) :: result(map())
  @callback origin(BackendContext.t(), binary()) :: result(non_neg_integer() | nil)
  @callback put_origin(BackendContext.t(), binary(), non_neg_integer()) :: result(non_neg_integer())
  @callback watermark(BackendContext.t()) :: result(non_neg_integer())
  @callback put_watermark(BackendContext.t(), non_neg_integer()) :: result(non_neg_integer())
end
