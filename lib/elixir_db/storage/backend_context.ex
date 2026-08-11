defmodule ElixirDB.Storage.BackendContext do
  @moduledoc """
  Opaque storage context handed across the storage boundary.

  Runtime and shared services may hold and pass a context; they must not
  pattern-match on backend-private fields. The selected backend module owns
  interpretation of `:backend_ref`.
  """

  @enforce_keys [:backend, :backend_ref, :bundle_root]
  defstruct [:backend, :backend_ref, :bundle_root, capabilities: %{}, identity: %{}]

  @type t :: %__MODULE__{
          backend: module(),
          backend_ref: term(),
          bundle_root: binary(),
          capabilities: map(),
          identity: map()
        }

  @doc "Builds an opaque backend context."
  @spec new(keyword()) :: t()
  def new(opts) when is_list(opts) do
    %__MODULE__{
      backend: Keyword.fetch!(opts, :backend),
      backend_ref: Keyword.fetch!(opts, :backend_ref),
      bundle_root: Keyword.fetch!(opts, :bundle_root),
      capabilities: Keyword.get(opts, :capabilities, %{}),
      identity: Keyword.get(opts, :identity, %{})
    }
  end

  @doc "Returns the backend module for `context`."
  @spec backend(t()) :: module()
  def backend(%__MODULE__{backend: backend}), do: backend

  @doc "Returns the portable bundle root for `context`."
  @spec bundle_root(t()) :: binary()
  def bundle_root(%__MODULE__{bundle_root: root}), do: root

  @doc "Returns the opaque backend reference; callers must not inspect it."
  @spec backend_ref(t()) :: term()
  def backend_ref(%__MODULE__{backend_ref: ref}), do: ref
end
