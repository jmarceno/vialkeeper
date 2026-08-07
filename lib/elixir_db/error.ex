defmodule ElixirDB.Error do
  @moduledoc "Stable domain and protocol errors."

  @enforce_keys [:code, :message, :retryable, :http_status, :details]
  defstruct [:code, :message, :retryable, :http_status, :details, :cause]

  @type t :: %__MODULE__{
          code: atom(),
          message: String.t(),
          retryable: boolean(),
          http_status: pos_integer(),
          details: map(),
          cause: term()
        }

  @registry %{
    unauthorized: {401, false},
    invalid_request: {400, false},
    invalid_bookmark: {400, false},
    database_not_registered: {404, false},
    database_not_found: {404, false},
    document_not_found: {404, false},
    revision_not_found: {404, false},
    index_not_found: {404, false},
    replication_job_not_found: {404, false},
    database_in_use: {409, true},
    database_not_closable: {409, true},
    duplicate_database_uuid: {409, false},
    revision_conflict: {409, false},
    bookmark_stale: {409, true},
    replication_incompatible: {409, false},
    replication_already_running: {409, true},
    checkpoint_conflict: {409, true},
    unsupported_format: {409, false},
    index_name_conflict: {409, false},
    invalid_index_hint: {422, false},
    index_required: {422, false},
    integrity_violation: {422, false},
    payload_too_large: {413, false},
    resource_limit: {422, false},
    database_overloaded: {429, true},
    database_closed: {503, true},
    database_unavailable: {503, true},
    # API-016: internal_error retryability is "Depends on details". The `:depends` sentinel
    # records that the registry does not own a single answer; callers must opt into
    # retryability for genuinely transient internal failures. Absent an explicit opt-in,
    # the safer default for an unknown internal failure is non-retryable.
    internal_error: {500, :depends}
  }

  @spec new(atom(), String.t(), map(), keyword()) :: t()
  def new(code, message, details \\ %{}, opts \\ [])
      when is_atom(code) and is_binary(message) and is_map(details) do
    {status, registry_retryable} = Map.get(@registry, code, {500, false})

    # API-016: a registry value of `:depends` means retryability is per-site. Without an
    # explicit `:retryable` opt, default to the safer non-retryable outcome.
    default_retryable =
      case registry_retryable do
        :depends -> false
        boolean when is_boolean(boolean) -> boolean
      end

    %__MODULE__{
      code: code,
      message: message,
      retryable: Keyword.get(opts, :retryable, default_retryable),
      http_status: Keyword.get(opts, :http_status, status),
      details: details,
      cause: Keyword.get(opts, :cause)
    }
  end

  @spec registry() :: map()
  def registry, do: @registry

  for {name, _} <- @registry do
    def unquote(name)(message \\ nil, details \\ %{}) do
      new(unquote(name), message || unquote(to_string(name)), details)
    end
  end

  @spec public(t()) :: map()
  def public(%__MODULE__{code: code, message: message, retryable: retryable, details: details}) do
    %{code: Atom.to_string(code), message: message, retryable: retryable, details: details}
  end
end
