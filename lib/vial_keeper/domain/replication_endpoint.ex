defmodule VialKeeper.Domain.ReplicationEndpoint do
  @moduledoc """
  Storage-neutral replication endpoint reference (`REPL-005`).

  A remote endpoint MAY carry an `auth_token` (`AUTH-003`) so a source can
  authenticate to a target that has bearer-token authentication enabled. The
  token is a raw bearer value stored as local, non-replicating job state; it
  never appears in the URL. Local endpoints MUST NOT carry an auth token.
  """

  alias VialKeeper.JSON.Stringify
  @max_token_bytes 4096

  @enforce_keys [:kind, :database_uuid]
  defstruct [:kind, :database_uuid, :base_url, :auth_token]

  @type t :: %__MODULE__{
          kind: atom(),
          database_uuid: binary(),
          base_url: binary() | nil,
          auth_token: binary() | nil
        }

  def new(attrs) when is_map(attrs) do
    attrs = Stringify.keys(attrs)

    case attrs do
      %{"kind" => "local", "database_uuid" => uuid} when is_binary(uuid) ->
        # Local endpoints never need credentials (AUTH-003).
        if Map.keys(attrs) -- ["kind", "database_uuid"] == [] and valid_uuid?(uuid),
          do: {:ok, struct(__MODULE__, %{kind: :local, database_uuid: uuid})},
          else: {:error, VialKeeper.Error.invalid_request("invalid local endpoint")}

      %{"kind" => "remote", "database_uuid" => uuid, "base_url" => url}
      when is_binary(uuid) and is_binary(url) ->
        new_remote(attrs, uuid, url)

      _ ->
        {:error, VialKeeper.Error.invalid_request("invalid replication endpoint")}
    end
  end

  def new(_), do: {:error, VialKeeper.Error.invalid_request("invalid replication endpoint")}

  defp new_remote(attrs, uuid, url) do
    uri = URI.parse(url)

    case validate_remote(attrs, uuid, uri) do
      nil ->
        {:ok,
         struct(__MODULE__, %{
           kind: :remote,
           database_uuid: uuid,
           base_url: String.trim_trailing(url, "/"),
           auth_token: Map.get(attrs, "auth_token")
         })}

      error ->
        {:error, error}
    end
  end

  defp validate_remote(attrs, uuid, uri) do
    validators = [
      fn -> validate_remote_fields(attrs) end,
      fn -> validate_remote_url(uuid, uri) end,
      fn -> validate_remote_auth_token(attrs) end
    ]

    Enum.find_value(validators, fn validate -> validate.() end)
  end

  defp validate_remote_fields(attrs) do
    if Map.keys(attrs) -- ["kind", "database_uuid", "base_url", "auth_token"] == [] do
      nil
    else
      VialKeeper.Error.invalid_request("unknown remote endpoint field")
    end
  end

  defp validate_remote_url(uuid, uri) do
    valid? =
      valid_uuid?(uuid) and uri.scheme in ["http", "https"] and not is_nil(uri.host) and
        is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment) and
        uri.path in [nil, "", "/"]

    if valid?, do: nil, else: VialKeeper.Error.invalid_request("invalid remote endpoint URL")
  end

  defp validate_remote_auth_token(attrs) do
    if valid_auth_token?(Map.get(attrs, "auth_token")),
      do: nil,
      else: VialKeeper.Error.invalid_request("invalid remote endpoint auth_token")
  end

  # auth_token is optional. When present it must be a non-empty bounded binary
  # (a raw bearer token, not a digest). nil/absent is always valid.
  defp valid_auth_token?(nil), do: true

  defp valid_auth_token?(token) when is_binary(token),
    do: byte_size(token) > 0 and byte_size(token) <= @max_token_bytes

  defp valid_auth_token?(_), do: false

  defp valid_uuid?(uuid),
    do:
      Regex.match?(
        ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        uuid
      )
end
