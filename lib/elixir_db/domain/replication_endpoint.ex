defmodule ElixirDB.Domain.ReplicationEndpoint do
  @enforce_keys [:kind, :database_uuid]
  defstruct [:kind, :database_uuid, :base_url]

  def new(attrs) when is_map(attrs) do
    attrs = stringify_keys(attrs)

    case attrs do
      %{"kind" => "local", "database_uuid" => uuid} when is_binary(uuid) ->
        if Map.keys(attrs) -- ["kind", "database_uuid"] == [] and valid_uuid?(uuid),
          do: {:ok, struct(__MODULE__, %{kind: :local, database_uuid: uuid})},
          else: {:error, ElixirDB.Error.invalid_request("invalid local endpoint")}

      %{"kind" => "remote", "database_uuid" => uuid, "base_url" => url}
      when is_binary(uuid) and is_binary(url) ->
        new_remote(attrs, uuid, url)

      _ ->
        {:error, ElixirDB.Error.invalid_request("invalid replication endpoint")}
    end
  end

  def new(_), do: {:error, ElixirDB.Error.invalid_request("invalid replication endpoint")}

  defp new_remote(attrs, uuid, url) do
    uri = URI.parse(url)

    cond do
      Map.keys(attrs) -- ["kind", "database_uuid", "base_url"] != [] ->
        {:error, ElixirDB.Error.invalid_request("unknown remote endpoint field")}

      not valid_uuid?(uuid) or uri.scheme not in ["http", "https"] or is_nil(uri.host) or
        uri.userinfo != nil or uri.query != nil or uri.fragment != nil or
          uri.path not in [nil, "", "/"] ->
        {:error, ElixirDB.Error.invalid_request("invalid remote endpoint URL")}

      true ->
        {:ok,
         struct(__MODULE__, %{
           kind: :remote,
           database_uuid: uuid,
           base_url: String.trim_trailing(url, "/")
         })}
    end
  end

  defp valid_uuid?(uuid),
    do:
      Regex.match?(
        ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i,
        uuid
      )

  defp stringify_keys(value) when is_map(value),
    do: Map.new(value, fn {key, child} -> {to_string(key), stringify_keys(child)} end)

  defp stringify_keys(value), do: value
end
