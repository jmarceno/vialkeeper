defmodule VialKeeper.WebUI.Assets do
  @moduledoc """
  Compile-time embedded Web UI assets served from the release BEAM modules.

  Source files under `assets/web_ui/` are read at compile time via
  `@external_resource` so the release does not depend on a runtime filesystem
  copy of those files. Asset lookup is a fixed allow-list; arbitrary path
  resolution is rejected.
  """

  @htmx_version "2.0.7"
  @htmx_expected_size 51_076
  @htmx_expected_sha256 "60231ae6ba9db3825eb15a261122d5f55921c4d53b66bf637dc18b4ee27c79f9"

  @asset_root Path.expand("../../../assets/web_ui", __DIR__)

  @htmx_path Path.join([@asset_root, "vendor", "htmx.min.js"])
  @htmx_license_path Path.join([@asset_root, "vendor", "HTMX-LICENSE.txt"])
  @css_path Path.join([@asset_root, "app.css"])
  @bootstrap_path Path.join([@asset_root, "auth-bootstrap.js"])

  @external_resource @htmx_path
  @external_resource @htmx_license_path
  @external_resource @css_path
  @external_resource @bootstrap_path

  @htmx_body File.read!(@htmx_path)
  @htmx_license File.read!(@htmx_license_path)
  @css_body File.read!(@css_path)
  @bootstrap_body File.read!(@bootstrap_path)

  @htmx_sha256 :crypto.hash(:sha256, @htmx_body) |> Base.encode16(case: :lower)
  @css_sha256 :crypto.hash(:sha256, @css_body) |> Base.encode16(case: :lower)
  @bootstrap_sha256 :crypto.hash(:sha256, @bootstrap_body) |> Base.encode16(case: :lower)

  if byte_size(@htmx_body) != @htmx_expected_size do
    raise "vendored HTMX #{@htmx_version} size mismatch: expected #{@htmx_expected_size}, got #{byte_size(@htmx_body)}"
  end

  if @htmx_sha256 != @htmx_expected_sha256 do
    raise "vendored HTMX #{@htmx_version} digest mismatch: expected #{@htmx_expected_sha256}, got #{@htmx_sha256}"
  end

  @assets %{
    "htmx.min.js" => %{
      body: @htmx_body,
      gzip_body: :zlib.gzip(@htmx_body),
      content_type: "text/javascript; charset=utf-8",
      etag: @htmx_sha256
    },
    "app.css" => %{
      body: @css_body,
      gzip_body: :zlib.gzip(@css_body),
      content_type: "text/css; charset=utf-8",
      etag: @css_sha256
    },
    "auth-bootstrap.js" => %{
      body: @bootstrap_body,
      gzip_body: :zlib.gzip(@bootstrap_body),
      content_type: "text/javascript; charset=utf-8",
      etag: @bootstrap_sha256
    }
  }

  @names Map.keys(@assets)

  @doc """
  Frozen HTMX vendor version string.
  """
  @spec htmx_version() :: String.t()
  def htmx_version, do: @htmx_version

  @doc """
  Expected byte size of the vendored HTMX release asset.
  """
  @spec htmx_expected_size() :: non_neg_integer()
  def htmx_expected_size, do: @htmx_expected_size

  @doc """
  Expected lowercase SHA-256 digest of the vendored HTMX release asset.
  """
  @spec htmx_expected_sha256() :: String.t()
  def htmx_expected_sha256, do: @htmx_expected_sha256

  @doc """
  Compiled HTMX license text retained for release legal metadata.
  """
  @spec htmx_license() :: String.t()
  def htmx_license, do: @htmx_license

  @doc """
  Fixed allow-list of asset names served by the console.
  """
  @spec names() :: [String.t()]
  def names, do: @names

  @doc """
  Fetches an embedded asset by fixed name.

  Returns `{:ok, asset}` or `:error` for unknown or traversal names.
  """
  @spec fetch(String.t()) ::
          {:ok,
           %{
             body: binary(),
             gzip_body: binary(),
             content_type: String.t(),
             etag: String.t()
           }}
          | :error
  def fetch(name) when is_binary(name), do: Map.fetch(@assets, name)
  def fetch(_), do: :error

  @doc """
  Returns true when `name` is an allow-listed asset filename.
  """
  @spec known_name?(String.t()) :: boolean()
  def known_name?(name) when is_binary(name), do: Map.has_key?(@assets, name)
  def known_name?(_), do: false
end
