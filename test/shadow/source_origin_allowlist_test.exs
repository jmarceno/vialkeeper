defmodule ElixirDB.Shadow.SourceOriginAllowlistTest do
  use ExUnit.Case, async: false

  alias ElixirDB.Error
  alias ElixirDB.HostConfig
  alias ElixirDB.Shadow.Worker

  @journal_filename "managed_shadows.json"

  setup do
    prefix = "shadow-origin-#{System.unique_integer([:positive])}"
    root = Path.join(ElixirDB.Config.database_root(), prefix)
    managed_root = Path.join(root, "managed")
    external_root = Path.join(root, "external")
    attachment_location = Path.join(external_root, "blobs")
    File.mkdir_p!(attachment_location)

    on_exit(fn ->
      ElixirDB.TempDatabase.cleanup(root)
    end)

    %{
      root: root,
      managed_root: managed_root,
      external_root: external_root,
      attachment_location: attachment_location
    }
  end

  test "HostConfig.canonical_origin normalizes case, root slash, and ports", %{} do
    assert {:ok, "https://example.com"} = HostConfig.canonical_origin("HTTPS://EXAMPLE.com:443/")
    assert {:ok, "http://example.com"} = HostConfig.canonical_origin("HTTP://Example.com")
    assert {:ok, "http://example.com"} = HostConfig.canonical_origin("http://example.com:80/")

    assert {:ok, "https://example.com:8443"} =
             HostConfig.canonical_origin("https://Example.com:8443")

    assert {:ok, "http://[2001:db8::1]"} = HostConfig.canonical_origin("http://[2001:DB8::1]")
  end

  test "malformed or non-canonical origins are rejected", %{} do
    assert {:error, _} = HostConfig.canonical_origin("http://userinfo@host/")
    assert {:error, _} = HostConfig.canonical_origin("http://host/?q=1")
    assert {:error, _} = HostConfig.canonical_origin("http://host/#frag")
    assert {:error, _} = HostConfig.canonical_origin("http://host/path")
    assert {:error, _} = HostConfig.canonical_origin("ftp://host/")
    assert {:error, _} = HostConfig.canonical_origin("http://")
  end

  test "provision with an empty allow-list and a remote source is rejected and writes nothing to the journal",
       ctx do
    opts = opts(ctx, [])

    request =
      provision_request(ctx.attachment_location)
      |> Map.put("source_base_url", "https://evil.example")

    assert {:error, %Error{code: :invalid_request, message: message}} =
             Worker.provision(request, opts)

    assert message =~ "source origin allow-list"
    refute journal_exists?(ctx)
  end

  test "provision with a matching allow-list entry succeeds in any case or root-slash spelling",
       ctx do
    opts = opts(ctx, ["https://example.com:8443"])

    request =
      provision_request(ctx.attachment_location)
      |> Map.put("source_base_url", "HTTPS://Example.com:8443/")

    assert {:ok, _} = Worker.provision(request, opts)
  end

  test "provision with a different-origin URL is rejected", ctx do
    opts = opts(ctx, ["https://example.com:8443"])

    for url <- [
          "https://other.example:8443",
          "http://example.com:8443",
          "https://example.com:9999"
        ] do
      request = provision_request(ctx.attachment_location) |> Map.put("source_base_url", url)
      assert {:error, %Error{code: :invalid_request}} = Worker.provision(request, opts)
    end
  end

  test "provision with a remote URL carrying userinfo, query, fragment, or path is rejected", ctx do
    opts = opts(ctx, ["https://example.com:8443"])

    for url <- [
          "https://user:pass@example.com:8443",
          "https://example.com:8443/?q=1",
          "https://example.com:8443/#frag",
          "https://example.com:8443/path"
        ] do
      request = provision_request(ctx.attachment_location) |> Map.put("source_base_url", url)
      assert {:error, %Error{code: :invalid_request}} = Worker.provision(request, opts)
    end
  end

  test "local provision without a source_base_url proceeds with an empty allow-list", ctx do
    opts = opts(ctx, [])

    request = provision_request(ctx.attachment_location)

    assert {:ok, _} = Worker.provision(request, opts)
  end

  defp opts(ctx, origins) do
    [
      root: ctx.managed_root,
      allowed_attachment_roots: [ctx.external_root],
      allowed_source_origins: origins
    ]
  end

  defp provision_request(attachment_location) do
    %{
      "source_uuid" => ElixirDB.UUID.v4(),
      "shadow_uuid" => ElixirDB.UUID.v4(),
      "generation" => 1,
      "operation_id" => ElixirDB.UUID.v4(),
      "attachment_store_type" => "external_cas",
      "attachment_location" => attachment_location,
      "specification_digest" => String.duplicate("b", 64),
      "source_bearer_token" => nil
    }
  end

  defp journal_exists?(ctx), do: File.exists?(Path.join(ctx.managed_root, @journal_filename))
end
