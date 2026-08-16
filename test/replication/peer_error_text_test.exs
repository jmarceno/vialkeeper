defmodule VialKeeper.Replication.PeerErrorTextTest do
  @moduledoc """
  A remote replication peer cannot make a local node adopt its error text.

  The local node keeps what is locally meaningful (the mapped error code and
  retryability) and replaces peer-authored message/details with fixed local
  text plus a bounded `remote_code` detail only.
  """
  use ExUnit.Case, async: false

  alias Plug.Conn
  alias VialKeeper.Error
  alias VialKeeper.Replication.RemoteTransport
  alias VialKeeper.Replication.WireCompression
  alias VialKeeper.TestServer

  @peer_message "PEER_INSTRUCT_ROLLBACK_TO_42_TOP_SECRET"
  @peer_detail "PEER_SUPER_SECRET_INTERNAL_VALUE"
  @peer_detail_key "peer_secret_hint"

  test "known peer error keeps mapped code, fixed message, retryable, and bounded details" do
    envelope = %{
      "error" => %{
        "code" => "revision_conflict",
        "message" => @peer_message,
        "details" => %{@peer_detail_key => @peer_detail},
        "retryable" => true
      }
    }

    assert {:error,
            %Error{
              code: :revision_conflict,
              message: message,
              retryable: true,
              details: details
            }} = request_error(envelope)

    assert message == "remote endpoint returned an error"
    assert details == %{remote_code: "revision_conflict"}
    refute inspect(details) =~ @peer_message
    refute inspect(details) =~ @peer_detail
  end

  test "unknown peer code maps to internal_error with fixed message" do
    envelope = %{
      "error" => %{
        "code" => "totally_made_up_code",
        "message" => @peer_message,
        "details" => %{@peer_detail_key => @peer_detail},
        "retryable" => true
      }
    }

    assert {:error,
            %Error{code: :internal_error, message: message, retryable: true, details: details}} =
             request_error(envelope)

    assert message == "remote endpoint returned an error"
    assert details == %{remote_code: "totally_made_up_code"}
  end

  test "peer message and details never surface in the public error envelope" do
    envelope = %{
      "error" => %{
        "code" => "revision_conflict",
        "message" => @peer_message,
        "details" => %{@peer_detail_key => @peer_detail},
        "retryable" => false
      }
    }

    assert {:error, %Error{} = error} = request_error(envelope)
    public = Error.public(error)

    assert public.code == "revision_conflict"
    assert public.message == "remote endpoint returned an error"
    assert public.details == %{remote_code: "revision_conflict"}

    rendered = inspect(public)
    refute rendered =~ @peer_message
    refute rendered =~ @peer_detail
  end

  defp request_error(envelope) do
    base_url = start_error_server(409, envelope)

    RemoteTransport.request(
      base_url,
      :get,
      "/v1/databases/peer/replication/identity",
      nil,
      nil,
      2_000
    )
  end

  defp start_error_server(status, envelope) do
    limit = VialKeeper.Config.host_limits()[:max_replication_batch_bytes] || 16_777_216
    {:ok, encoded} = WireCompression.encode_json(envelope, limit)

    server =
      TestServer.start_supervised!(
        request_hook: fn conn ->
          conn =
            conn
            |> Conn.put_resp_header("content-type", "application/json")
            |> Conn.put_resp_header("content-encoding", "zstd")
            |> Conn.put_resp_header(
              "x-vialkeeper-uncompressed-length",
              Integer.to_string(encoded.uncompressed_length)
            )

          {:halt, Conn.send_resp(conn, status, encoded.body)}
        end
      )

    server.base_url
  end
end
