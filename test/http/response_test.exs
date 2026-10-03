defmodule VialKeeper.HTTP.ResponseTest do
  @moduledoc "Covers JSON response envelopes, headers, and request ids."
  use ExUnit.Case, async: true

  alias VialKeeper.HTTP.Response
  alias VialKeeper.JSON.StrictDecoder
  alias VialKeeper.Storage.Results

  defp conn(headers \\ []) do
    Enum.reduce(headers, Plug.Test.conn(:post, "/v1/x"), fn {key, value}, conn ->
      Plug.Conn.put_req_header(conn, key, value)
    end)
  end

  defp decode!(conn) do
    {:ok, body} = StrictDecoder.decode(conn.resp_body)
    body
  end

  test "a well-formed caller request id is echoed in the header and envelope" do
    conn = Response.ok(conn([{"x-request-id", "abc.DEF_12-3"}]), %{"n" => 1})

    assert Plug.Conn.get_resp_header(conn, "x-request-id") == ["abc.DEF_12-3"]
    assert Plug.Conn.get_resp_header(conn, "content-type") == ["application/json; charset=utf-8"]
    assert %{"request_id" => "abc.DEF_12-3", "data" => %{"n" => 1}} = decode!(conn)
  end

  test "a malformed or oversized request id is replaced by one fresh UUID" do
    for value <- ["has space", "semi;colon", String.duplicate("a", 129)] do
      conn = Response.ok(conn([{"x-request-id", value}]), %{})
      [header] = Plug.Conn.get_resp_header(conn, "x-request-id")

      assert header =~ ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
      assert decode!(conn)["request_id"] == header
    end
  end

  test "an error envelope carries the same request id as its header" do
    conn = Response.error(conn(), VialKeeper.Error.invalid_request("nope"))
    [header] = Plug.Conn.get_resp_header(conn, "x-request-id")

    assert conn.status == 400
    assert %{"request_id" => ^header, "error" => %{"code" => "invalid_request"}} = decode!(conn)
  end

  test "query rows shared by documents and results are encoded once and identically" do
    rows = [
      %{id: "a", revision: "1-x", body: %{"title" => "A", "nested" => %{"k" => [1, nil]}}},
      %{id: "b", revision: "1-y", fields: %{"/title" => "B"}}
    ]

    result = %{documents: rows, results: rows, has_more: false, bookmark: nil, sequence: 4}
    data = decode!(Response.result(conn(), {:ok, result}))["data"]

    expected = [
      %{
        "id" => "a",
        "revision" => "1-x",
        "body" => %{"title" => "A", "nested" => %{"k" => [1, nil]}}
      },
      %{"id" => "b", "revision" => "1-y", "fields" => %{"/title" => "B"}}
    ]

    assert data["documents"] == expected
    assert data["results"] == expected
    assert %{"has_more" => false, "bookmark" => nil, "sequence" => 4} = data
  end

  test "to_public passes document bodies through and converts envelopes" do
    body = %{"a" => [%{"b" => 1}]}

    assert Results.to_public(%{documents: [%{id: "x", body: body}], has_more: true}) ==
             %{"documents" => [%{"id" => "x", "body" => body}], "has_more" => true}
  end
end
