defmodule ElixirDB.HTTP.ReadConsistencyTest do
  use ExUnit.Case, async: true

  import Plug.Conn, only: [put_req_header: 3]
  import Plug.Test, only: [conn: 2]

  alias ElixirDB.HTTP.Request

  test "omitted consistency is eventual" do
    assert {:ok, :eventual} = Request.read_consistency(conn(:get, "/"))
  end

  test "primary and eventual are accepted exactly once" do
    for value <- ["primary", "eventual"] do
      request = conn(:get, "/") |> put_req_header("x-elixirdb-read-consistency", value)
      expected = String.to_existing_atom(value)
      assert {:ok, ^expected} = Request.read_consistency(request)
    end
  end

  test "unknown and duplicate consistency headers are invalid" do
    assert {:error, %{code: :invalid_request}} =
             Request.read_consistency(
               conn(:get, "/")
               |> put_req_header("x-elixirdb-read-consistency", "linearizable")
             )

    base = conn(:get, "/")
    assert %Plug.Conn{} = base

    duplicate = %{
      base
      | req_headers: [
          {"x-elixirdb-read-consistency", "primary"},
          {"x-elixirdb-read-consistency", "eventual"}
        ]
    }

    assert {:error, %{code: :invalid_request}} = Request.read_consistency(duplicate)
  end
end
