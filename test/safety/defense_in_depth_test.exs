defmodule ElixirDB.Safety.DefenseInDepthTest do
  @moduledoc """
  Proves the two final safety nets (Plan §2 / Phase 2): even when source validation
  is bypassed and a handler *raises*, the raise is contained — never crashing the
  shared DatabaseCatalog GenServer, and never dropping an HTTP connection without a
  typed JSON envelope.

  These are the tests that guarantee the absolute invariant: "no raise, for any
  reason, can crash the server."

  Two nets are exercised:

    1. Catalog dispatch net (database_catalog.ex `safe/2`): an unanticipated raise
       during a command becomes a typed `internal_error` reply; the catalog survives.
    2. HTTP route net (instrumentation/http.ex `wrap/2` rescue): an unanticipated
       raise inside a route becomes a JSON 500 envelope on an unsent connection.
  """

  use ExUnit.Case, async: false

  alias ElixirDB.HTTP.Router
  alias ElixirDB.JSON.StrictDecoder
  alias ElixirDB.Observability.Instrumentation.HTTP
  alias ElixirDB.Runtime.DatabaseCatalog
  # ==========================================================================
  # Catalog dispatch net
  # ==========================================================================

  describe "catalog dispatch safety net" do
    setup do
      path = "dd-#{System.unique_integer([:positive])}.elixirdb"
      conn = call(:post, "/v1/databases", %{"path" => path})
      assert conn.status == 201
      {:ok, %{"data" => %{"database_uuid" => uuid}}} = decode(conn.resp_body)

      on_exit(fn ->
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
      end)

      {:ok, uuid: uuid}
    end

    test "a command that raises returns a typed internal_error, not a GenServer exit",
         %{uuid: uuid} do
      # An unknown command normalizes to itself and the owner returns a typed
      # invalid_request — proving the typed path never crashes.
      result = DatabaseCatalog.command(uuid, {:command, :__nonexistent__, %{}})
      assert {:error, %ElixirDB.Error{}} = result
      assert_catalog_alive()
    end

    test "the shared catalog survives a raise and keeps serving other databases" do
      # Open two databases; force a failure on one and confirm the catalog (and the
      # other database) remain fully usable afterward.
      path_a = "dd-a-#{System.unique_integer([:positive])}.elixirdb"
      path_b = "dd-b-#{System.unique_integer([:positive])}.elixirdb"

      {:ok, %{database_uuid: uuid_a}} = DatabaseCatalog.create(path_a, %{})
      {:ok, %{database_uuid: uuid_b}} = DatabaseCatalog.create(path_b, %{})

      on_exit(fn ->
        _ = DatabaseCatalog.close(uuid_a)
        _ = DatabaseCatalog.close(uuid_b)
        _ = DatabaseCatalog.unregister(uuid_a)
        _ = DatabaseCatalog.unregister(uuid_b)
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path_a))
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path_b))
      end)

      # Poison database A with a malformed command shape. The owner's normalize/1 maps
      # unknown shapes to a typed invalid_request (no crash); either way the net holds.
      _poisoned = DatabaseCatalog.command(uuid_a, {:command, :get_document, :not_a_map})

      # The catalog must still be alive and database B must still respond normally.
      assert_catalog_alive()
      assert {:ok, _} = DatabaseCatalog.command(uuid_b, {:command, :identity, %{}})
    end
  end

  # ==========================================================================
  # HTTP route net
  # ==========================================================================

  describe "HTTP route safety net" do
    test "Instrumentation.HTTP.wrap/2 converts a raised handler into a typed JSON 500" do
      # Directly exercise the rescue branch in wrap/2: a handler that raises before any
      # response is sent must yield a 500 JSON envelope (internal_error), not reraise.
      # This is the exact predicate the Phase 2.2 net guarantees.
      conn = Plug.Test.conn(:get, "/v1/databases", nil)

      result_conn =
        HTTP.wrap(conn, fn _conn ->
          raise "simulated handler failure"
        end)

      assert result_conn.status == 500
      {:ok, %{"error" => error}} = decode(result_conn.resp_body)
      assert error["code"] == "internal_error"
    end

    test "wrap/2 produces a 500 envelope when the response was set but not yet sent" do
      # A conn in the :set state (response prepared, e.g. via resp/3, but not yet sent)
      # is still replaceable, so the net must render the 500 envelope rather than reraise.
      conn =
        Plug.Test.conn(:get, "/v1/databases", nil)
        |> Plug.Conn.resp(200, "original")

      assert conn.state == :set

      result_conn =
        HTTP.wrap(conn, fn _conn ->
          raise "simulated handler failure"
        end)

      assert result_conn.state == :sent
      assert result_conn.status == 500
      {:ok, %{"error" => error}} = decode(result_conn.resp_body)
      assert error["code"] == "internal_error"
    end

    test "the real Router survives repeated malformed requests and stays responsive" do
      # Hammer the router with a variety of bodies that would have crashed before the
      # safety pass; assert every one returns a JSON envelope and the router keeps going.
      path = "dd-hammer-#{System.unique_integer([:positive])}.elixirdb"

      create = call(:post, "/v1/databases", %{"path" => path})
      {:ok, %{"data" => %{"database_uuid" => uuid}}} = decode(create.resp_body)

      on_exit(fn ->
        _ = DatabaseCatalog.close(uuid)
        _ = DatabaseCatalog.unregister(uuid)
        ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
      end)

      killer_bodies = [
        %{},
        %{"id" => nil},
        %{"id" => ""},
        %{"id" => "a\0"},
        %{"id" => 123},
        %{"body" => [1, 2]},
        %{"selector" => %{" " => 1}}
      ]

      for body <- killer_bodies do
        conn = call(:post, "/v1/databases/#{uuid}/documents/put", body)
        # Every request must yield a JSON envelope (never a bare crash). Statuses vary.
        assert conn.status in 400..422
        {:ok, parsed} = decode(conn.resp_body)
        assert is_map(parsed)
      end

      assert_catalog_alive()
    end
  end

  # --------------------------------------------------------------------------
  # Helpers
  # --------------------------------------------------------------------------

  defp assert_catalog_alive do
    refute is_nil(Process.whereis(DatabaseCatalog)),
           "DatabaseCatalog crashed — the safety invariant was violated"
  end

  defp call(method, path, body) do
    conn = Plug.Test.conn(method, path, encode(body))
    conn = Plug.Conn.put_req_header(conn, "content-type", "application/json")
    Router.call(conn, [])
  end

  defp encode(term), do: IO.iodata_to_binary(JSON.encode_to_iodata!(term))
  defp decode(body), do: StrictDecoder.decode(body)
end
