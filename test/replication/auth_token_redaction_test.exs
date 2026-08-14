defmodule ElixirDB.Replication.AuthTokenRedactionTest do
  @moduledoc """
  Guards that raw stored replication `auth_token` values never leak through the
  public `ElixirDB.Replication.JobManager` read API, while round-trips and
  lifecycle paths keep operating with the real credential.
  """

  alias ElixirDB.Replication.JobManager
  alias ElixirDB.Runtime.DatabaseCatalog
  use ExUnit.Case, async: false

  @moduletag :integration

  @remote_uuid "11111111-1111-4111-8111-111111111111"
  @redacted "[redacted]"

  setup do
    prefix = "auth-token-%{System.unique_integer([:positive])}"
    path = prefix <> ".elixirdb"
    ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    {:ok, identity} = DatabaseCatalog.create(path)

    on_exit(fn ->
      _ = DatabaseCatalog.close(identity.database_uuid)
      _ = DatabaseCatalog.unregister(identity.database_uuid)
      ElixirDB.TempDatabase.cleanup(Path.join(ElixirDB.Config.database_root(), path))
    end)

    {:ok, uuid: identity.database_uuid}
  end

  defp remote_definition(opts \\ %{}) do
    %{
      "persist" => true,
      "mode" => "one_shot",
      "direction" => "push",
      "enabled" => false,
      "endpoint" => %{
        "kind" => "remote",
        "database_uuid" => @remote_uuid,
        "base_url" => "http://127.0.0.1:9"
      }
    }
    |> Map.merge(opts)
  end

  test "list and get redact the stored token and never leak the raw value", %{uuid: uuid} do
    secret = "super-secret-#{System.unique_integer([:positive])}"

    assert {:ok, %{job_id: job_id}} =
             JobManager.put(
               uuid,
               remote_definition(%{
                 "endpoint" => %{
                   "kind" => "remote",
                   "database_uuid" => @remote_uuid,
                   "base_url" => "http://127.0.0.1:9",
                   "auth_token" => secret
                 }
               })
             )

    assert {:ok, job} = JobManager.get(uuid, job_id)
    assert get_in(job.definition, ["endpoint", "auth_token"]) == @redacted
    refute inspect(job.definition) =~ secret

    assert {:ok, jobs} = JobManager.list(uuid)
    listed = Enum.find(jobs, &(&1.job_id == job_id))
    assert listed
    assert get_in(listed.definition, ["endpoint", "auth_token"]) == @redacted
    refute inspect(listed.definition) =~ secret

    assert JobManager.stored_remote_auth_token(uuid, job_id) == secret
  end

  test "round-tripping a redacted definition preserves the stored token", %{uuid: uuid} do
    secret = "roundtrip-secret-#{System.unique_integer([:positive])}"

    assert {:ok, %{job_id: job_id}} =
             JobManager.put(
               uuid,
               remote_definition() |> put_in(["endpoint", "auth_token"], secret)
             )

    assert {:ok, redacted} = JobManager.get(uuid, job_id)
    assert get_in(redacted.definition, ["endpoint", "auth_token"]) == @redacted

    assert {:ok, _} = JobManager.put(uuid, redacted.definition)
    assert JobManager.stored_remote_auth_token(uuid, job_id) == secret

    assert {:ok, %{state: :disabled}} = JobManager.disable(uuid, job_id)
    assert JobManager.stored_remote_auth_token(uuid, job_id) == secret
  end

  test "an explicit new token replaces the stored token", %{uuid: uuid} do
    old_token = "old-token-#{System.unique_integer([:positive])}"
    new_token = "new-token-#{System.unique_integer([:positive])}"

    assert {:ok, %{job_id: job_id}} =
             JobManager.put(
               uuid,
               remote_definition() |> put_in(["endpoint", "auth_token"], old_token)
             )

    assert JobManager.stored_remote_auth_token(uuid, job_id) == old_token

    definition =
      remote_definition()
      |> Map.put("job_id", job_id)
      |> put_in(["endpoint", "auth_token"], new_token)

    assert {:ok, _} = JobManager.put(uuid, definition)
    assert JobManager.stored_remote_auth_token(uuid, job_id) == new_token
  end

  test "local endpoint definitions are unaffected by redaction", %{uuid: uuid} do
    local = %{
      "persist" => true,
      "mode" => "one_shot",
      "direction" => "push",
      "enabled" => false,
      "endpoint" => %{"kind" => "local", "database_uuid" => uuid}
    }

    assert {:ok, %{job_id: job_id}} = JobManager.put(uuid, local)

    assert {:ok, job} = JobManager.get(uuid, job_id)
    assert Map.has_key?(job.definition, "endpoint")
    refute Map.has_key?(get_in(job.definition, ["endpoint"]), "auth_token")
  end
end
