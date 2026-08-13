defmodule ElixirDB.Attachments.MetadataRequestTest do
  @moduledoc "Contract and adversarial coverage for attachment metadata request helpers."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias ElixirDB.Attachments.MetadataRequest
  alias ElixirDB.Error
  alias ElixirDB.TestSupport.GarbageGenerators

  @digest String.duplicate("a", 64)

  describe "request_digest/1" do
    test "accepts digest and blob fallback keys" do
      assert {:ok, @digest} = MetadataRequest.request_digest(%{digest: @digest})
      assert {:ok, @digest} = MetadataRequest.request_digest(%{"blob" => @digest})
    end

    test "rejects absent and junk digest values" do
      for request <- [%{}, %{digest: 42}, %{blob: []}, %{"digest" => String.upcase(@digest)}] do
        assert_error(MetadataRequest.request_digest(request), :invalid_request)
      end
    end
  end

  describe "request_logical_size/1" do
    test "accepts logical_size and length fallback keys" do
      assert {:ok, 0} = MetadataRequest.request_logical_size(%{logical_size: 0})
      assert {:ok, 12} = MetadataRequest.request_logical_size(%{"length" => 12})
    end

    test "rejects invalid and absent sizes" do
      for request <- [
            %{logical_size: -1},
            %{logical_size: 1.5},
            %{logical_size: "5"},
            %{}
          ] do
        assert_error(MetadataRequest.request_logical_size(request), :invalid_request)
      end
    end
  end

  describe "validate_digest_list/1" do
    test "accepts an empty list and valid digests" do
      assert :ok = MetadataRequest.validate_digest_list([])
      assert :ok = MetadataRequest.validate_digest_list([@digest, String.duplicate("b", 64)])
    end

    test "halts at the first invalid member" do
      assert {:error,
              %Error{
                code: :invalid_request,
                message: "attachment digest must be a string"
              }} = MetadataRequest.validate_digest_list([42, String.upcase(@digest)])
    end

    test "rejects non-binary members" do
      for value <- [nil, [], %{}] do
        assert_error(MetadataRequest.validate_digest_list([value]), :invalid_request)
      end
    end
  end

  describe "page_limit/1,2" do
    test "defaults invalid limits to the maximum" do
      assert MetadataRequest.page_limit(nil) == 4096

      for limit <- [0, -5, "x", 1.5] do
        assert MetadataRequest.page_limit(limit, 100) == 100
      end
    end

    test "caps valid limits at the maximum" do
      assert MetadataRequest.page_limit(101, 100) == 100
      assert MetadataRequest.page_limit(10, 100) == 10
      assert MetadataRequest.page_limit(nil, 100) == 100
    end
  end

  describe "validate_after_digest/1" do
    test "accepts nil and lowercase SHA-256 hex" do
      assert :ok = MetadataRequest.validate_after_digest(nil)
      assert :ok = MetadataRequest.validate_after_digest(@digest)
    end

    test "rejects malformed and non-binary cursors" do
      for value <- [
            String.upcase(@digest),
            String.duplicate("a", 63),
            String.duplicate("a", 65),
            42
          ] do
        assert_error(MetadataRequest.validate_after_digest(value), :invalid_request)
      end
    end
  end

  describe "cleanup_now/1" do
    test "uses the current second when now is absent" do
      before = DateTime.utc_now() |> DateTime.truncate(:second)
      assert {:ok, resolved} = MetadataRequest.cleanup_now(%{})
      after_now = DateTime.utc_now() |> DateTime.truncate(:second)

      assert resolved.microsecond == {0, 0}
      assert DateTime.compare(resolved, before) in [:eq, :gt]
      assert DateTime.compare(resolved, after_now) in [:eq, :lt]
    end

    test "truncates DateTime values and parses RFC3339 strings" do
      datetime = DateTime.new!(~D[2026-08-13], ~T[12:34:56.123456], "Etc/UTC")
      expected = DateTime.truncate(datetime, :second)

      assert {:ok, ^expected} = MetadataRequest.cleanup_now(%{now: datetime})

      assert {:ok, ~U[2026-08-13 12:34:56Z]} =
               MetadataRequest.cleanup_now(%{"now" => "2026-08-13T12:34:56Z"})
    end

    test "rejects invalid overrides" do
      for value <- ["garbage", 42, %{}] do
        assert_error(MetadataRequest.cleanup_now(%{now: value}), :invalid_request)
      end
    end
  end

  describe "resolve_revision_id/2" do
    test "reports a document without a winning revision" do
      assert_error(
        MetadataRequest.resolve_revision_id(%{winning_revision: nil}, nil),
        :document_not_found
      )
    end

    test "uses a winning revision or an explicit non-empty revision" do
      assert {:ok, "1-winner"} =
               MetadataRequest.resolve_revision_id(%{winning_revision: "1-winner"}, nil)

      assert {:ok, "2-explicit"} =
               MetadataRequest.resolve_revision_id(%{winning_revision: "1-winner"}, "2-explicit")

      assert {:ok, "2-explicit"} =
               MetadataRequest.resolve_revision_id(%{winning_revision: nil}, "2-explicit")
    end

    test "rejects empty and non-binary revisions" do
      for revision <- ["", 42, %{}] do
        assert_error(
          MetadataRequest.resolve_revision_id(%{winning_revision: "1-winner"}, revision),
          :invalid_request
        )
      end
    end
  end

  describe "pending_digests_from_request/1" do
    test "passes a digest list through without validating it" do
      digests = [@digest, :not_validated]
      assert MetadataRequest.pending_digests_from_request(%{digests: digests}) == digests
    end

    test "falls back to a single digest and drops an invalid one" do
      assert MetadataRequest.pending_digests_from_request(%{blob: @digest}) == [@digest]
      assert MetadataRequest.pending_digests_from_request(%{digest: 42}) == []
      assert MetadataRequest.pending_digests_from_request(%{}) == []
    end
  end

  @tag :slow
  property "map-taking public functions never raise for junk maps" do
    check all(request <- GarbageGenerators.junk_map(), max_runs: 400) do
      assert_ok_or_error(MetadataRequest.request_digest(request), &is_binary/1)

      assert_ok_or_error(
        MetadataRequest.request_logical_size(request),
        &non_negative_integer?/1
      )

      assert_ok_or_error(MetadataRequest.cleanup_now(request), &match?(%DateTime{}, &1))
      assert is_list(MetadataRequest.pending_digests_from_request(request))
      assert_ok_or_error(MetadataRequest.resolve_revision_id(request, nil), &is_binary/1)
    end
  end

  defp assert_error(result, code) do
    assert {:error, %Error{code: ^code}} = result
  end

  defp assert_ok_or_error({:ok, value}, valid_value?), do: assert(valid_value?.(value))
  defp assert_ok_or_error({:error, %Error{}}, _valid_value?), do: assert(true)

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0
end
