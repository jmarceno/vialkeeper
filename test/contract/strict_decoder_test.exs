defmodule ElixirDB.Contract.StrictDecoderTest do
  @moduledoc """
  Adversarial coverage for JSON-002 / JSON-003 via StrictDecoder.

  These cases are the proof corpus for strict-decoder compatibility behavior.
  """
  use ExUnit.Case, async: false

  @moduletag :integration

  alias ElixirDB.JSON.{StrictCache, StrictDecoder}

  @safe_max 9_007_199_254_740_991

  describe "duplicate keys (JSON-002)" do
    test "rejects top-level duplicate object keys" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(~s({"a":1,"a":2}))

      assert message =~ "duplicate"
    end

    test "rejects nested duplicate object keys" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(~s({"a":{"b":1,"b":2}}))

      assert message =~ "duplicate"
    end

    test "accepts distinct nested keys" do
      assert {:ok, %{"a" => %{"b" => 1, "c" => 2}}} =
               StrictDecoder.decode(~s({"a":{"b":1,"c":2}}))
    end
  end

  describe "binary64 safe integers (JSON-003)" do
    test "accepts inclusive safe range bounds" do
      assert {:ok, @safe_max} = StrictDecoder.decode(Integer.to_string(@safe_max))
      assert {:ok, -@safe_max} = StrictDecoder.decode(Integer.to_string(-@safe_max))
    end

    test "rejects integers above the safe range" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(Integer.to_string(@safe_max + 1))

      assert message =~ "binary64"
    end

    test "rejects integers below the safe range" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(Integer.to_string(-(@safe_max + 1)))

      assert message =~ "binary64"
    end
  end

  describe "float overflow and underflow (JSON-003)" do
    test "rejects non-zero underflow to zero" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("1e-400")

      assert message =~ "underflow"
    end

    test "rejects negative non-zero underflow to zero" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("-1e-400")

      assert message =~ "underflow"
    end

    test "accepts explicit zero literals including scientific zero" do
      assert {:ok, zero} = StrictDecoder.decode("0.0")
      assert zero == 0.0

      assert {:ok, neg_zero} = StrictDecoder.decode("-0.0")
      assert neg_zero == 0.0

      assert {:ok, sci_zero} = StrictDecoder.decode("0e10")
      assert sci_zero == 0.0
    end

    test "accepts in-range scientific notation companions to overflow" do
      assert {:ok, value} = StrictDecoder.decode("1e308")
      assert is_float(value)
      assert value > 0.0
    end

    test "rejects overflow to infinity" do
      refute match?({:ok, _}, StrictDecoder.decode("1e309"))

      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("1e309")

      assert message =~ "overflow" or message =~ "infinity"
    end

    test "rejects negative overflow to infinity" do
      refute match?({:ok, _}, StrictDecoder.decode("-1e309"))

      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("-1e309")

      assert message =~ "overflow" or message =~ "infinity"
    end
  end

  describe "non-finite tokens (JSON-002)" do
    test "rejects lexical NaN" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("NaN")

      assert message =~ "invalid" or message =~ "non-finite" or message =~ "number"
    end

    test "rejects lexical Infinity" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("Infinity")

      assert message =~ "invalid" or message =~ "non-finite" or message =~ "number"
    end

    test "rejects lexical -Infinity" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode("-Infinity")

      assert message =~ "invalid" or message =~ "non-finite" or message =~ "number"
    end

    test "rejects nested Infinity object values" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(~s({"x":Infinity}))

      assert message =~ "invalid" or message =~ "non-finite" or message =~ "number"
    end
  end

  describe "nesting depth and size (JSON-002, SEC-001)" do
    test "rejects nesting beyond max_depth option" do
      assert {:error, %ElixirDB.Error{code: :resource_limit}} =
               StrictDecoder.decode("[[[0]]]", max_depth: 1)
    end

    test "uses host-configured nesting depth when option omitted" do
      previous = Application.get_env(:elixir_db, :host_limits)

      on_exit(fn ->
        if previous,
          do: Application.put_env(:elixir_db, :host_limits, previous),
          else: Application.delete_env(:elixir_db, :host_limits)
      end)

      Application.put_env(:elixir_db, :host_limits, max_json_nesting_depth: 1)

      assert {:error, %ElixirDB.Error{code: :resource_limit}} =
               StrictDecoder.decode("[[[0]]]")
    end

    test "preserves configured limits above RustyJson's native ceiling" do
      body = Enum.reduce(1..129, "0", fn _level, value -> "[" <> value <> "]" end)

      assert {:ok, _value} = StrictDecoder.decode(body, max_depth: 129)

      assert {:ok, %{"text" => "quote: \"ok\""}} =
               StrictDecoder.decode(~S({"text":"quote: \"ok\""}), max_depth: 129)

      assert {:ok, %{"number" => value}} =
               StrictDecoder.decode(~s({"number":1e308}), max_depth: 129)

      assert is_float(value)

      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(~s({"number":1e-400}), max_depth: 129)

      assert message =~ "underflow"

      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(~s({"a":1,"a":2}), max_depth: 129)

      assert message =~ "duplicate"
    end

    test "rejects bodies exceeding max_bytes" do
      body = ~s({"a":1})

      assert {:error, %ElixirDB.Error{code: :payload_too_large}} =
               StrictDecoder.decode(body, max_bytes: byte_size(body) - 1)
    end

    test "strict JSON cache keeps nesting limits part of the cache key" do
      cache_name = :strict_decoder_contract

      assert {:error, %ElixirDB.Error{code: :resource_limit}} =
               StrictCache.decode_with_cache("[[0]]", 1, cache_name, 2)

      assert {:ok, [[0]]} = StrictCache.decode_with_cache("[[0]]", 2, cache_name, 2)
    end
  end

  describe "UTF-8 and malformed input (JSON-002)" do
    test "rejects invalid UTF-8" do
      assert {:error, %ElixirDB.Error{code: :invalid_request, message: message}} =
               StrictDecoder.decode(<<255>>)

      assert message =~ "UTF-8"
    end

    test "rejects trailing garbage after a valid value" do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} =
               StrictDecoder.decode("true false")
    end

    test "rejects empty input" do
      assert {:error, %ElixirDB.Error{code: :invalid_request}} = StrictDecoder.decode("")
    end
  end

  describe "fixture-driven reject vectors" do
    test "loads and applies decoder reject fixtures" do
      path = Path.join(:code.priv_dir(:elixir_db), "fixtures/strict_json/rejects.json")
      assert File.exists?(path), "missing #{path}"

      {:ok, fixtures} = path |> File.read!() |> JSON.decode()

      for fixture <- fixtures, fixture["skip"] != true do
        input = fixture["input"]
        expected_code = error_code!(fixture["error_code"])

        assert {:error, %ElixirDB.Error{code: ^expected_code, message: message}} =
                 StrictDecoder.decode(input),
               "expected #{expected_code} for #{fixture["id"]}"

        if fragment = fixture["message_includes"] do
          assert String.contains?(message, fragment),
                 "fixture #{fixture["id"]} message #{inspect(message)} missing #{inspect(fragment)}"
        end
      end
    end
  end

  defp error_code!("invalid_request"), do: :invalid_request
  defp error_code!("resource_limit"), do: :resource_limit
  defp error_code!("payload_too_large"), do: :payload_too_large
end
