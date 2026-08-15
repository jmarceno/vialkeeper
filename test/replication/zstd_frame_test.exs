defmodule VialKeeper.Replication.ZstdFrameTest do
  @moduledoc "Structural coverage for exactly one ordinary Zstandard frame."

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias VialKeeper.Error
  alias VialKeeper.Replication.ZstdFrame

  @magic <<0x28, 0xB5, 0x2F, 0xFD>>

  describe "valid header variants" do
    test "accepts single-segment 1-byte FCS with no dictionary or checksum" do
      frame = frame(content: "hi")
      assert {:ok, %{content_size: 2, frame_size: size}} = ZstdFrame.validate(frame)
      assert size == byte_size(frame)
    end

    test "accepts dictionary ID lengths 1, 2, and 4" do
      for did_flag <- [1, 2, 3] do
        frame = frame(content: "ok", did_flag: did_flag, dictionary_id: did_bytes(did_flag))
        assert {:ok, %{content_size: 2}} = ZstdFrame.validate(frame)
      end
    end

    test "accepts 2-byte FCS with window descriptor" do
      content = :binary.copy(<<0>>, 256)
      frame = frame(content: content, fcs_flag: 1, single_segment?: false, window: 0x60)
      assert {:ok, %{content_size: 256}} = ZstdFrame.validate(frame)
    end

    test "accepts 4-byte FCS with window descriptor" do
      frame = frame(content: "abcd", fcs_flag: 2, single_segment?: false, window: 0x50)
      assert {:ok, %{content_size: 4}} = ZstdFrame.validate(frame)
    end

    test "accepts 8-byte FCS" do
      frame = frame(content: "xyz", fcs_flag: 3)
      assert {:ok, %{content_size: 3}} = ZstdFrame.validate(frame)
    end

    test "accepts content checksum trailer" do
      frame = frame(content: "hi", checksum?: true, checksum: <<1, 2, 3, 4>>)
      assert {:ok, %{content_size: 2, frame_size: size}} = ZstdFrame.validate(frame)
      assert size == byte_size(frame)
    end

    test "accepts raw, RLE, and compressed last blocks" do
      raw = frame(content: "raw", block_type: 0)
      rle = rle_frame(<<?a>>, 4)
      compressed = frame(content: <<1, 2, 3>>, block_type: 2)

      assert {:ok, %{content_size: 3}} = ZstdFrame.validate(raw)
      assert {:ok, %{content_size: 4}} = ZstdFrame.validate(rle)
      assert {:ok, %{content_size: 3}} = ZstdFrame.validate(compressed)
    end

    test "accepts a non-last raw block followed by a last raw block" do
      first = block_header(last?: false, type: 0, size: 2) <> "ab"
      last = block_header(last?: true, type: 0, size: 2) <> "cd"
      frame = header(content_size: 4) <> first <> last
      assert {:ok, %{content_size: 4}} = ZstdFrame.validate(frame)
    end

    test "accepts an ezstd level-1 frame that declares content size" do
      compressed = :ezstd.compress("{}", 1)
      assert is_binary(compressed)
      assert {:ok, info} = ZstdFrame.validate(compressed)
      assert info.content_size == 2
      assert info.frame_size == byte_size(compressed)
    end
  end

  describe "invalid frames" do
    test "rejects corrupt magic" do
      assert {:error, %Error{code: :invalid_request}} =
               ZstdFrame.validate(<<0, 1, 2, 3, 4, 5, 6, 7>>)
    end

    test "rejects skippable frames" do
      for nibble <- 0x50..0x5F do
        assert {:error, %Error{code: :invalid_request}} =
                 ZstdFrame.validate(<<nibble, 0x2A, 0x4D, 0x18, 0, 0, 0, 0>>)
      end
    end

    test "rejects unused and reserved descriptor bits" do
      unused = header(content_size: 1, unused?: true) <> block_header(size: 1) <> "x"
      reserved = header(content_size: 1, reserved?: true) <> block_header(size: 1) <> "x"

      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(unused)
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(reserved)
    end

    test "rejects unknown content size" do
      descriptor = descriptor(fcs_flag: 0, single_segment?: false)
      frame = @magic <> <<descriptor, 0x60>> <> block_header(size: 1) <> "x"
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(frame)
    end

    test "rejects truncated header, block payload, and checksum" do
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(@magic)
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(@magic <> <<0x20>>)

      truncated_block = header(content_size: 4) <> block_header(size: 4) <> "xy"
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(truncated_block)

      truncated_checksum =
        header(content_size: 1, checksum?: true) <> block_header(size: 1) <> "x" <> <<1, 2>>

      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(truncated_checksum)
    end

    test "rejects reserved block type" do
      frame = header(content_size: 1) <> block_header(type: 3, size: 1) <> "x"
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(frame)
    end

    test "rejects concatenated frames and trailing bytes" do
      first = frame(content: "ab")
      second = frame(content: "cd")
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(first <> second)
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(first <> <<0>>)
    end

    test "rejects expected content size mismatch without allocating" do
      frame = frame(content: "abcd", fcs_flag: 3)
      baseline = process_memory()

      assert {:error, %Error{code: :invalid_request}} =
               ZstdFrame.validate(frame, expected_content_size: 1)

      huge =
        header(content_size: 1_099_511_627_776, fcs_flag: 3) <>
          block_header(type: 2, size: 4) <> <<0, 1, 2, 3>>

      assert {:error, %Error{code: :invalid_request}} =
               ZstdFrame.validate(huge, expected_content_size: 4)

      assert {:ok, %{content_size: 1_099_511_627_776}} = ZstdFrame.validate(huge)
      assert process_memory() <= baseline + 256 * 1024
    end

    test "never raises on non-binary input" do
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(:not_a_frame)
      assert {:error, %Error{code: :invalid_request}} = ZstdFrame.validate(nil)
    end
  end

  @tag :slow
  property "random binaries never raise" do
    check all(binary <- StreamData.binary(max_length: 512), max_runs: 400) do
      result = ZstdFrame.validate(binary)
      assert match?({:ok, %{}}, result) or match?({:error, %Error{}}, result)

      expected_result = ZstdFrame.validate(binary, expected_content_size: 0)
      assert match?({:ok, %{}}, expected_result) or match?({:error, %Error{}}, expected_result)
    end
  end

  @tag :slow
  test "bounded random loop never raises" do
    for _ <- 1..300 do
      binary = :crypto.strong_rand_bytes(:rand.uniform(256))
      result = ZstdFrame.validate(binary)
      assert match?({:ok, _}, result) or match?({:error, %Error{}}, result)
    end
  end

  defp frame(opts) do
    content = Keyword.get(opts, :content, "hi")
    block_type = Keyword.get(opts, :block_type, 0)
    payload = if block_type == 1, do: binary_part(content, 0, 1), else: content
    size = if block_type == 1, do: byte_size(content), else: byte_size(payload)

    header(Keyword.put(opts, :content_size, byte_size(content))) <>
      block_header(last?: true, type: block_type, size: size) <>
      payload <>
      checksum_bytes(opts)
  end

  defp rle_frame(byte, repeat) when byte_size(byte) == 1 do
    header(content_size: repeat) <>
      block_header(last?: true, type: 1, size: repeat) <>
      byte
  end

  defp header(opts) do
    content_size = Keyword.fetch!(opts, :content_size)
    fcs_flag = Keyword.get(opts, :fcs_flag, default_fcs_flag(content_size, opts))
    single_segment? = Keyword.get(opts, :single_segment?, fcs_flag == 0)
    unused? = Keyword.get(opts, :unused?, false)
    reserved? = Keyword.get(opts, :reserved?, false)
    checksum? = Keyword.get(opts, :checksum?, false)
    did_flag = Keyword.get(opts, :did_flag, 0)
    dictionary_id = Keyword.get(opts, :dictionary_id, <<>>)
    window = Keyword.get(opts, :window)

    @magic <>
      <<descriptor(
          fcs_flag: fcs_flag,
          single_segment?: single_segment?,
          unused?: unused?,
          reserved?: reserved?,
          checksum?: checksum?,
          did_flag: did_flag
        )>> <>
      window_bytes(single_segment?, window) <>
      dictionary_id <>
      fcs_bytes(fcs_flag, single_segment?, content_size)
  end

  defp default_fcs_flag(size, opts) do
    cond do
      Keyword.has_key?(opts, :fcs_flag) -> Keyword.fetch!(opts, :fcs_flag)
      Keyword.get(opts, :single_segment?) == false and size >= 256 -> 1
      Keyword.get(opts, :single_segment?) == false -> 2
      true -> 0
    end
  end

  defp descriptor(opts) do
    fcs_flag = Keyword.fetch!(opts, :fcs_flag)
    did_flag = Keyword.get(opts, :did_flag, 0)

    fcs_flag * 64 +
      bool_bit(Keyword.get(opts, :single_segment?, true), 32) +
      bool_bit(Keyword.get(opts, :unused?, false), 16) +
      bool_bit(Keyword.get(opts, :reserved?, false), 8) +
      bool_bit(Keyword.get(opts, :checksum?, false), 4) +
      did_flag
  end

  defp bool_bit(true, weight), do: weight
  defp bool_bit(false, _weight), do: 0

  defp window_bytes(true, _window), do: <<>>
  defp window_bytes(false, window) when is_integer(window), do: <<window>>
  defp window_bytes(false, _), do: <<0x60>>

  defp fcs_bytes(0, false, _size), do: <<>>
  defp fcs_bytes(0, true, size), do: <<size::unsigned-8>>
  defp fcs_bytes(1, _ss, size), do: <<size - 256::unsigned-little-16>>
  defp fcs_bytes(2, _ss, size), do: <<size::unsigned-little-32>>
  defp fcs_bytes(3, _ss, size), do: <<size::unsigned-little-64>>

  defp block_header(opts) do
    last = if Keyword.get(opts, :last?, true), do: 1, else: 0
    type = Keyword.get(opts, :type, 0)
    size = Keyword.get(opts, :size, 0)
    <<last + type * 2 + size * 8::unsigned-little-24>>
  end

  defp checksum_bytes(opts) do
    if Keyword.get(opts, :checksum?, false) do
      Keyword.get(opts, :checksum, <<0, 0, 0, 0>>)
    else
      <<>>
    end
  end

  defp did_bytes(1), do: <<7>>
  defp did_bytes(2), do: <<7, 0>>
  defp did_bytes(3), do: <<7, 0, 0, 0>>

  defp process_memory do
    :erlang.garbage_collect()
    {:memory, memory} = :erlang.process_info(self(), :memory)
    memory
  end
end
