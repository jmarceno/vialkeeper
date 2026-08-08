defmodule ElixirDB.Query.Regex do
  @moduledoc """
  Bounded, project-owned regular expression handling for query predicates.

  The compiled pattern is transient.  Only `source/1` is suitable for
  canonical query or plan representations.
  """

  @max_pattern_bytes 1_024
  @match_limit 100_000
  @match_limit_recursion 1_000

  @enforce_keys [:source, :compiled]
  defstruct [:source, :compiled]

  @type t :: %__MODULE__{source: binary(), compiled: term()}

  @spec compile(binary()) :: {:ok, t()} | {:error, ElixirDB.Error.t()}
  def compile(source) when is_binary(source) do
    cond do
      byte_size(source) > @max_pattern_bytes ->
        {:error, ElixirDB.Error.resource_limit("regex pattern exceeds the configured limit")}

      not String.valid?(source) ->
        {:error, ElixirDB.Error.invalid_request("regex pattern must be valid UTF-8")}

      inline_flags?(source) ->
        {:error, ElixirDB.Error.invalid_request("regex inline flags are not supported")}

      true ->
        case :re.compile(source, [:unicode]) do
          {:ok, compiled} ->
            {:ok, %__MODULE__{source: source, compiled: compiled}}

          {:error, reason} ->
            {:error,
             ElixirDB.Error.invalid_request("regex pattern is invalid", %{
               reason: regex_reason(reason)
             })}
        end
    end
  end

  def compile(_), do: {:error, ElixirDB.Error.invalid_request("regex pattern must be a string")}

  @spec match?(t(), term()) :: {:ok, boolean()} | {:error, ElixirDB.Error.t()}
  def match?(%__MODULE__{}, value) when not is_binary(value), do: {:ok, false}

  def match?(%__MODULE__{compiled: compiled}, value) when is_binary(value) do
    case :re.run(value, compiled, [
           {:capture, :none},
           {:match_limit, @match_limit},
           {:match_limit_recursion, @match_limit_recursion},
           :report_errors
         ]) do
      :match ->
        {:ok, true}

      :nomatch ->
        {:ok, false}

      {:error, reason} when reason in [:match_limit, :match_limit_recursion] ->
        {:error, ElixirDB.Error.resource_limit("regex evaluation exceeded its resource limit")}

      {:error, reason} ->
        {:error,
         ElixirDB.Error.invalid_request("regex evaluation failed", %{reason: regex_reason(reason)})}
    end
  rescue
    ArgumentError ->
      {:error, ElixirDB.Error.invalid_request("regex evaluation failed")}
  end

  def match?(_, _), do: {:error, ElixirDB.Error.invalid_request("compiled regex is invalid")}

  @spec source(t()) :: binary()
  def source(%__MODULE__{source: source}), do: source

  defp regex_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp regex_reason({tag, detail}) when is_atom(tag), do: "#{tag}:#{inspect(detail)}"
  defp regex_reason(reason), do: inspect(reason)

  defp inline_flags?(source), do: scan_inline_flags(source, false)

  defp scan_inline_flags(<<>>, _in_character_class), do: false

  defp scan_inline_flags(<<"\\", rest::binary>>, in_character_class) do
    case rest do
      <<_escaped, tail::binary>> -> scan_inline_flags(tail, in_character_class)
      <<>> -> false
    end
  end

  defp scan_inline_flags(<<"[", rest::binary>>, false),
    do: scan_inline_flags(rest, true)

  defp scan_inline_flags(<<"]", rest::binary>>, true),
    do: scan_inline_flags(rest, false)

  defp scan_inline_flags(<<"(?", rest::binary>>, false) do
    {flags, tail} = take_flag_modifiers(rest, [])

    if flags != [] and tail in [")", ":"],
      do: true,
      else: scan_inline_flags(rest, false)
  end

  defp scan_inline_flags(<<_byte, rest::binary>>, in_character_class),
    do: scan_inline_flags(rest, in_character_class)

  defp take_flag_modifiers(<<char, rest::binary>>, acc) when char in ~c"imsUx-" do
    take_flag_modifiers(rest, [char | acc])
  end

  defp take_flag_modifiers(<<tail, _rest::binary>>, acc), do: {Enum.reverse(acc), <<tail>>}
  defp take_flag_modifiers(<<>>, acc), do: {Enum.reverse(acc), ""}
end
