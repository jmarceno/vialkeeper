defmodule VialKeeper.HTTP.RoutePath do
  @moduledoc """
  Canonical route-segment view of a request path.

  `Plug.Router` dispatches on `conn.path_info` with empty segments dropped and
  each segment URI-decoded once. Security classification must use the same
  view: the raw `request_path` diverges for spellings such as
  `/v1//control-plane` or `/v1/%63ontrol-plane`, which still route to the same
  handlers.
  """

  @malformed_escape ~r/%(?![0-9A-Fa-f]{2})/

  @doc """
  Returns the decoded segments the router matches for `path`.

  Returns `:error` when a segment carries a malformed percent escape; the
  router rejects such a target as well.
  """
  @spec segments(binary()) :: {:ok, [binary()]} | :error
  def segments(path) when is_binary(path) do
    path
    |> :binary.split("/", [:global])
    |> Enum.reject(&(&1 == ""))
    |> decode_segments([])
  end

  defp decode_segments([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_segments([segment | rest], acc) do
    if Regex.match?(@malformed_escape, segment) do
      :error
    else
      decode_segments(rest, [URI.decode(segment) | acc])
    end
  end
end
