defmodule VialKeeper.ReplicationHarness.FrankensteinCorpus do
  @moduledoc """
  Splits the bundled Frankenstein markdown into letter and chapter documents
  for the replication lab full-text search panel.
  """

  @heading ~r/^## (Letter|Chapter) (\d+)\s*$/
  @excerpt_graphemes 280

  @doc "Reads and parses the corpus, raising if the file is missing or empty of sections."
  @spec documents!(Path.t()) :: [map()]
  def documents!(path) do
    case documents(path) do
      {:ok, [_ | _] = docs} ->
        docs

      {:ok, []} ->
        raise ArgumentError, "Frankenstein corpus produced no letter or chapter documents"

      {:error, reason} ->
        raise ArgumentError, "could not read Frankenstein corpus: #{inspect(reason)}"
    end
  end

  @doc "Reads and parses the corpus into put-ready document maps."
  @spec documents(Path.t()) :: {:ok, [map()]} | {:error, File.posix()}
  def documents(path) do
    case File.read(path) do
      {:ok, markdown} -> {:ok, parse(markdown)}
      {:error, _} = error -> error
    end
  end

  @doc "Parses Project Gutenberg markdown into letter and chapter documents."
  @spec parse(binary()) :: [map()]
  def parse(markdown) when is_binary(markdown) do
    markdown
    |> literary_text()
    |> String.split("\n", trim: false)
    |> Enum.reduce({[], nil}, &consume_line/2)
    |> flush_last()
    |> Enum.reverse()
  end

  defp literary_text(markdown) do
    case String.split(markdown, "END OF THE PROJECT GUTENBERG", parts: 2) do
      [before, _] -> before
      [whole] -> whole
    end
  end

  defp consume_line(line, {acc, current}) do
    case Regex.run(@heading, line) do
      [_, kind, ordinal] ->
        {flush_current(acc, current), start_section(kind, ordinal)}

      _ ->
        {acc, append_line(current, line)}
    end
  end

  defp start_section(kind, ordinal) do
    %{
      id: "#{String.downcase(kind)}-#{ordinal}",
      title: "#{kind} #{ordinal}",
      part: String.downcase(kind),
      ordinal: String.to_integer(ordinal),
      lines: []
    }
  end

  defp append_line(nil, _line), do: nil
  defp append_line(current, line), do: %{current | lines: [line | current.lines]}

  defp flush_last({acc, current}), do: flush_current(acc, current)

  defp flush_current(acc, nil), do: acc

  defp flush_current(acc, current) do
    text =
      current.lines
      |> Enum.reverse()
      |> Enum.join("\n")
      |> String.trim()

    [
      %{
        id: current.id,
        body: %{
          "title" => current.title,
          "part" => current.part,
          "ordinal" => current.ordinal,
          "text" => text,
          "excerpt" => excerpt(text)
        }
      }
      | acc
    ]
  end

  defp excerpt(text) do
    collapsed = text |> String.replace(~r/\s+/u, " ") |> String.trim()

    if String.length(collapsed) <= @excerpt_graphemes do
      collapsed
    else
      String.slice(collapsed, 0, @excerpt_graphemes - 1) <> "…"
    end
  end
end
