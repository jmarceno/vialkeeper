defmodule ElixirDB.DerivedView.Path do
  @moduledoc "Generates portable, human-readable derived bundle paths."

  @max_slug_bytes 48

  @spec for(binary(), binary()) :: binary()
  def for(name, uuid) when is_binary(name) and is_binary(uuid) do
    slug =
      name
      |> :unicode.characters_to_nfkd_binary()
      |> String.downcase()
      |> strip_combining_marks()
      |> String.replace(~r/[^a-z0-9]+/u, "-")
      |> String.trim("-")

    slug =
      if byte_size(slug) > @max_slug_bytes, do: binary_part(slug, 0, @max_slug_bytes), else: slug

    slug = if slug == "", do: "view", else: slug
    uuid_prefix = uuid |> String.replace("-", "") |> binary_part(0, 8)
    Path.join(["_derived", "#{slug}--#{uuid_prefix}.derived.elixirdb"])
  end

  defp strip_combining_marks(value) do
    value
    |> String.to_charlist()
    |> Enum.reject(&(&1 in 0x300..0x36F))
    |> List.to_string()
  end
end
