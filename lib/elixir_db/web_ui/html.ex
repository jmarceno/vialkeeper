defmodule ElixirDB.WebUI.HTML do
  @moduledoc """
  HTML escaping and attribute construction for the embedded Web UI.

  Every value derived from database, user, or configuration state must pass
  through this module before it is inserted into markup. Route modules must not
  concatenate unescaped dynamic values into tags or attributes.
  """

  @doc """
  Escapes text for safe insertion into HTML element content.
  """
  @spec escape(term()) :: String.t()
  def escape(value) when is_binary(value), do: Plug.HTML.html_escape(value)

  def escape(nil), do: ""

  def escape(value) when is_atom(value), do: escape(Atom.to_string(value))
  def escape(value) when is_integer(value), do: Integer.to_string(value)
  def escape(value) when is_float(value), do: :erlang.float_to_binary(value)
  def escape(value), do: escape(Kernel.inspect(value))

  @doc """
  Escapes a value for use inside a double-quoted HTML attribute.
  """
  @spec attr(term()) :: String.t()
  def attr(value), do: escape(value)

  @doc """
  Builds a double-quoted attribute assignment `name="escaped-value"`.
  """
  @spec attribute(String.t(), term()) :: iodata()
  def attribute(name, value) when is_binary(name) do
    [name, "=\"", attr(value), "\""]
  end

  @doc """
  Escapes JSON text intended for a `<textarea>` body.
  """
  @spec textarea(term()) :: String.t()
  def textarea(value) when is_binary(value), do: escape(value)
  def textarea(value), do: escape(encode_json(value))

  @doc """
  Encodes a term as compact JSON using the project JSON encoder.
  """
  @spec encode_json(term()) :: String.t()
  def encode_json(value), do: JSON.encode!(value)
end
