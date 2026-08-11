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

  @doc """
  Recursively converts map keys to strings for JSON/HTML presentation.

  Booleans and `nil` are preserved so values can be round-tripped into facades
  when needed.
  """
  @spec stringify_keys(map()) :: map()
  def stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_value(value)}
      {key, value} when is_binary(key) -> {key, stringify_value(value)}
      {key, value} -> {to_string(key), stringify_value(value)}
    end)
  end

  @doc """
  Recursively redacts known secret keys such as `auth_token`.
  """
  @spec redact_secrets(term()) :: term()
  def redact_secrets(term) when is_map(term) do
    Map.new(term, fn
      {key, _value} when key in ["auth_token", :auth_token] ->
        {key, "[redacted]"}

      {key, value} ->
        {key, redact_secrets(value)}
    end)
  end

  def redact_secrets(list) when is_list(list), do: Enum.map(list, &redact_secrets/1)
  def redact_secrets(other), do: other

  defp stringify_value(map) when is_map(map) and not is_struct(map), do: stringify_keys(map)
  defp stringify_value(list) when is_list(list), do: Enum.map(list, &stringify_value/1)
  defp stringify_value(value) when is_boolean(value), do: value
  defp stringify_value(nil), do: nil
  defp stringify_value(atom) when is_atom(atom), do: Atom.to_string(atom)
  defp stringify_value(other), do: other
end
