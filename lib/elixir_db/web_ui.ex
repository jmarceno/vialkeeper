defmodule ElixirDB.WebUI do
  @moduledoc """
  Embedded offline administration console served under `/ui`.

  The console is a presentation layer over existing application facades. It is
  compiled into the release with vendored HTMX and project-owned CSS/JS so a
  production OTP node needs no Internet access and no runtime frontend source
  tree.
  """

  @doc """
  Returns whether the embedded Web UI is enabled in host configuration.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    :elixir_db
    |> Application.get_env(:web_ui, [])
    |> Keyword.get(:enabled, true)
  end
end
