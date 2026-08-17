defmodule VialKeeper.Search.RebuildTimeoutTest do
  @moduledoc """
  Full-text rebuild duration is a host ceiling (`max_search_rebuild_ms`), not
  the 5s `GenServer.call/3` default and not `max_query_execution_ms`.
  """
  use ExUnit.Case, async: true

  alias VialKeeper.Config
  alias VialKeeper.Quality.ReachSmellCase

  setup do
    previous = Application.get_env(:vial_keeper, :host_limits, [])

    on_exit(fn ->
      Application.put_env(:vial_keeper, :host_limits, previous)
    end)

    %{previous: previous}
  end

  test "search_rebuild_timeout_ms reads max_search_rebuild_ms", %{previous: previous} do
    Application.put_env(
      :vial_keeper,
      :host_limits,
      Keyword.put(previous, :max_search_rebuild_ms, 12_345)
    )

    assert Config.search_rebuild_timeout_ms() == 12_345
  end

  test "search_rebuild_timeout_ms uses the compiled floor when the key is absent", %{
    previous: previous
  } do
    Application.put_env(
      :vial_keeper,
      :host_limits,
      Keyword.delete(previous, :max_search_rebuild_ms)
    )

    assert Config.search_rebuild_timeout_ms() == 300_000
  end

  test "full-text create_index and rebuild_index pass the rebuild budget, not 30s" do
    ast = query_ast()

    assert match?(
             {:index_lifecycle_timeout, _, [{:normalized, _, _}]},
             catalog_timeout(ast, :create_index)
           )

    assert match?(
             {{:., _, [{:__aliases__, _, [:Config]}, :search_rebuild_timeout_ms]}, _, []},
             catalog_timeout(ast, :rebuild_index)
           )

    assert match?(
             {{:., _, [{:__aliases__, _, [:Config]}, :search_rebuild_timeout_ms]}, _, []},
             lifecycle_timeout(ast, "full_text")
           )

    assert match?(
             {{:., _, [{:__aliases__, _, [:Config]}, :request_timeout_ms]}, _, []},
             lifecycle_timeout(ast, :default)
           )
  end

  defp query_ast do
    "lib/vial_keeper/query.ex"
    |> File.read!()
    |> ReachSmellCase.parse!()
  end

  defp catalog_timeout(ast, kind) do
    {_ast, timeout} =
      Macro.prewalk(ast, nil, fn
        {:def, _, [{name, _, _args}, opts]} = node, acc
        when name == kind and is_list(opts) ->
          {node, command_timeout(do_body(opts), kind, acc)}

        other, acc ->
          {other, acc}
      end)

    timeout || flunk("DatabaseCatalog.command timeout for #{kind} was not found")
  end

  defp command_timeout(body, _kind, acc) do
    {_body, timeout} =
      Macro.prewalk(body, acc, fn
        {{:., _, [{:__aliases__, _, [:DatabaseCatalog]}, :command]}, _, [_uuid, _command, timeout]} =
            node,
        _acc ->
          {node, timeout}

        other, acc ->
          {other, acc}
      end)

    timeout
  end

  defp lifecycle_timeout(ast, "full_text") do
    {_ast, timeout} =
      Macro.prewalk(ast, nil, fn
        {:defp, _, [{:index_lifecycle_timeout, _, [{:%{}, _, _}]}, opts]} = node, acc
        when is_list(opts) ->
          {node, unwrap_do(do_body(opts) || acc)}

        other, acc ->
          {other, acc}
      end)

    timeout || flunk("full-text index_lifecycle_timeout clause was not found")
  end

  defp lifecycle_timeout(ast, :default) do
    {_ast, timeout} =
      Macro.prewalk(ast, nil, fn
        {:defp, _, [{:index_lifecycle_timeout, _, [{:_definition, _, _}]}, opts]} = node, acc
        when is_list(opts) ->
          {node, unwrap_do(do_body(opts) || acc)}

        other, acc ->
          {other, acc}
      end)

    timeout || flunk("default index_lifecycle_timeout clause was not found")
  end

  defp do_body(opts) when is_list(opts) do
    Enum.find_value(opts, fn
      {{:__block__, _, [:do]}, body} -> body
      {:do, body} -> body
      _ -> nil
    end)
  end

  defp unwrap_do({:__block__, _, [value]}), do: value
  defp unwrap_do(value), do: value
end
