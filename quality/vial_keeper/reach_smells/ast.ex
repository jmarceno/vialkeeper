defmodule VialKeeper.Quality.ReachSmells.AST do
  @moduledoc "Shared AST walking helpers for VialKeeper Reach smells."

  alias Reach.Smell.Finding

  @type module_name :: [atom()]

  @special_forms [
    :__aliases__,
    :__block__,
    :__cursor__,
    :{},
    :%{},
    :<<>>,
    :->,
    :|,
    :.,
    :defmodule,
    :def,
    :defp,
    :defmacro,
    :defmacrop,
    :defdelegate,
    :defguard,
    :defguardp,
    :when,
    :alias,
    :require,
    :import,
    :use,
    :if,
    :unless,
    :case,
    :cond,
    :with,
    :fn,
    :for,
    :try,
    :catch,
    :rescue,
    :after,
    :receive,
    :quote,
    :unquote,
    :unquote_splicing,
    :super,
    :&,
    :@,
    :and,
    :or,
    :not,
    :in,
    :"::"
  ]

  @spec walk_modules(Macro.t(), (module_name(), Macro.t() -> [Finding.t()])) :: [Finding.t()]
  def walk_modules(ast, visitor) when is_function(visitor, 2) do
    {_ast, findings} =
      Macro.prewalk(ast, [], fn
        {:defmodule, _meta, [{:__aliases__, _, parts}, opts]} = node, acc ->
          {node, visitor.(parts, do_body(opts)) ++ acc}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(findings)
  end

  @spec each_call(Macro.t(), (atom() | module_name() | Macro.t(), atom(), [Macro.t()], keyword() ->
                                [term()])) :: [term()]
  def each_call(ast, visitor) when is_function(visitor, 4) do
    {_ast, acc} = Macro.prewalk(ast, [], &collect_call(&1, &2, visitor))
    Enum.reverse(acc)
  end

  @spec line(keyword() | Macro.t()) :: pos_integer()
  def line(meta) when is_list(meta), do: Keyword.get(meta, :line, 1)
  def line({_, meta, _}) when is_list(meta), do: line(meta)
  def line(_node), do: 1

  @spec finding(atom(), String.t(), Path.t(), keyword() | Macro.t()) :: Finding.t()
  def finding(kind, message, file, meta) do
    Finding.new(
      kind: kind,
      message: message,
      location: "#{file}:#{line(meta)}"
    )
  end

  @spec aliases(Macro.t()) :: %{atom() => module_name()}
  def aliases(ast) do
    {_ast, aliases} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, [target]} = node, acc ->
          {node, put_alias(acc, target, nil)}

        {:alias, _, [target, opts]} = node, acc ->
          {node, put_alias(acc, target, alias_as(opts))}

        node, acc ->
          {node, acc}
      end)

    aliases
  end

  @spec expand_alias(Macro.t(), %{atom() => module_name()}) :: module_name() | nil
  def expand_alias({:__aliases__, _, [head | rest]}, aliases) when is_atom(head) do
    case Map.fetch(aliases, head) do
      {:ok, prefix} -> prefix ++ rest
      :error -> [head | rest]
    end
  end

  def expand_alias({:__aliases__, _, parts}, _aliases) when is_list(parts), do: parts

  def expand_alias(node, aliases) do
    case unwrap(node) do
      name when is_atom(name) -> [name]
      other when other != node -> expand_alias(other, aliases)
      _other -> nil
    end
  end

  @spec option_keyword(Macro.t()) :: keyword() | nil
  def option_keyword(ast) when is_list(ast) do
    pairs = Enum.map(ast, &keyword_pair/1)

    if Enum.all?(pairs, &match?({key, _} when is_atom(key), &1)), do: pairs
  end

  def option_keyword(_ast), do: nil

  @spec unwrap(Macro.t()) :: Macro.t()
  def unwrap({:__block__, _meta, [value]}), do: unwrap(value)
  def unwrap(value), do: value

  @spec do_body(Macro.t()) :: Macro.t()
  def do_body([{:do, body} | _rest]), do: body
  def do_body([{{:__block__, _, [:do]}, body} | _rest]), do: body
  def do_body(other), do: other

  defp collect_call(
         {:|>, meta, [left, {{:., call_meta, [module_ast, name]}, _, args}]},
         acc,
         visitor
       )
       when is_atom(name) and is_list(args) do
    {
      {:|>, meta, [left, {{:., call_meta, [module_ast, name]}, [], args}]},
      visitor.(module_ast, name, [left | args], call_meta) ++ acc
    }
  end

  defp collect_call({{:., meta, [module_ast, name]}, _, args} = node, acc, visitor)
       when is_atom(name) and is_list(args) do
    {node, visitor.(module_ast, name, args, meta) ++ acc}
  end

  defp collect_call({name, meta, args} = node, acc, visitor)
       when is_atom(name) and is_list(args) and name not in @special_forms do
    {node, visitor.(:kernel, name, args, meta) ++ acc}
  end

  defp collect_call(
         {:&, meta, [{:/, _, [{{:., _, [module_ast, name]}, _, []}, arity]}]} = node,
         acc,
         visitor
       )
       when is_atom(name) and is_integer(arity) do
    args = List.duplicate(:capture, arity)
    {node, visitor.(module_ast, name, args, meta) ++ acc}
  end

  defp collect_call(node, acc, _visitor), do: {node, acc}

  defp keyword_pair({key, value}) when is_atom(key), do: {key, unwrap(value)}

  defp keyword_pair({{:__block__, _, [key]}, value}) when is_atom(key),
    do: {key, unwrap(value)}

  defp keyword_pair(other), do: other

  defp put_alias(acc, {:__aliases__, _, parts}, nil) when is_list(parts) do
    Map.put(acc, List.last(parts), parts)
  end

  defp put_alias(acc, {:__aliases__, _, parts}, as) when is_list(parts) and is_atom(as) do
    Map.put(acc, as, parts)
  end

  defp put_alias(acc, _target, _as), do: acc

  defp alias_as(opts) when is_list(opts) do
    case unwrap(Keyword.get(opts, :as) || sourceror_option(opts, :as)) do
      {:__aliases__, _, [name]} -> name
      name when is_atom(name) -> name
      _other -> nil
    end
  end

  defp alias_as(_opts), do: nil

  defp sourceror_option(opts, key) do
    Enum.find_value(opts, fn
      {{:__block__, _, [^key]}, value} -> value
      {^key, value} -> value
      _other -> nil
    end)
  end
end
