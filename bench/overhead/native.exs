defmodule VialKeeper.Benchmarks.Overhead.Native do
  @moduledoc """
  Builds and drives `bench/native/vk_replay.c`, the native SQLite control.

  The program is compiled from ExQLite's vendored `sqlite3.c` amalgamation with
  ExQLite's own SQLite compile definitions (read from its Makefile), so both
  sides run the same SQLite engine. The build is cached under
  `_build/<env>/bench/` keyed by the compiler, flags, and source contents.

  The process is an Erlang port with 4-byte length framing; see the C file for
  the protocol. Each `run/3` call sends one fully encoded sample; the program
  times only the SQLite calls with `CLOCK_MONOTONIC`.
  """

  @request_timeout 120_000

  @type port_handle :: port()
  @type op :: {:stmt, boolean(), non_neg_integer(), list()} | {:exec, binary()}

  @doc "Compiles the native control if needed and returns build metadata."
  @spec build!() :: map()
  def build! do
    exqlite = Map.fetch!(Mix.Project.deps_paths(), :exqlite)
    c_src = Path.join(exqlite, "c_src")
    amalgamation = Path.join(c_src, "sqlite3.c")
    source = Path.expand("../native/vk_replay.c", __DIR__)
    compiler = System.get_env("CC", "cc")
    definitions = exqlite_definitions(Path.join(exqlite, "Makefile"))
    flags = ["-O2", "-DNDEBUG=1" | definitions]
    compiler_version = compiler_version!(compiler)

    key =
      :crypto.hash(:sha256, [
        compiler_version,
        Enum.intersperse(flags, " "),
        File.read!(source),
        File.read!(amalgamation)
      ])
      |> Base.encode16(case: :lower)
      |> binary_part(0, 16)

    output = Path.join([Mix.Project.build_path(), "bench", "vk_replay-#{key}"])

    unless File.exists?(output) do
      File.mkdir_p!(Path.dirname(output))
      IO.puts("Building native SQLite control (one-time, cached): #{output}")

      args = flags ++ ["-I", c_src, source, amalgamation, "-lpthread", "-lm", "-o", output]

      case System.cmd(compiler, args, stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {log, status} -> Mix.raise("native control build failed (#{status}):\n#{log}")
      end
    end

    %{
      "path" => output,
      "compiler" => compiler_version,
      "flags" => flags,
      "amalgamation" => Path.relative_to_cwd(amalgamation)
    }
  end

  @doc "Starts the native control process."
  @spec start(binary()) :: port_handle()
  def start(path) do
    Port.open({:spawn_executable, path}, [:binary, :exit_status, :use_stdio, {:packet, 4}])
  end

  @doc "Opens a regular in-memory database restored from a serialized image."
  @spec open_image(port_handle(), binary()) :: :ok
  def open_image(port, image), do: ok!(request(port, [?O, 0, <<byte_size(image)::32>>, image]))

  @doc "Opens a database file."
  @spec open_file(port_handle(), binary()) :: :ok
  def open_file(port, path), do: ok!(request(port, [?O, 1, <<byte_size(path)::32>>, path]))

  @doc "Runs setup SQL through `sqlite3_exec` (untimed)."
  @spec exec(port_handle(), binary()) :: :ok
  def exec(port, sql), do: ok!(request(port, [?E, string(sql)]))

  @doc "Returns the first column of every row of `sql`."
  @spec scalar(port_handle(), binary()) :: [binary()]
  def scalar(port, sql) do
    case request(port, [?S, string(sql)]) do
      {:text, ""} -> []
      {:text, text} -> String.split(text, "\n")
      other -> Mix.raise("native control query failed: #{inspect(other)}")
    end
  end

  @doc "Prepares statement `id`."
  @spec prepare(port_handle(), non_neg_integer(), binary()) :: :ok
  def prepare(port, id, sql), do: ok!(request(port, [?P, <<id::32>>, string(sql)]))

  @doc "Encodes one sample of operations into a run request (do this before timing)."
  @spec encode_run([op()]) :: iodata()
  def encode_run(ops), do: [?R, <<length(ops)::32>> | Enum.map(ops, &encode_op/1)]

  @doc "Executes a pre-encoded sample and returns the native measurements."
  @spec run(port_handle(), iodata()) :: map()
  def run(port, encoded) do
    case request(port, encoded) do
      {:run, result} -> result
      other -> Mix.raise("native control run failed: #{inspect(other)}")
    end
  end

  @doc "Closes the database and stops the process."
  @spec stop(port_handle()) :: :ok
  def stop(port) do
    _ = request(port, [?C])
    Port.close(port)
    :ok
  catch
    _kind, _reason -> :ok
  end

  defp request(port, payload) do
    true = Port.command(port, payload)

    receive do
      {^port, {:data, <<?k>>}} ->
        :ok

      {^port, {:data, <<?t, text::binary>>}} ->
        {:text, text}

      {^port, {:data, <<?r, ns::64, rows::64, vm_steps::64, statements::64>>}} ->
        {:run, %{ns: ns, rows: rows, vm_steps: vm_steps, statements: statements}}

      {^port, {:data, <<?e, message::binary>>}} ->
        {:error, message}

      {^port, {:exit_status, status}} ->
        Mix.raise("native control exited with status #{status}")
    after
      @request_timeout -> Mix.raise("native control did not reply within #{@request_timeout} ms")
    end
  end

  defp ok!(:ok), do: :ok
  defp ok!(other), do: Mix.raise("native control request failed: #{inspect(other)}")

  defp encode_op({:exec, sql}), do: [1, string(sql)]

  defp encode_op({:stmt, count_rows, id, params}) do
    [
      0,
      if(count_rows, do: 1, else: 0),
      <<id::32, length(params)::16>> | Enum.map(params, &encode_param/1)
    ]
  end

  # Mirrors Exqlite.Sqlite3.bind/2 value conversion.
  defp encode_param(nil), do: <<0>>
  defp encode_param(:undefined), do: <<0>>
  defp encode_param(value) when is_integer(value), do: <<1, value::signed-64>>
  defp encode_param(value) when is_float(value), do: <<2, value::float-64>>
  defp encode_param(value) when is_binary(value), do: [3, string(value)]
  defp encode_param(value) when is_atom(value), do: [3, string(Atom.to_string(value))]
  defp encode_param({:blob, value}), do: [4, string(IO.iodata_to_binary(value))]
  defp encode_param(value) when is_list(value), do: [3, string(IO.iodata_to_binary(value))]

  defp string(value), do: [<<byte_size(value)::32>>, value]

  defp exqlite_definitions(makefile) do
    makefile
    |> File.read!()
    |> String.split("\n")
    |> Enum.flat_map(fn line ->
      case Regex.run(~r/^CFLAGS \+= (-D\S+)\s*$/, line) do
        [_, definition] -> [definition]
        nil -> []
      end
    end)
  end

  defp compiler_version!(compiler) do
    case System.cmd(compiler, ["--version"], stderr_to_stdout: true) do
      {output, 0} -> output |> String.split("\n") |> List.first() |> String.trim()
      _ -> Mix.raise("C compiler #{inspect(compiler)} is not available; set CC")
    end
  rescue
    ErlangError -> Mix.raise("C compiler #{inspect(compiler)} is not available; set CC")
  end
end
