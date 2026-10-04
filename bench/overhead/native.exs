defmodule VialKeeper.Benchmarks.Overhead.Native do
  @moduledoc """
  Builds and drives `bench/native/vk_replay.c`, the native SQLite control.

  The program is compiled from the `sqlite3.c` amalgamation that the driver
  NIF (`native/vial_sqlite`) bundles through `libsqlite3-sys`, with the SQLite
  compile definitions that crate's build script passes for a bundled Unix
  build plus the crate's `LIBSQLITE3_FLAGS` (from
  `native/vial_sqlite/.cargo/config.toml`, unless the environment sets it), so
  both sides run the same SQLite engine. The build is cached under
  `_build/<env>/bench/` keyed by the compiler, flags, and source contents.

  The process is an Erlang port with 4-byte length framing; see the C file for
  the protocol. Each `run/3` call sends one fully encoded sample; the program
  times only the SQLite calls with `CLOCK_MONOTONIC`.
  """

  @request_timeout 120_000
  @driver_crate Path.expand("../../native/vial_sqlite", __DIR__)
  @sqlite_sys "libsqlite3-sys"

  # Cargo's release profile (opt-level 3, which rustler always builds) makes
  # the cc crate compile the bundled SQLite at -O3.
  @optimization ["-O3"]

  # Environment variables libsqlite3-sys's build script turns into SQLite
  # limits for a bundled build.
  @limit_env ~w(SQLITE_MAX_VARIABLE_NUMBER SQLITE_MAX_EXPR_DEPTH SQLITE_MAX_COLUMN)

  @type port_handle :: port()
  @type op :: {:stmt, boolean(), non_neg_integer(), list()} | {:exec, binary()}

  @doc "Compiles the native control if needed and returns build metadata."
  @spec build!() :: map()
  def build! do
    sqlite_sys = sqlite_sys_package!()
    sqlite_dir = Path.join(Path.dirname(sqlite_sys["manifest_path"]), "sqlite3")
    amalgamation = Path.join(sqlite_dir, "sqlite3.c")
    source = Path.expand("../native/vk_replay.c", __DIR__)
    compiler = System.get_env("CC", "cc")

    definitions =
      bundled_definitions(Path.join(Path.dirname(sqlite_sys["manifest_path"]), "build.rs")) ++
        limit_definitions() ++ extra_definitions()

    flags = @optimization ++ resolve_undefines(definitions)
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

      args = flags ++ ["-I", sqlite_dir, source, amalgamation, "-lpthread", "-lm", "-o", output]

      case System.cmd(compiler, args, stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {log, status} -> Mix.raise("native control build failed (#{status}):\n#{log}")
      end
    end

    %{
      "path" => output,
      "compiler" => compiler_version,
      "flags" => flags,
      "sqlite_sys_version" => sqlite_sys["version"],
      "amalgamation" =>
        Path.join([@sqlite_sys <> "-" <> sqlite_sys["version"], "sqlite3", "sqlite3.c"])
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

  # Mirrors the parameter conversion `Connection` applies before the driver.
  defp encode_param(nil), do: <<0>>
  defp encode_param(:undefined), do: <<0>>
  defp encode_param(value) when is_integer(value), do: <<1, value::signed-64>>
  defp encode_param(value) when is_float(value), do: <<2, value::float-64>>
  defp encode_param(value) when is_binary(value), do: [3, string(value)]
  defp encode_param(value) when is_atom(value), do: [3, string(Atom.to_string(value))]
  defp encode_param({:blob, value}), do: [4, string(IO.iodata_to_binary(value))]
  defp encode_param(value) when is_list(value), do: [3, string(IO.iodata_to_binary(value))]

  defp string(value), do: [<<byte_size(value)::32>>, value]

  defp sqlite_sys_package! do
    manifest = Path.join(@driver_crate, "Cargo.toml")
    args = ["metadata", "--format-version", "1", "--manifest-path", manifest]

    case System.cmd("cargo", args) do
      {json, 0} ->
        case json
             |> JSON.decode!()
             |> Map.fetch!("packages")
             |> Enum.filter(&(&1["name"] == @sqlite_sys)) do
          [package] ->
            package

          other ->
            Mix.raise("expected one #{@sqlite_sys} package in cargo metadata, got #{length(other)}")
        end

      {_output, status} ->
        Mix.raise("cargo metadata failed (#{status}) for #{manifest}")
    end
  rescue
    ErlangError -> Mix.raise("cargo is not available; it is needed to locate the driver's SQLite")
  end

  # The `-D`/`-U` flags of the bundled build's `cc::Build` chain, then the
  # definition libsqlite3-sys adds for every non-Windows target.
  defp bundled_definitions(build_rs) do
    source = File.read!(build_rs)

    chain =
      case Regex.run(
             ~r/cfg\.file\(format!\("\{lib_name\}\/sqlite3\.c"\)\)(.*?)\.warnings\(false\);/s,
             source
           ) do
        [_, chain] -> chain
        nil -> Mix.raise("#{build_rs}: bundled SQLite build definitions not found")
      end

    unix =
      case Regex.run(~r/if !win_target\(\) \{\s*cfg\.flag\("(-D[^"]+)"\);\s*\}/, source) do
        [_, definition] -> [definition]
        nil -> Mix.raise("#{build_rs}: non-Windows SQLite definition not found")
      end

    case Regex.scan(~r/\.flag\("(-[DU][^"]+)"\)/, chain, capture: :all_but_first) do
      [] -> Mix.raise("#{build_rs}: no SQLite definitions in the bundled build")
      found -> List.flatten(found) ++ unix
    end
  end

  defp limit_definitions do
    Enum.flat_map(@limit_env, fn name ->
      case System.get_env(name) do
        nil -> []
        limit -> ["-D#{name}=#{limit}"]
      end
    end)
  end

  # Cargo's `[env]` table does not override a variable already set in the
  # environment, so an exported LIBSQLITE3_FLAGS wins, as it does for cargo.
  defp extra_definitions do
    case System.get_env("LIBSQLITE3_FLAGS") do
      nil -> configured_extra_flags()
      flags -> flags
    end
    |> String.split()
    |> Enum.map(fn
      "-D" <> _ = flag -> flag
      "-U" <> _ = flag -> flag
      "SQLITE_" <> _ = flag -> "-D" <> flag
      flag -> Mix.raise("LIBSQLITE3_FLAGS entry #{inspect(flag)} is not understood")
    end)
  end

  defp configured_extra_flags do
    config = Path.join([@driver_crate, ".cargo", "config.toml"])

    case File.read(config) do
      {:ok, contents} ->
        case Regex.run(~r/^LIBSQLITE3_FLAGS\s*=\s*"([^"]*)"/m, contents) do
          [_, flags] ->
            flags

          nil ->
            if String.contains?(contents, "LIBSQLITE3_FLAGS"),
              do: Mix.raise("#{config}: LIBSQLITE3_FLAGS is not a plain string entry"),
              else: ""
        end

      {:error, :enoent} ->
        ""

      {:error, reason} ->
        Mix.raise("could not read #{config}: #{inspect(reason)}")
    end
  end

  # `-UNAME` cancels every earlier definition of NAME, as it does on the C
  # compiler command line; the resolved list keeps only effective definitions.
  defp resolve_undefines(flags) do
    flags
    |> Enum.reduce([], fn
      "-U" <> name, acc -> Enum.reject(acc, &(macro_name(&1) == name))
      "-D" <> _ = flag, acc -> [flag | acc]
    end)
    |> Enum.reverse()
  end

  defp macro_name("-D" <> definition), do: definition |> String.split("=", parts: 2) |> hd()

  defp compiler_version!(compiler) do
    case System.cmd(compiler, ["--version"], stderr_to_stdout: true) do
      {output, 0} -> output |> String.split("\n") |> List.first() |> String.trim()
      _ -> Mix.raise("C compiler #{inspect(compiler)} is not available; set CC")
    end
  rescue
    ErlangError -> Mix.raise("C compiler #{inspect(compiler)} is not available; set CC")
  end
end
