defmodule VialKeeper.Storage.Turso.Native do
  @moduledoc """
  Turso driver NIF (`native/vial_turso`, Rust over the `turso` crate).

  Same function set and term encoding as `VialKeeper.Storage.SQLite.Native`.
  Every database call runs on a dirty IO scheduler; `query_inline/3` always
  returns `:contended`. A write-write conflict is `{:error, :write_conflict}`.
  `serialize/1` is unsupported.

  Only `VialKeeper.Storage.Turso.Driver` calls this module.
  """
  # Turso is pure Rust: an unoptimized build is far too slow even for tests.
  use Rustler, otp_app: :vial_keeper, crate: :vial_turso, path: "native/vial_turso", mode: :release

  use VialKeeper.Storage.SQLite.Driver, :nif
end
