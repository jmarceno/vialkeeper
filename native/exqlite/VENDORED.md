# Vendored exqlite

This directory is a trimmed copy of [exqlite](https://github.com/elixir-sqlite/exqlite)
0.39.0 (MIT, see `LICENSE`). It bundles the SQLite amalgamation in `c_src/`.

VialKeeper uses only `Exqlite.Sqlite3`. The DBConnection adapter, the
precompiled-artifact download and the upstream tests were dropped, and the NIF
is always built from source with `elixir_make`.

## Local changes

- `multi_step_inline/3` (`c_src/sqlite3_nif.c`, `lib/exqlite/sqlite3_nif.ex`,
  `lib/exqlite/sqlite3.ex`): the same C function as `multi_step/3`, registered
  without the dirty IO flag. A dirty-scheduler hop costs 3–10 µs, while a
  primary-key lookup or a single-row write takes 0.5–1.5 µs inside SQLite.
  The storage layer uses it only for bounded point statements inside its own
  write transaction, where no statement can wait on a lock.

## Updating

1. Copy `c_src/`, `Makefile` and the five `lib/exqlite/*.ex` files from the
   new upstream release.
2. Re-apply the changes listed above, then bump `@version` in `mix.exs`.
