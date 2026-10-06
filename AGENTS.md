# Contributor instructions

This file contains repository-safe guidance only.

Do not add internal specifications, private document identifiers, personal
filesystem paths, credentials, tokens, or other non-public project material to
tracked files.

## Validation

- Keep test guarantees intact; never weaken checks, thresholds, or coverage to make a gate pass.
- Use `mix check.fast` while iterating.
- Run `mix check.full` before handoff. A change is incomplete while that gate is red.
- Use the narrowest focused checks that cover the files being changed during iteration.

## Documentation

- Keep `README.md` focused on the public product and developer interface.
- Keep `Operations.md` focused on public deployment and operator procedures.
- Update the appropriate public document when a new public feature changes its contract.
- Do not document bug fixes in those files unless the public contract itself changed.

## Elixir

- Normalize external maps once at the boundary.
- Prefer pattern matching and function clauses over defensive branching.
- Handle `{:ok, _} | {:error, _}` results explicitly.
- Do not blanket-rescue exceptions or use unsupervised production concurrency.
- Pass explicit timeouts to blocking calls.
- Keep backend-specific APIs inside their boundary modules.
- Add precise `@spec`s to new public APIs.

Do not publish or push changes automatically. Review the complete diff and
remove any secrets or personal data before distribution.


## Remote Rust builds

Compile with `cargo remote` instead of a local `cargo build`, `cargo test`, or `cargo clippy`. One command syncs the sources, builds on the shared builder, and copies back only the finished executable into `target/<profile>/`. Crates, incremental files, and the rest of `target/` stay on the builder.

```bash
cargo remote -- build --release
cargo remote -- test
cargo remote -- clippy --all-targets -- -D warnings
```

`cargo fmt` stays local. Dotfiles are not synced; pass `-h` only when the build needs them. Pass `-c <path-under-target>` only when you explicitly want a different file copied back.


The builder is already set up. Do not compile locally because a host is missing from Cargo's config. `cargo remote` does not read `~/.cargo/config.toml` for the host. That file only sets the `kache` wrapper, and a host will not be there.

Read these two files directly. A search that stays inside this repo will not see them, and that is not evidence that the builder is unconfigured.

- `~/.config/cargo-remote/cargo-remote.toml` — the only config `cargo remote` uses. The filename is `cargo-remote.toml`, not `config.toml`. It defines one remote, `rust-builder`, so `cargo remote -- build` uses it with no `-H` and no `-r`. Builds land in `~/remote-builds` on that machine.
- `~/.ssh/config`, host `rust-builder` — `192.168.0.65`, user `builder`, key `~/.ssh/id_ed25519`.

If `cargo remote` fails, report the command's error. Do not switch to a local `cargo build`, `cargo test`, or `cargo clippy`.
