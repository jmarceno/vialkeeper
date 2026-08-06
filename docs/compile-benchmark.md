# Compile benchmark — Plan E3 / `module_definition: :interpreted`

**Decision: drop / do not enable** `elixirc_options: [module_definition: :interpreted]` in `mix.exs`.

## Measurement

Host: Manjaro Linux, Elixir/OTP from project `mise.toml`, project root `/home/jmarceno/Projects/elixirdb`. Command: `mix compile --force` after a warm compile (88 `.ex` files).

| Mode | Run | Approximate wall time |
| --- | --- | --- |
| Default (`:compiled`) | 1 | ~1.37 s |
| Default (`:compiled`) | 2 | ~1.35 s |
| Spike `:interpreted` (one-off `Code.put_compiler_option/2`) | 1 | ~1.16 s |
| Spike `:interpreted` | 2 | ~1.20 s |

User/sys CPU for default force compiles was roughly 7 s user / 1.4 s sys (parallel compiler); wall time stayed under 1.5 s.

## Justification

Plan §3.4 allows `elixirc_options: [module_definition: :interpreted]` **only if** an initial clean-compile benchmark confirms a benefit without tool incompatibility. On this tree the default force compile is already ~1.35 s wall. Interpreted mode saved on the order of ~150–200 ms (~12%) — noise-level for everyday developer and CI feedback, and not worth a permanent project compiler option.

The option does not change generated BEAM files; it only changes how the compiler represents modules during compilation. Keeping it out of `mix.exs` avoids an extra dialyzer/tooling surface and an undocumented divergence from stock Mix defaults for a project whose clean compile is already sub-second to low-second.

**E3 residual: CLOSED (drop).** Do not enable `:interpreted` unless a future monorepo-scale compile regression reopens the measurement.
